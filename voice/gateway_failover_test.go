package voice

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/disgoorg/godave"
	"github.com/gorilla/websocket"
)

func TestGatewayCloseCancelsDialAndReconnect(t *testing.T) {
	started := make(chan struct{}, 2)
	dialer := &websocket.Dialer{
		NetDialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			started <- struct{}{}
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}
	gateway := NewGateway(nil, nil, func(Gateway, error) {
		t.Error("cancelled reconnect must not invoke the close handler")
	}, WithGatewayDialer(dialer)).(*gatewayImpl)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	finished := make(chan error, 1)
	go func() { finished <- gateway.Open(ctx, State{Endpoint: "pending.discord.media"}) }()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("dial did not start")
	}
	closed := make(chan struct{})
	go func() {
		gateway.Close()
		close(closed)
	}()
	select {
	case <-closed:
	case <-ctx.Done():
		t.Fatal("Close blocked behind an in-flight dial")
	}
	select {
	case err := <-finished:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected cancellation, got %v", err)
		}
	case <-ctx.Done():
		t.Fatal("Open did not stop after Close")
	}
	if err := gateway.doReconnect(gateway.reconnectCtx, State{Endpoint: "stale.discord.media"}, maximumReconnectAttempts); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected cancelled reconnect, got %v", err)
	}
	select {
	case <-started:
		t.Fatal("closed gateway restarted an old connection attempt")
	default:
	}
}

func TestGatewayServerReplacementIdentifies(t *testing.T) {
	for _, closeSocket := range []bool{false, true} {
		t.Run(map[bool]string{false: "connected", true: "socket already closed"}[closeSocket], func(t *testing.T) {
			gateway, state, packets := newFailoverTestGateway(t, nil)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			if err := gateway.Open(ctx, state); err != nil {
				t.Fatal(err)
			}
			if packet := waitForGatewayPacket(t, packets); packet.Op != OpcodeIdentify {
				t.Fatalf("initial connection sent %v instead of Identify", packet.Op)
			}
			gateway.connMu.Lock()
			oldConnection := gateway.conn
			gateway.connMu.Unlock()
			if closeSocket {
				gateway.CloseWithCode(websocket.CloseServiceRestart, "old socket closed")
			}
			gateway.Close()
			state.Token = "new-token"
			if err := gateway.Open(ctx, state); err != nil {
				t.Fatal(err)
			}
			packet := waitForGatewayPacket(t, packets)
			identify, ok := packet.D.(GatewayMessageDataIdentify)
			if !ok || packet.Op != OpcodeIdentify || identify.Token != state.Token || identify.SessionID != state.SessionID {
				t.Fatalf("replacement did not Identify with new credentials: %+v", packet)
			}
			gateway.reconnect(oldConnection, "late heartbeat failure")
			if gateway.closeWithCode(oldConnection, websocket.CloseNormalClosure, "late cancellation") || gateway.Status() != StatusReady {
				t.Fatal("old connection interfered with its replacement")
			}
		})
	}
}

func TestGatewayTransientCloseStillResumes(t *testing.T) {
	disconnect := make(chan struct{})
	gateway, state, packets := newFailoverTestGateway(t, disconnect)
	resumed := make(chan struct{})
	gateway.eventHandlerFunc = func(_ Gateway, op Opcode, _ int, _ GatewayMessageData) {
		if op == OpcodeResumed {
			close(resumed)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := gateway.Open(ctx, state); err != nil {
		t.Fatal(err)
	}
	waitForGatewayPacket(t, packets)
	close(disconnect)
	select {
	case <-resumed:
	case <-ctx.Done():
		t.Fatal("transient disconnect did not resume")
	}
	packet := waitForGatewayPacket(t, packets)
	resume, ok := packet.D.(GatewayMessageDataResume)
	if !ok || packet.Op != OpcodeResume || resume.Token != state.Token || resume.SessionID != state.SessionID || resume.SeqAck != 1 {
		t.Fatalf("transient disconnect did not preserve resume data: %+v", packet)
	}
}

func newFailoverTestGateway(t *testing.T, disconnect <-chan struct{}) (*gatewayImpl, State, <-chan GatewayMessage) {
	t.Helper()
	packets := make(chan GatewayMessage, 4)
	upgrader := websocket.Upgrader{}
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Error(err)
			return
		}
		defer conn.Close()
		_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
		if err = conn.WriteJSON(map[string]any{"op": OpcodeHello, "d": map[string]any{"heartbeat_interval": 60000}}); err != nil {
			t.Error(err)
			return
		}
		var packet GatewayMessage
		if err = conn.ReadJSON(&packet); err != nil {
			t.Error(err)
			return
		}
		packets <- packet
		response := GatewayMessage{Op: OpcodeReady, Seq: 1, D: GatewayMessageDataReady{SSRC: 42}}
		if packet.Op == OpcodeResume {
			response = GatewayMessage{Op: OpcodeResumed, D: GatewayMessageDataResumed{}}
		}
		if err = conn.WriteJSON(response); err != nil {
			t.Error(err)
			return
		}
		if disconnect != nil && packet.Op == OpcodeIdentify {
			select {
			case <-disconnect:
				_ = conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseServiceRestart, "restart"))
			case <-time.After(3 * time.Second):
				t.Error("disconnect was not requested")
			}
			return
		}
		for {
			if _, _, err = conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
	t.Cleanup(server.Close)
	gateway := NewGateway(godave.NewNoopSession(slog.Default(), "2", nil), func(Gateway, Opcode, int, GatewayMessageData) {},
		func(_ Gateway, err error) { t.Errorf("unexpected gateway close: %v", err) }, WithGatewayDialer(&websocket.Dialer{
			TLSClientConfig: server.Client().Transport.(*http.Transport).TLSClientConfig,
		})).(*gatewayImpl)
	t.Cleanup(gateway.Close)
	state := State{GuildID: 1, UserID: 2, ChannelID: 3, SessionID: "session", Token: "token", Endpoint: strings.TrimPrefix(server.URL, "https://")}
	return gateway, state, packets
}

func waitForGatewayPacket(t *testing.T, packets <-chan GatewayMessage) GatewayMessage {
	t.Helper()
	select {
	case packet := <-packets:
		return packet
	case <-time.After(3 * time.Second):
		t.Fatal("gateway did not send an authentication packet")
		return GatewayMessage{}
	}
}
