package voice

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/disgoorg/snowflake/v2"

	"github.com/disgoorg/disgo/discord"
	botgateway "github.com/disgoorg/disgo/gateway"
)

func TestVoiceServerFailover(t *testing.T) {
	for _, endpoint := range []string{"new.discord.media", "old.discord.media"} {
		t.Run(endpoint, func(t *testing.T) {
			conn, gateway, udp := newServerUpdateTestConn(t)
			conn.HandleVoiceServerUpdate(botgateway.EventVoiceServerUpdate{
				GuildID: 1, Token: "new-token", Endpoint: &endpoint,
			})
			state := waitForServerGatewayOpen(t, gateway)
			if state.SessionID != "session" || state.ChannelID != 3 || state.Token != "new-token" || state.Endpoint != endpoint {
				t.Fatalf("unexpected failover state: %+v", state)
			}
			if gateway.closeCount() == 0 || udp.closeCount() == 0 {
				t.Fatal("old voice transports were not closed")
			}
		})
	}
}

func TestVoiceServerEndpointRemoved(t *testing.T) {
	conn, gateway, udp := newServerUpdateTestConn(t)
	conn.HandleVoiceServerUpdate(botgateway.EventVoiceServerUpdate{GuildID: 1, Token: "unallocated"})
	if gateway.closeCount() == 0 || udp.closeCount() == 0 {
		t.Fatal("endpoint removal did not close the old voice transports")
	}
	if conn.state.Endpoint != "" || conn.state.Token != "" {
		t.Fatalf("endpoint removal retained stale server credentials: %+v", conn.state)
	}
	if conn.state.SessionID != "session" || conn.state.ChannelID != 3 {
		t.Fatalf("endpoint removal lost the voice state: %+v", conn.state)
	}
	channelID := snowflake.ID(3)
	conn.HandleVoiceStateUpdate(botgateway.EventVoiceStateUpdate{VoiceState: discord.VoiceState{
		GuildID: 1, UserID: 2, ChannelID: &channelID, SessionID: "session", SelfMute: true,
	}})
	assertNoServerGatewayOpen(t, gateway)
	endpoint := "reallocated.discord.media"
	conn.HandleVoiceServerUpdate(botgateway.EventVoiceServerUpdate{GuildID: 1, Token: "new-token", Endpoint: &endpoint})
	state := waitForServerGatewayOpen(t, gateway)
	if state.Endpoint != endpoint || state.Token != "new-token" || state.SessionID != "session" || !state.SelfMute {
		t.Fatalf("unexpected reallocated state: %+v", state)
	}
}

func TestVoiceServerUpdateIgnoresOtherGuild(t *testing.T) {
	conn, gateway, udp := newServerUpdateTestConn(t)
	before := conn.state
	endpoint := "other.discord.media"
	for _, value := range []*string{nil, &endpoint} {
		conn.HandleVoiceServerUpdate(botgateway.EventVoiceServerUpdate{GuildID: 9, Token: "other-token", Endpoint: value})
	}
	assertNoServerGatewayOpen(t, gateway)
	if conn.state != before || gateway.closeCount() != 0 || udp.closeCount() != 0 {
		t.Fatal("an unrelated guild changed the voice connection")
	}
}

func TestVoiceStateUpdateDoesNotReopenGateway(t *testing.T) {
	conn, gateway, _ := newServerUpdateTestConn(t)
	channelID := snowflake.ID(3)
	conn.HandleVoiceStateUpdate(botgateway.EventVoiceStateUpdate{VoiceState: discord.VoiceState{
		GuildID: 1, UserID: 2, ChannelID: &channelID, SessionID: "session", SelfMute: true,
	}})
	assertNoServerGatewayOpen(t, gateway)
	if gateway.closeCount() != 0 {
		t.Fatal("a mute update closed the voice gateway")
	}
}

func TestVoiceChannelMoveReplacesAttemptWithLatestVoiceState(t *testing.T) {
	for _, serverFirst := range []bool{false, true} {
		t.Run(map[bool]string{false: "state first", true: "server first"}[serverFirst], func(t *testing.T) {
			conn, gateway, _ := newServerUpdateTestConn(t)
			channelID := snowflake.ID(4)
			endpoint := "moved.discord.media"
			stateUpdate := func() {
				conn.HandleVoiceStateUpdate(botgateway.EventVoiceStateUpdate{VoiceState: discord.VoiceState{
					GuildID: 1, UserID: 2, ChannelID: &channelID, SessionID: "moved-session",
				}})
			}
			serverUpdate := func() {
				conn.HandleVoiceServerUpdate(botgateway.EventVoiceServerUpdate{
					GuildID: 1, Token: "moved-token", Endpoint: &endpoint,
				})
			}

			if serverFirst {
				serverUpdate()
				waitForServerGatewayOpen(t, gateway)
				stateUpdate()
			} else {
				stateUpdate()
				waitForServerGatewayOpen(t, gateway)
				serverUpdate()
			}

			state := waitForServerGatewayOpen(t, gateway)
			if state.ChannelID != channelID || state.SessionID != "moved-session" || state.Token != "moved-token" || state.Endpoint != endpoint {
				t.Fatalf("voice channel move did not use the latest state: %+v", state)
			}
		})
	}
}

func TestVoiceServerUpdateCancelsPendingOpen(t *testing.T) {
	for _, action := range []string{"replace", "remove endpoint", "leave channel"} {
		t.Run(action, func(t *testing.T) {
			conn, gateway, _ := newServerUpdateTestConn(t)
			pending := &pendingServerGateway{
				serverUpdateGateway: gateway,
				started:             make(chan struct{}),
				finished:            make(chan struct{}),
			}
			conn.gateway = pending
			endpoint := "pending.discord.media"
			conn.HandleVoiceServerUpdate(botgateway.EventVoiceServerUpdate{GuildID: 1, Token: "pending-token", Endpoint: &endpoint})
			select {
			case <-pending.started:
			case <-time.After(time.Second):
				t.Fatal("pending gateway open did not start")
			}
			switch action {
			case "replace":
				endpoint = "latest.discord.media"
				conn.HandleVoiceServerUpdate(botgateway.EventVoiceServerUpdate{GuildID: 1, Token: "latest-token", Endpoint: &endpoint})
				state := waitForServerGatewayOpen(t, gateway)
				if state.Endpoint != endpoint || state.Token != "latest-token" {
					t.Fatalf("opened a superseded voice server: %+v", state)
				}
			case "remove endpoint":
				conn.HandleVoiceServerUpdate(botgateway.EventVoiceServerUpdate{GuildID: 1})
				assertNoServerGatewayOpen(t, gateway)
			case "leave channel":
				conn.HandleVoiceStateUpdate(botgateway.EventVoiceStateUpdate{VoiceState: discord.VoiceState{GuildID: 1, UserID: 2}})
				endpoint = "late.discord.media"
				conn.HandleVoiceServerUpdate(botgateway.EventVoiceServerUpdate{GuildID: 1, Token: "late-token", Endpoint: &endpoint})
				assertNoServerGatewayOpen(t, gateway)
			}
			select {
			case <-pending.finished:
			default:
				t.Fatal("superseded gateway open was not cancelled and drained")
			}
		})
	}
}

func TestVoiceOpenStillRequiresFreshPair(t *testing.T) {
	for _, serverFirst := range []bool{false, true} {
		t.Run(map[bool]string{false: "state first", true: "server first"}[serverFirst], func(t *testing.T) {
			conn, gateway, _ := newServerUpdateTestConn(t)
			conn.voiceStateUpdateFunc = func(_ context.Context, _ snowflake.ID, channelID *snowflake.ID, _, _ bool) error {
				stateUpdate := func() {
					conn.HandleVoiceStateUpdate(botgateway.EventVoiceStateUpdate{VoiceState: discord.VoiceState{
						GuildID: 1, UserID: 2, ChannelID: channelID, SessionID: "fresh-session",
					}})
				}
				serverUpdate := func() {
					endpoint := "fresh.discord.media"
					conn.HandleVoiceServerUpdate(botgateway.EventVoiceServerUpdate{GuildID: 1, Token: "fresh-token", Endpoint: &endpoint})
				}
				if serverFirst {
					serverUpdate()
					assertNoServerGatewayOpen(t, gateway)
					stateUpdate()
				} else {
					stateUpdate()
					assertNoServerGatewayOpen(t, gateway)
					serverUpdate()
				}
				state := waitForServerGatewayOpen(t, gateway)
				if state.SessionID != "fresh-session" || state.Token != "fresh-token" || state.ChannelID != *channelID {
					t.Fatalf("explicit Open reused stale handshake data: %+v", state)
				}
				conn.openedFunc()
				return nil
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if err := conn.Open(ctx, 4, false, false); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func newServerUpdateTestConn(t *testing.T) (*connImpl, *serverUpdateGateway, *serverUpdateUDP) {
	t.Helper()
	gateway := &serverUpdateGateway{opened: make(chan State, 10)}
	udp := &serverUpdateUDP{}
	conn := &connImpl{
		config:  defaultConnConfig(),
		state:   State{GuildID: 1, UserID: 2},
		gateway: gateway,
		udp:     udp,
		voiceStateUpdateFunc: func(context.Context, snowflake.ID, *snowflake.ID, bool, bool) error {
			t.Error("server failover must not send a channel leave or join request")
			return nil
		},
		removeConnFunc: func() { t.Error("server failover must not unregister the connection") },
	}
	t.Cleanup(func() {
		conn.stateMu.Lock()
		defer conn.stateMu.Unlock()
		conn.closeGatewayLocked()
	})
	channelID := snowflake.ID(3)
	conn.HandleVoiceStateUpdate(botgateway.EventVoiceStateUpdate{VoiceState: discord.VoiceState{
		GuildID: 1, UserID: 2, ChannelID: &channelID, SessionID: "session",
	}})
	endpoint := "old.discord.media"
	conn.HandleVoiceServerUpdate(botgateway.EventVoiceServerUpdate{GuildID: 1, Token: "old-token", Endpoint: &endpoint})
	waitForServerGatewayOpen(t, gateway)
	gateway.mu.Lock()
	gateway.closes = 0
	gateway.mu.Unlock()
	udp.mu.Lock()
	udp.closes = 0
	udp.mu.Unlock()
	return conn, gateway, udp
}

func waitForServerGatewayOpen(t *testing.T, gateway *serverUpdateGateway) State {
	t.Helper()
	select {
	case state := <-gateway.opened:
		return state
	case <-time.After(time.Second):
		t.Fatal("voice gateway did not open")
		return State{}
	}
}

func assertNoServerGatewayOpen(t *testing.T, gateway *serverUpdateGateway) {
	t.Helper()
	select {
	case state := <-gateway.opened:
		t.Fatalf("unexpected voice gateway open: %+v", state)
	case <-time.After(20 * time.Millisecond):
	}
}

type serverUpdateGateway struct {
	Gateway
	mu        sync.Mutex
	connected bool
	closes    int
	opened    chan State
}

func (g *serverUpdateGateway) Open(_ context.Context, state State) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.connected {
		return discord.ErrGatewayAlreadyConnected
	}
	g.connected = true
	g.opened <- state
	return nil
}

func (g *serverUpdateGateway) Close() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.connected = false
	g.closes++
}

func (g *serverUpdateGateway) closeCount() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.closes
}

type serverUpdateUDP struct {
	UDPConn
	mu     sync.Mutex
	closes int
}

func (u *serverUpdateUDP) Close() error {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.closes++
	return nil
}

func (u *serverUpdateUDP) closeCount() int {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.closes
}

type pendingServerGateway struct {
	*serverUpdateGateway
	started  chan struct{}
	finished chan struct{}
}

func (g *pendingServerGateway) Open(ctx context.Context, state State) error {
	if state.Endpoint != "pending.discord.media" {
		return g.serverUpdateGateway.Open(ctx, state)
	}
	close(g.started)
	<-ctx.Done()
	close(g.finished)
	return ctx.Err()
}
