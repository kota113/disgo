package voice

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestReconnectDelay(t *testing.T) {
	tests := []struct {
		attempt int
		want    time.Duration
	}{
		{attempt: 0, want: 0},
		{attempt: 1, want: time.Second},
		{attempt: 2, want: 2 * time.Second},
		{attempt: 3, want: 4 * time.Second},
		{attempt: 4, want: 8 * time.Second},
		{attempt: 5, want: maximumConnectDelay},
		{attempt: 100, want: maximumConnectDelay},
	}

	for _, tt := range tests {
		if got := reconnectDelay(tt.attempt); got != tt.want {
			t.Errorf("reconnectDelay(%d) = %v, want %v", tt.attempt, got, tt.want)
		}
	}
}

func TestNonReconnectableCloseCodesDoNotStartNewConnection(t *testing.T) {
	for _, closeCode := range []GatewayCloseEventCode{
		GatewayCloseEventCodeDisconnected,
		GatewayCloseEventCodeRateLimited,
		GatewayCloseEventCodeCallTerminated,
	} {
		if closeCode.Reconnect || closeCode.NewConnection {
			t.Errorf("close code %d must not reconnect: %+v", closeCode.Code, closeCode)
		}
	}
}

func TestSendDoesNotBlockStatusWhileWaitingForConnection(t *testing.T) {
	gateway := &gatewayImpl{status: StatusReady}
	gateway.connMu.Lock()

	sendStarted := make(chan struct{})
	sendDone := make(chan error, 1)
	go func() {
		close(sendStarted)
		sendDone <- gateway.Send(context.Background(), OpcodeHeartbeat, GatewayMessageDataHeartbeat{})
	}()
	<-sendStarted

	// Give Send time to reach the connection lock held by this test.
	time.Sleep(10 * time.Millisecond)

	statusDone := make(chan Status, 1)
	go func() {
		statusDone <- gateway.Status()
	}()

	statusBlocked := false
	select {
	case status := <-statusDone:
		if status != StatusReady {
			t.Errorf("unexpected gateway status: %v", status)
		}
	case <-time.After(100 * time.Millisecond):
		statusBlocked = true
	}

	gateway.connMu.Unlock()

	if err := <-sendDone; !errors.Is(err, ErrGatewayNotConnected) {
		t.Errorf("expected ErrGatewayNotConnected, got %v", err)
	}
	if statusBlocked {
		<-statusDone
		t.Fatal("Status was blocked by Send while Send was waiting for the connection lock")
	}
}
