package messaging

import (
	"context"
	"net"
	"testing"
	"time"
	"transfer-engine/internal/business/events"
)

func TestHandshakeRespectsCallerDeadline(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	accepted := make(chan net.Conn, 1)
	go func() {
		conn, err := listener.Accept()
		if err == nil {
			accepted <- conn
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- (Broker{URL: "amqp://guest:guest@" + listener.Addr().String() + "/"}).Publish(ctx, "test", events.Packet{})
	}()
	select {
	case conn := <-accepted:
		defer conn.Close() // Accept TCP but never answer the AMQP handshake.
	case <-time.After(2 * time.Second):
		t.Fatal("connection not established")
	}
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("stalled handshake succeeded")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("handshake ignored caller deadline")
	}
}
