// Package messaging owns AMQP transport and topology. Business never imports AMQP.
package messaging

import (
	"context"
	"crypto/tls"
	"log/slog"
	"net"
	"strconv"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"transfer-engine/internal/business/events"
)

type Broker struct {
	URL    string
	Logger *slog.Logger
}
type deadlineConn struct{ net.Conn }

func (c *deadlineConn) Write(p []byte) (int, error) {
	if err := c.SetWriteDeadline(time.Now().Add(5 * time.Second)); err != nil {
		return 0, err
	}
	return c.Conn.Write(p)
}
func (b Broker) dial(ctx context.Context) (*amqp.Connection, net.Conn, error) {
	var socket net.Conn
	connection, err := amqp.DialConfig(b.URL, amqp.Config{Heartbeat: 5 * time.Second, TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12}, Dial: func(network, address string) (net.Conn, error) {
		c, err := (&net.Dialer{Timeout: 5 * time.Second}).DialContext(ctx, network, address)
		if err != nil {
			return nil, err
		}
		socket = c
		deadline := time.Now().Add(5 * time.Second)
		if parent, ok := ctx.Deadline(); ok && parent.Before(deadline) {
			deadline = parent
		}
		_ = c.SetDeadline(deadline)
		return &deadlineConn{Conn: c}, nil
	}})
	if err != nil {
		if socket != nil {
			_ = socket.Close()
		}
		return nil, nil, events.ErrTransient
	}
	return connection, socket, nil
}

// Publish uses one bounded connection per confirmed message. This deliberately
// favors simple ownership over throughput; a return cannot be confused with a
// different concurrent publication. No transport retries hide unknown outcomes.
func (b Broker) Publish(ctx context.Context, queue string, p events.Packet) error {
	if envelope, err := events.Decode(p.Body); err == nil {
		ctx = otel.GetTextMapPropagator().Extract(ctx, propagation.MapCarrier{"traceparent": envelope.Traceparent})
	}
	ctx, span := otel.Tracer("transfer-engine/messaging").Start(ctx, "event publish", trace.WithSpanKind(trace.SpanKindProducer))
	defer span.End()
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	conn, socket, err := b.dial(ctx)
	if err != nil {
		return err
	}
	defer socket.Close()
	deadline, _ := ctx.Deadline()
	_ = socket.SetDeadline(deadline)
	stop := context.AfterFunc(ctx, func() { _ = socket.Close() })
	defer stop()
	channel, err := conn.Channel()
	if err != nil {
		return events.ErrHandoff
	}
	returns := channel.NotifyReturn(make(chan amqp.Return, 1))
	confirms := channel.NotifyPublish(make(chan amqp.Confirmation, 1))
	closed := channel.NotifyClose(make(chan *amqp.Error, 1))
	if err = channel.Confirm(false); err != nil {
		return events.ErrHandoff
	}
	expiration := ""
	if p.Delay > 0 {
		expiration = strconv.FormatInt(max(1, p.Delay.Milliseconds()), 10)
	}
	err = channel.PublishWithContext(ctx, "", queue, true, false, amqp.Publishing{ContentType: "application/json", DeliveryMode: amqp.Persistent, MessageId: p.ID, Timestamp: time.Now().UTC(), Body: p.Body, Expiration: expiration, Headers: amqp.Table{"attempt": int32(p.Attempt), "replays": int32(p.Replays), "failure_reason": p.Reason}})
	if err != nil {
		return events.ErrHandoff
	}
	select {
	case <-ctx.Done():
		return events.ErrHandoff
	case <-closed:
		return events.ErrHandoff
	case <-returns:
		return events.ErrHandoff
	case confirmation, ok := <-confirms:
		if !ok || !confirmation.Ack {
			return events.ErrHandoff
		}
		// RabbitMQ sends basic.return before basic.ack for an unroutable mandatory
		// publication; the library dispatches that return before the confirmation.
		select {
		case <-returns:
			return events.ErrHandoff
		default:
			return nil
		}
	}
}
func (b Broker) Declare(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	conn, socket, err := b.dial(ctx)
	if err != nil {
		return err
	}
	defer socket.Close()
	_ = socket.SetDeadline(time.Now().Add(10 * time.Second))
	stop := context.AfterFunc(ctx, func() { _ = socket.Close() })
	defer stop()
	ch, err := conn.Channel()
	if err != nil {
		return events.ErrTransient
	}
	args := func() amqp.Table {
		return amqp.Table{"x-queue-type": "quorum", "x-overflow": "reject-publish", "x-max-length": int32(10000)}
	}
	if _, err = ch.QueueDeclare(events.DeadQueue, true, false, false, false, args()); err != nil {
		return events.ErrTransient
	}
	main := args()
	main["x-dead-letter-exchange"] = ""
	main["x-dead-letter-routing-key"] = events.DeadQueue
	main["x-dead-letter-strategy"] = "at-least-once"
	main["x-delivery-limit"] = int32(20)
	if _, err = ch.QueueDeclare(events.MainQueue, true, false, false, false, main); err != nil {
		return events.ErrTransient
	}
	for i := 1; i <= 3; i++ {
		retry := args()
		retry["x-dead-letter-exchange"] = ""
		retry["x-dead-letter-routing-key"] = events.MainQueue
		retry["x-dead-letter-strategy"] = "at-least-once"
		if _, err = ch.QueueDeclare(events.RetryQueue(i), true, false, false, false, retry); err != nil {
			return events.ErrTransient
		}
	}
	return nil
}

type Delivery struct{ raw amqp.Delivery }

func (d Delivery) Ack() error {
	if err := d.raw.Ack(false); err != nil {
		return events.ErrTransient
	}
	return nil
}
func (d Delivery) Packet() events.Packet {
	return events.Packet{Body: d.raw.Body, ID: d.raw.MessageId, Attempt: headerInt(d.raw.Headers["attempt"]), Replays: headerInt(d.raw.Headers["replays"]), Reason: headerString(d.raw.Headers["failure_reason"])}
}
func headerInt(v any) int {
	switch x := v.(type) {
	case nil:
		return 0
	case int32:
		if x >= 0 && x <= 100 {
			return int(x)
		}
	case int64:
		if x >= 0 && x <= 100 {
			return int(x)
		}
	case int:
		if x >= 0 && x <= 100 {
			return x
		}
	}
	return -1
}
func headerString(v any) string {
	value, _ := v.(string)
	if len(value) > 100 {
		return "invalid_metadata"
	}
	return value
}

// Consume owns one channel/connection per worker. Closing it requeues all
// unacknowledged deliveries, including those prefetched but not dispatched.
func (b Broker) Consume(ctx context.Context, prefetch int, handler func(context.Context, events.Packet, events.Delivery) error) error {
	conn, socket, err := b.dial(ctx)
	if err != nil {
		return err
	}
	defer socket.Close()
	_ = socket.SetDeadline(time.Now().Add(5 * time.Second))
	ch, err := conn.Channel()
	if err != nil {
		return events.ErrTransient
	}
	if err = ch.Qos(prefetch, 0, false); err != nil {
		return events.ErrTransient
	}
	deliveries, err := ch.Consume(events.MainQueue, "", false, false, false, false, nil)
	if err != nil {
		return events.ErrTransient
	}
	_ = socket.SetDeadline(time.Time{})
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case raw, ok := <-deliveries:
			if !ok {
				return events.ErrTransient
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
			work, cancel := context.WithTimeout(context.WithoutCancel(ctx), 6*time.Second)
			if envelope, decodeErr := events.Decode(raw.Body); decodeErr == nil {
				work = otel.GetTextMapPropagator().Extract(work, propagation.MapCarrier{"traceparent": envelope.Traceparent})
			}
			work, span := otel.Tracer("transfer-engine/messaging").Start(work, "transfer.completed consume", trace.WithSpanKind(trace.SpanKindConsumer))
			err = handler(work, Delivery{raw}.Packet(), Delivery{raw})
			outcome := "handled"
			if err != nil {
				outcome = "unconfirmed"
			}
			if b.Logger != nil {
				sc := span.SpanContext()
				b.Logger.InfoContext(work, "message_handled", "trace_id", sc.TraceID().String(), "span_id", sc.SpanID().String(), "outcome", outcome)
			}
			span.SetAttributes(attribute.String("messaging.outcome", outcome))
			span.End()
			counter, _ := otel.Meter("transfer-engine/messaging").Int64Counter("messaging.deliveries")
			counter.Add(work, 1, metric.WithAttributes(attribute.String("outcome", outcome)))
			cancel()
			if err != nil {
				return err
			} // No immediate requeue loop; supervisor backs off.
		}
	}
}

type ReplayPreview struct {
	EventID          string `json:"event_id"`
	Valid            bool   `json:"valid"`
	Attempt, Replays int
	Reason           string `json:"failure_reason"`
}

func (b Broker) ReplayBatch(ctx context.Context, limit int, request events.ReplayRequest, audit events.ReplayAudit) ([]ReplayPreview, error) {
	if limit < 1 || limit > 100 {
		return nil, events.ErrPermanent
	}
	conn, socket, err := b.dial(ctx)
	if err != nil {
		return nil, err
	}
	defer socket.Close()
	stop := context.AfterFunc(ctx, func() { _ = socket.Close() })
	defer stop()
	_ = socket.SetDeadline(time.Now().Add(30 * time.Second))
	ch, err := conn.Channel()
	if err != nil {
		return nil, events.ErrTransient
	}
	var previews []ReplayPreview
	for i := 0; i < limit; i++ {
		raw, ok, err := ch.Get(events.DeadQueue, false)
		if err != nil {
			return previews, events.ErrTransient
		}
		if !ok {
			break
		}
		delivery := Delivery{raw}
		p := delivery.Packet()
		e, decodeErr := events.Decode(p.Body)
		previews = append(previews, ReplayPreview{EventID: e.ID, Valid: decodeErr == nil, Attempt: p.Attempt, Replays: p.Replays, Reason: p.Reason})
		if request.DryRun {
			continue
		} // Connection close returns inspected originals.
		if err = events.Replay(ctx, p, delivery, b, audit, request); err != nil {
			return previews, err
		}
	}
	return previews, nil
}
