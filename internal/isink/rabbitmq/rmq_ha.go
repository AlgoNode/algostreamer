package rabbitmq

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	log "github.com/sirupsen/logrus"

	amqp "github.com/rabbitmq/amqp091-go"
)

func getReconnDelay() int {
	if os.Getenv("AMQP_RECONN_DELAY_SECONDS") == "" {
		return 1
	}
	delay, err := strconv.Atoi(os.Getenv("AMQP_RECONN_DELAY_SECONDS"))
	if err != nil {
		log.Errorln("Cannot convert env `AMQP_RECONN_DELAY_SECONDS` to a number, default to 1.")
		return 1
	}
	return delay
}

var delay = getReconnDelay() // reconnect after delay seconds

// Connection amqp.Connection wrapper
type Connection struct {
	*amqp.Connection
	log *logrus.Logger
}

// Channel wrap amqp.Connection.Channel, get a auto reconnect channel
func (c *Connection) Channel() (*Channel, error) {
	ch, err := c.Connection.Channel()
	if err != nil {
		return nil, err
	}

	channel := &Channel{
		Channel: ch,
		log:     c.log,
	}

	go func() {
		for {
			reason, ok := <-channel.Channel.NotifyClose(make(chan *amqp.Error))
			// exit this goroutine if closed by developer
			if !ok || channel.IsClosed() {
				c.log.Debug("channel closed")
				channel.Close() // close again, ensure closed flag set when connection closed
				break
			}
			c.log.Debugf("channel closed, reason: %v", reason)

			// reconnect if not closed by developer
			for {
				// wait 1s for connection reconnect
				time.Sleep(time.Duration(delay) * time.Second)

				ch, err := c.Connection.Channel()
				if err == nil {
					c.log.Debug("channel recreate success")
					channel.Lock()
					channel.Channel = ch
					channel.Unlock()
					break
				}

				c.log.Debugf("channel recreate failed, err: %v", err)
			}
		}

	}()

	return channel, nil
}

// Dial wrap amqp.Dial, dial and get a reconnect connection
func DialConfig(url string, config amqp.Config, log *logrus.Logger) (*Connection, error) {
	conn, err := amqp.DialConfig(url, config)
	if err != nil {
		for {
			// wait 1s for reconnect
			time.Sleep(time.Duration(delay) * time.Second)
			conn, err = amqp.DialConfig(url, config)
			if err == nil {
				break
			}
			log.Debugf("reconnect failed, err: %v", err)
		}
	}

	connection := &Connection{
		Connection: conn,
		log:        log,
	}

	go func() {
		for {
			reason, ok := <-connection.Connection.NotifyClose(make(chan *amqp.Error))
			// exit this goroutine if closed by developer
			if !ok {
				log.Debug("connection closed")
				break
			}
			log.Debugf("connection closed, reason: %v", reason)

			// reconnect if not closed by developer
			for {
				// wait 1s for reconnect
				time.Sleep(time.Duration(delay) * time.Second)

				conn, err := amqp.DialConfig(url, config)
				if err == nil {
					connection.Connection = conn
					log.Debugf("reconnect success")
					break
				}

				log.Debugf("reconnect failed, err: %v", err)
			}
		}
	}()

	return connection, nil
}

// DialCluster with reconnect
func DialConfigCluster(urls []string, config amqp.Config, log *logrus.Logger) (*Connection, error) {
	nodeSequence := 0
	conn, err := amqp.DialConfig(urls[nodeSequence], config)

	if err != nil {
		return nil, err
	}
	connection := &Connection{
		Connection: conn,
		log:        log,
	}

	go func(urls []string, seq *int) {
		for {
			reason, ok := <-connection.Connection.NotifyClose(make(chan *amqp.Error))
			if !ok {
				log.Debug("connection closed")
				break
			}
			log.Debugf("connection closed, reason: %v", reason)

			// reconnect with another node of cluster
			for {
				time.Sleep(time.Duration(delay) * time.Second)

				newSeq := next(urls, *seq)
				*seq = newSeq

				conn, err := amqp.DialConfig(urls[newSeq], config)
				if err == nil {
					connection.Connection = conn
					log.Debugf("reconnect success")
					break
				}

				log.Debugf("reconnect failed, err: %v", err)
			}
		}
	}(urls, &nodeSequence)

	return connection, nil
}

// Next element index of slice
func next(s []string, lastSeq int) int {
	length := len(s)
	if length == 0 || lastSeq == length-1 {
		return 0
	} else if lastSeq < length-1 {
		return lastSeq + 1
	} else {
		return -1
	}
}

// Channel amqp.Channel wapper
type Channel struct {
	sync.Mutex
	*amqp.Channel
	closed bool
	log    *logrus.Logger
}

// GetChannel returns current channel
func (ch *Channel) GetChannel() *amqp.Channel {
	ch.Lock()
	defer ch.Unlock()
	return ch.Channel
}

// IsClosed indicate closed by developer
func (ch *Channel) IsClosed() bool {
	ch.Lock()
	defer ch.Unlock()
	return ch.closed
}

// Close ensure closed flag set
func (ch *Channel) Close() error {
	if ch.IsClosed() {
		return amqp.ErrClosed
	}

	ch.Lock()
	defer ch.Unlock()
	ch.closed = true

	return ch.Channel.Close()
}

// Consume wrap amqp.Channel.Consume, the returned delivery will end only when channel closed by developer
func (ch *Channel) ReliableConsumeWithCtx(ctx context.Context, queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error) {
	deliveries := make(chan amqp.Delivery)

	go func() {
		for {
			d, err := ch.Channel.Consume(queue, consumer, autoAck, exclusive, noLocal, noWait, args)
			if err != nil {
				ch.log.Debugf("consume failed, err: %v", err)
				time.Sleep(time.Duration(delay) * time.Second)
				continue
			}

		RLOOP:
			for {
				select {
				case <-ctx.Done():
					close(deliveries)
					return
				case msg, ok := <-d:
					if !ok {
						break RLOOP
					}
					deliveries <- msg
				}
			}

			// sleep before IsClose call. closed flag may not set before sleep.
			time.Sleep(time.Duration(delay) * time.Second)

			if ch.IsClosed() {
				close(deliveries)
				break
			}
		}
	}()

	return deliveries, nil
}

func (ch *Channel) ReliablePublishWithContext(ctx context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error {
	for {
		if ch.IsClosed() {
			return fmt.Errorf("Channel is closed")
		}
		_, err := ch.GetChannel().PublishWithDeferredConfirmWithContext(ctx, exchange, key, mandatory, immediate, msg)
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return err
		}
		ch.log.Debugf("publish failed, err: %v", err)
		time.Sleep(time.Duration(delay) * time.Second)
	}
}
