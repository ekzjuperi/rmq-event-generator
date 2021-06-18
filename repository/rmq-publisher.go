package repository

import (
	"encoding/json"
	"fmt"
	"runtime/debug"
	"sync"
	"time"

	"github.com/golang/glog"
	"github.com/streadway/amqp"
)

const (
	tryPublishMax = 5
	reconnectsMax = 10
)

// RabbitMQ publisher with reconnects on connection fail.
type Publisher struct {
	URI string

	chann *amqp.Channel
	conn  *amqp.Connection

	sync.Mutex
}

// Create new Publisher.
func NewPublisher(rabbitmqURI string) *Publisher {
	rmqPublisher := &Publisher{
		URI: rabbitmqURI,
	}

	return rmqPublisher
}

// Initialize RabbitMQ channel.
func (p *Publisher) InitChannel() error {
	conn, err := amqp.Dial(p.URI)
	if err != nil {
		errString := "Publisher.InitChannel() amqp.Dial(%s) error: %v"
		errMessage := fmt.Errorf(errString, p.URI, err)

		return errMessage
	}

	if conn == nil {
		errString := "Publisher.InitChannel() amqp.Dial(%s) connection is nil"
		errMessage := fmt.Errorf(errString, p.URI)

		return errMessage
	}

	p.conn = conn

	channel, err := p.conn.Channel()
	if err != nil {
		errString := "Publisher.InitChannel() conn.Channel(%s) error: %v"
		errMessage := fmt.Errorf(errString, p.URI, err)

		return errMessage
	}

	p.chann = channel

	return nil
}

// Send message to RabbitMQ exchange.
func (p *Publisher) processMessage(exchangeName, key string, messageJSON []byte) error {
	// retry to send message to RabbitMQ
	var i int

	for {
		i++

		err := p.publishToChannel(exchangeName, key, messageJSON)
		if err != nil {
			switch i {
			case tryPublishMax:
				return fmt.Errorf("%d publish fail (%s), last error: %v", tryPublishMax, messageJSON, err)
			default:
				glog.Errorf("Publisher.processMessage() push message to rmq %s.%s (%v) error: %v",
					exchangeName, key, messageJSON, err)
			}

			p.Reconnect()

			// retry to send message after reconnect
			continue
		}

		// message successfully sent
		break
	}

	return nil
}

// Try reconnect to RabbitMQ.
func (p *Publisher) Reconnect() error {
	defer func() {
		if r := recover(); r != nil {
			glog.Errorf("Reconnect recovery: %v stack: %v", r, string(debug.Stack()))
		}
	}()

	p.Lock()
	defer p.Unlock()

	var i int

	for {
		i++

		err := p.InitChannel()
		if err != nil {
			switch i {
			case reconnectsMax:
				return fmt.Errorf("all %d Publisher.Reconnect() fail, last error: %v", reconnectsMax, err)
			default:
				glog.Errorf("Publisher.Reconnect() initialize RabbitMQ connection: %v", err)
			}

			time.Sleep(reconnectsMax)

			// try to reconnect again
			continue
		}

		// successfully reconnected
		break
	}

	return nil
}

// Publish message to RabbitMQ exchange.
func (p *Publisher) publishToChannel(exchangeName, key string, msg []byte) error {
	message := amqp.Publishing{
		Headers:         amqp.Table{},
		ContentType:     "application/json",
		ContentEncoding: "",
		Body:            msg,
		DeliveryMode:    amqp.Persistent,
	}
	err := p.chann.Publish(
		exchangeName,
		key,
		false,
		false,
		message,
	)

	if err != nil {
		errString := "Publisher.publishToChannel() publish message to rmq - message (%s) exchange %s, error: %v"
		return fmt.Errorf(errString, string(msg), exchangeName, err)
	}

	// message successfully sent
	return nil
}

// Marshal struct to json and try publish it to RabbitMQ exchange.
func (p *Publisher) PublishMessage(exchangeName, key string, message interface{}) error {
	messageJSON, err := json.Marshal(message)
	if err != nil {
		errString := "Publisher.PublishMessage() encode message struct to json: %v"
		return fmt.Errorf(errString, err)
	}

	return p.processMessage(exchangeName, key, messageJSON)
}

// Close connection to RabbitMQ.
func (p *Publisher) CloseChannel() {
	defer func() {
		if r := recover(); r != nil {
			glog.Errorf("CloseChannel recovery: %v stack: %v", r, string(debug.Stack()))
		}
	}()

	if p.chann != nil {
		p.chann.Close()
		p.chann = nil
	}

	if p.conn != nil {
		p.conn.Close()
		p.conn = nil
	}
}
