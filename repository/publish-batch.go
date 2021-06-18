package repository

import (
	"sync"

	"github.com/ekzjuperi/rmq-event-generator/models"
	"github.com/golang/glog"
)

// PublishBatchToRmq агтс publishes batch of event to rep.
func PublishBatchToRmq(
	wg *sync.WaitGroup,
	publisher *Publisher,
	eventChan chan models.Event,
	exchangeName string,
	routingKey string,
	batchSize int,
) {
	defer wg.Done()

	event := models.Event{}

	batch := make([]models.Event, 0)

	for {
		if len(batch) < batchSize {
			event, ok := <-eventChan

			if !ok {
				break
			}

			batch = append(batch, event)
		} else {
			err := publisher.PublishMessage(exchangeName, routingKey, batch)

			if err != nil {
				glog.Fatalf("publisher.PublishMessage() to queue:%v Error: %v", event, err)
			}

			batch = make([]models.Event, 0)
		}
	}

	if len(batch) > 0 {
		err := publisher.PublishMessage(exchangeName, routingKey, batch)

		if err != nil {
			glog.Fatalf("publisher.PublishMessage() to queue:%v Error: %v", event, err)
		}
	}
}
