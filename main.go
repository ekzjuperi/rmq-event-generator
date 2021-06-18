package main

import (
	"flag"
	"runtime/debug"
	"sync"
	"time"

	"github.com/ekzjuperi/rmq-event-generator/models"
	rep "github.com/ekzjuperi/rmq-event-generator/repository"
	"github.com/golang/glog"
)

var (
	batchSize, workerGenerate, workerPublish, eventLimit int
	eventCounter                                         int32
	exchangeName, routingKey                             string
)

func main() {
	defer func() {
		if r := recover(); r != nil {
			glog.Errorln(r, debug.Stack(), "Main")
		}

		glog.Flush()
	}()

	flag.IntVar(&batchSize, "b", 10000, "Batch size")
	flag.IntVar(&eventLimit, "l", 10000000, "Maximum number of generated messages")
	flag.IntVar(&workerGenerate, "wg", 2, "workers count for generate events")
	flag.IntVar(&workerPublish, "wp", 2, "workers count for publish batches")
	flag.StringVar(&exchangeName, "e", "actions", "rmq exchange name")
	flag.StringVar(&routingKey, "r", "actions", "rmq routing key")
	flag.Parse()

	glog.Infoln("Script started")

	beginTime := time.Now()

	publisher := rep.NewPublisher("amqp://user:password@0.0.0.0:5672/sp")

	err := publisher.InitChannel()
	if err != nil {
		glog.Fatalf("publisher.InitChannel() Error: %v", err)
	}

	defer publisher.CloseChannel()

	eventChan := make(chan models.Event, 100)
	eventGeneratingWorkers := &sync.WaitGroup{}
	eventLimitsForWorker := rep.GetLimitsForWorkers(eventLimit, workerGenerate)

	for i := 0; i < workerGenerate; i++ {
		eventGeneratingWorkers.Add(1)

		go rep.GenerationEvent(eventGeneratingWorkers, eventChan, &eventCounter, eventLimitsForWorker[i])
	}

	eventPublishWorker := &sync.WaitGroup{}

	var publisherPool []*rep.Publisher

	for i := 0; i < workerPublish; i++ {
		publisher := rep.NewPublisher("amqp://user:pass@0.0.0.0:5672/sp")

		err := publisher.InitChannel()
		if err != nil {
			glog.Fatalf("publisher.InitChannel() Error: %v", err)
		}

		publisherPool = append(publisherPool, publisher)
	}

	defer func() {
		for i := 0; i < workerPublish; i++ {
			publisherPool[i].CloseChannel()
		}
	}()

	for i := 0; i < workerPublish; i++ {
		go rep.PublishBatchToRmq(eventPublishWorker, publisherPool[i], eventChan, exchangeName, routingKey, batchSize)

		eventPublishWorker.Add(1)
	}

	eventGeneratingWorkers.Wait()

	close(eventChan)

	eventPublishWorker.Wait()

	glog.Infof("Total generates event : %v\n", eventCounter)
	glog.Infof("Script done in: %v\n", time.Since(beginTime))
}
