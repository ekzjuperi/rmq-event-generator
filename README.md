# rmq-event-generator
Script for generate event and sending them to the RabbitMQ exchange in multiple goroutines.
    flags:
        -b Batch size (default 10_000)
	    -l Maximum number of generated messages (default 10_000_000)
	    -wg Number of workers for generate events (default 2)
	    -wp Number of workers for publish batches to rmq (default 2)
	    -e RabbitMQ exchange name (default "actions")
	    -r RabbitMQ routing key (default "actions")