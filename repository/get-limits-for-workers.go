package repository

// GetLimitsForGenerationsWorkers func get limits for workers thats generates event.
func GetLimitsForWorkers(eventLimit, workerGenerate int) []int {
	eventLimitsForWorker := make([]int, 0)

	limit := eventLimit / workerGenerate

	if (workerGenerate % 2) == 0 {
		for i := 0; i < workerGenerate; i++ {
			eventLimitsForWorker = append(eventLimitsForWorker, limit)
		}
	} else {
		for i := 0; i < (workerGenerate - 1); i++ {
			eventLimitsForWorker = append(eventLimitsForWorker, limit)
		}
		eventLimitsForWorker = append(eventLimitsForWorker, (eventLimit - (limit * (workerGenerate - 1))))
	}

	return eventLimitsForWorker
}
