package repository

import (
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ekzjuperi/rmq-event-generator/models"
	"github.com/golang/glog"
	murh "github.com/spaolacci/murmur3"
)

var (
	multiplay   int32 = 1000_000
	letterRunes       = []rune("abcdefghijklmnopqrstuvwxyz")
)

// GenerationEvent func generates events.
func GenerationEvent(wg *sync.WaitGroup, eventChan chan models.Event, eventCounter *int32, eventLimitForWorker int) {
	defer wg.Done()

	count := 0

	for {
		if count == eventLimitForWorker {
			return
		}

		ts := time.Now().Unix()
		pid := GetRandomPID()

		event := models.Event{
			TimeStamp: ts,
			ProfileID: pid,
		}

		eventChan <- event

		count++

		atomic.AddInt32(eventCounter, 1)

		remainder := atomic.LoadInt32(eventCounter) % multiplay
		if remainder == 0 {
			glog.Infof("The script generate %v events ", atomic.LoadInt32(eventCounter))
		}
	}
}

// GetRandomPID func get random profile id.
func GetRandomPID() int64 {
	b := make([]rune, 5)

	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}

	return GetMurmur3Int64Pk(fmt.Sprintf(string(b), "@gmail.com"))
}

// Get murmur3 64 hash from data to lower string.
func GetMurmur3Int64Pk(data string) int64 {
	data = strings.ToLower(data)
	return int64(murh.Sum64([]byte(data)))
}
