package main

import (
	"errors"
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

const (
	WORKER_ID_BITS     = 5
	DATACENTER_ID_BITS = 5
	SEQUENCE_BITS      = 12
	TIMESTAMP_BITS     = 41

	MAX_DATACENTER_ID = (1 << DATACENTER_ID_BITS) - 1

	WORKER_ID_SHIFT       = SEQUENCE_BITS
	TIMESTAMPT_LEFT_SHIFT = SEQUENCE_BITS + WORKER_ID_BITS + DATACENTER_ID_BITS
	DATACENTER_ID_SHIFT   = SEQUENCE_BITS + WORKER_ID_BITS

	SEQUENCE_MASK = (1 << SEQUENCE_BITS) - 1

	EPOCH = 1288834974657
)

type Worker struct {
	lastTimeStamp int64
	sequence      int64

	datacenter_id int64
	nodeID        int64
	counter       int64

	mutx sync.Mutex
}

func (w *Worker) nextID() (int64, error) {
	w.mutx.Lock()
	defer w.mutx.Unlock()

	currentTimeStamp := time.Now().UnixMilli()
	if currentTimeStamp == w.lastTimeStamp {
		w.sequence = (w.sequence + 1) & SEQUENCE_MASK

		if w.sequence == 0 {
			for currentTimeStamp <= w.lastTimeStamp {
				currentTimeStamp = time.Now().UnixMilli()
			}
		} else {
			w.sequence = 0
		}
	}

	if currentTimeStamp < w.lastTimeStamp {
		return -1, errors.New("clock is running backwards")
	}

	w.lastTimeStamp = currentTimeStamp

	flake_id := ((currentTimeStamp - EPOCH) << TIMESTAMPT_LEFT_SHIFT) |
		(w.datacenter_id << DATACENTER_ID_SHIFT) |
		(w.nodeID << WORKER_ID_SHIFT) |
		w.sequence + w.counter

	w.counter += 1

	return flake_id, nil
}

func main() {
	node := maelstrom.NewNode()

	worker := Worker{
		lastTimeStamp: -1,
		datacenter_id: int64(rand.Intn(MAX_DATACENTER_ID)),
	}

	var once sync.Once

	node.Handle("generate", func(msg maelstrom.Message) error {
		once.Do(func() {
			var strValue string = string(msg.Dest[1])
			value, _ := strconv.Atoi(strValue)
			worker.nodeID += int64(value)
		})

		id, _ := worker.nextID()

		var body = map[string]any{
			"type": "generate_ok",
			"id":   fmt.Sprintf("%v-%v", worker.nodeID, id),
		}

		return node.Reply(msg, body)
	})

	if err := node.Run(); err != nil {
		log.Fatal("Error: ", err)
	}
}
