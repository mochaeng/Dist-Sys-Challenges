package main

import (
	"errors"
	"fmt"
	"log"
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

func main() {
	n := maelstrom.NewNode()

	var lastTimeStamp int64 = -1
	var sequence int64 = 0
	// var datacenter_id int64 = int64(rand.Intn(MAX_DATACENTER_ID))
	var datacenter_id int64 = 3
	var nodeID int64 = 0

	var mutx sync.Mutex
	var once sync.Once

	var counter int64 = 0

	n.Handle("generate", func(msg maelstrom.Message) error {
		mutx.Lock()
		defer mutx.Unlock()

		once.Do(func() {
			var strValue string = string(msg.Dest[1])
			value, _ := strconv.Atoi(strValue)
			nodeID += int64(value)
		})

		currentTimeStamp := time.Now().UnixMilli()

		if currentTimeStamp == lastTimeStamp {
			sequence = (sequence + 1) & SEQUENCE_MASK

			if sequence == 0 {
				for currentTimeStamp <= lastTimeStamp {
					currentTimeStamp = time.Now().UnixMilli()
				}
			} else {
				sequence = 0
			}
		}

		if currentTimeStamp < lastTimeStamp {
			return errors.New("clock is running backwards")
		}

		lastTimeStamp = currentTimeStamp

		flake_id := ((currentTimeStamp - EPOCH) << TIMESTAMPT_LEFT_SHIFT) |
			(datacenter_id << DATACENTER_ID_SHIFT) |
			(nodeID << WORKER_ID_SHIFT) |
			sequence + counter

		counter += 1

		var body = map[string]any{
			"type": "generate_ok",
			"id":   fmt.Sprintf("%v-%v", nodeID, flake_id),
		}

		return n.Reply(msg, body)
	})

	if err := n.Run(); err != nil {
		log.Fatal("Error: ", err)
	}
}
