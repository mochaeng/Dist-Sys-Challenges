package main

import (
	"encoding/json"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Worker struct {
	values     []int
	Know       map[string]map[int]bool
	SeenValues map[int]bool
	ID         string
	Neighboors []string

	ValMutex  sync.Mutex
	TopoMutex sync.Mutex
}

func (w *Worker) AddValue(value int) {
	w.SeenValues[value] = true
	w.values = append(w.values, value)
}

type BroadcastRequest struct {
	TP    string `json:"type"`
	Value int    `json:"message"`
	ID    *int   `json:"msg_id"`
}

type TopologyRequest struct {
	Topology map[string][]string `json:"topology"`
}

func main() {
	node := maelstrom.NewNode()

	worker := Worker{
		values:     []int{},
		Know:       make(map[string]map[int]bool),
		SeenValues: make(map[int]bool),
		Neighboors: []string{},
	}

	var once sync.Once

	node.Handle("broadcast", func(msg maelstrom.Message) error {
		broadcastMSG := BroadcastRequest{}
		json.Unmarshal(msg.Body, &broadcastMSG)

		worker.ValMutex.Lock()
		_, ok := worker.SeenValues[broadcastMSG.Value]
		if !ok {
			worker.AddValue(broadcastMSG.Value)
		}
		_, ok = worker.Know[msg.Src]
		if !ok {
			worker.Know[msg.Src] = make(map[int]bool)
		}
		worker.Know[msg.Src][broadcastMSG.Value] = true
		worker.ValMutex.Unlock()

		once.Do(func() {
			go func() {
				ticker := time.Tick(300 * time.Millisecond)
				for range ticker {
					worker.ValMutex.Lock()

					for _, value := range worker.values {
						msgToGoosip := BroadcastRequest{
							TP:    "broadcast",
							Value: value,
						}
						for _, neighboorID := range worker.Neighboors {
							if !worker.Know[neighboorID][value] {
								node.Send(neighboorID, msgToGoosip)
							}
						}
					}

					worker.ValMutex.Unlock()
				}

			}()
		})

		response := map[string]any{
			"type": "broadcast_ok",
		}

		if broadcastMSG.ID != nil {
			return node.Reply(msg, response)
		}
		return nil
	})

	node.Handle("read", func(msg maelstrom.Message) error {
		worker.ValMutex.Lock()
		values := worker.values
		worker.ValMutex.Unlock()

		response := map[string]any{
			"type":     "read_ok",
			"messages": values,
		}

		return node.Reply(msg, response)
	})

	node.Handle("topology", func(msg maelstrom.Message) error {
		topologyMSG := TopologyRequest{}
		if err := json.Unmarshal(msg.Body, &topologyMSG); err != nil {
			return err
		}

		worker.TopoMutex.Lock()
		worker.Neighboors = topologyMSG.Topology[node.ID()]
		worker.TopoMutex.Unlock()

		response := map[string]any{
			"type": "topology_ok",
		}

		return node.Reply(msg, response)
	})

	if err := node.Run(); err != nil {
		log.Fatal("Error: ", err)
	}

}
