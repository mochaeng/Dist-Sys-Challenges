package main

import (
	"encoding/json"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Worker struct {
	values []int
	ID     string
}

func (w *Worker) AddValue(value int) {
	w.values = append(w.values, value)
}

type BroadcastRequest struct {
	Value int `json:"message"`
}

type TopologyRequest struct {
	Topology map[string][]string `json:"topology"`
}

func main() {
	node := maelstrom.NewNode()
	worker := Worker{values: []int{}}

	node.Handle("broadcast", func(msg maelstrom.Message) error {
		broadcastMSG := BroadcastRequest{}
		json.Unmarshal(msg.Body, &broadcastMSG)

		worker.AddValue(broadcastMSG.Value)

		response := map[string]any{
			"type": "broadcast_ok",
		}

		return node.Reply(msg, response)
	})

	node.Handle("read", func(msg maelstrom.Message) error {
		response := map[string]any{
			"type":     "read_ok",
			"messages": worker.values,
		}

		return node.Reply(msg, response)
	})

	node.Handle("topology", func(msg maelstrom.Message) error {
		topologyMSG := TopologyRequest{}

		if err := json.Unmarshal(msg.Body, &topologyMSG); err != nil {
			return err
		}

		response := map[string]any{
			"type": "topology_ok",
		}

		return node.Reply(msg, response)
	})

	if err := node.Run(); err != nil {
		log.Fatal("Error: ", err)
	}

}
