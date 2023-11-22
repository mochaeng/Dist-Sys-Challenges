package main

import (
	"encoding/json"
	"log"
	"sync"
	"time"

	mapset "github.com/deckarep/golang-set/v2"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type BroadcastRequest struct {
	TP     string `json:"type"`
	Value  int    `json:"message"`
	Values []int  `json:"messages"`
	ID     *int   `json:"msg_id"`
}

type TopologyRequest struct {
	Topology map[string][]string `json:"topology"`
}

type Worker struct {
	Node         *maelstrom.Node
	Values       mapset.Set[int]
	ValuesKnowBy map[string]mapset.Set[int]

	SeenValues map[int]bool
	ID         string
	Neighbors  []string

	ValMutex  sync.Mutex
	TopoMutex sync.Mutex
}

func (w *Worker) AddValue(value int) {
	w.Values.Add(value)
}

func (w *Worker) AddValues(values []int) {
	for _, value := range values {
		w.Values.Add(value)
	}
}

func (w *Worker) AddValuesKnowByNeighbor(neighborID string, values []int) {
	for _, value := range values {
		w.ValuesKnowBy[neighborID].Add(value)
	}
}

func (w *Worker) HandleValueSendByClient(neighboorID string, value int) {
	w.Values.Add(value)

	w.ValMutex.Lock()
	defer w.ValMutex.Unlock()

	_, ok := w.ValuesKnowBy[neighboorID]
	if !ok {
		w.ValuesKnowBy[neighboorID] = mapset.NewSet[int]()
	}
	w.ValuesKnowBy[neighboorID].Add(value)
}

func (w *Worker) HandleValuesSendByWorker(neighboorID string, values []int) {
	w.AddValues(values)

	w.ValMutex.Lock()
	defer w.ValMutex.Unlock()

	_, ok := w.ValuesKnowBy[neighboorID]
	if !ok {
		w.ValuesKnowBy[neighboorID] = mapset.NewSet[int]()
	}
	w.AddValuesKnowByNeighbor(neighboorID, values)
}

func (w *Worker) onHandleInit(msg maelstrom.Message) error {
	go func() {
		ticker := time.Tick(time.Duration(25 * time.Millisecond))
		for range ticker {

			for _, neighborID := range w.Neighbors {
				neighbor := neighborID

				w.ValMutex.Lock()
				valuesKnowBy := w.ValuesKnowBy[neighbor]
				w.ValMutex.Unlock()

				valuesToSend := w.Values.Difference(valuesKnowBy).ToSlice()

				if len(valuesToSend) == 0 {
					continue
				}

				msgToGoosip := BroadcastRequest{
					TP:     "broadcast",
					Values: valuesToSend,
				}

				w.Node.RPC(neighbor, msgToGoosip, func(msg maelstrom.Message) error {
					w.ValMutex.Lock()
					w.AddValuesKnowByNeighbor(msg.Src, valuesToSend)
					w.ValMutex.Unlock()
					return nil
				})
			}

		}
	}()

	return nil
}

func (w *Worker) onHandleBroadcast(msg maelstrom.Message) error {
	broadcastMSG := BroadcastRequest{}
	json.Unmarshal(msg.Body, &broadcastMSG)

	if broadcastMSG.Values != nil {
		w.HandleValuesSendByWorker(msg.Src, broadcastMSG.Values)
	} else {
		w.HandleValueSendByClient(msg.Src, broadcastMSG.Value)
	}

	response := map[string]any{
		"type": "broadcast_ok",
	}

	return w.Node.Reply(msg, response)
}

func (w *Worker) onHandleTopology(msg maelstrom.Message) error {
	topologyMSG := TopologyRequest{}
	if err := json.Unmarshal(msg.Body, &topologyMSG); err != nil {
		return err
	}

	w.TopoMutex.Lock()
	w.Neighbors = topologyMSG.Topology[w.Node.ID()]
	w.TopoMutex.Unlock()

	w.ValMutex.Lock()
	for _, neighborID := range w.Neighbors {
		w.ValuesKnowBy[neighborID] = mapset.NewSet[int]()
	}
	w.ValMutex.Unlock()

	response := map[string]any{
		"type": "topology_ok",
	}

	return w.Node.Reply(msg, response)
}

func (w *Worker) onHandleRead(msg maelstrom.Message) error {
	values := w.Values.ToSlice()

	response := map[string]any{
		"type":     "read_ok",
		"messages": values,
	}

	return w.Node.Reply(msg, response)
}

func main() {
	node := maelstrom.NewNode()

	worker := Worker{
		Node:         node,
		Values:       mapset.NewSet[int](),
		ValuesKnowBy: make(map[string]mapset.Set[int]),
		SeenValues:   make(map[int]bool),
		Neighbors:    []string{},
	}

	worker.Node.Handle("init", worker.onHandleInit)
	worker.Node.Handle("broadcast", worker.onHandleBroadcast)
	worker.Node.Handle("topology", worker.onHandleTopology)
	worker.Node.Handle("read", worker.onHandleRead)

	if err := worker.Node.Run(); err != nil {
		log.Fatal("Error: ", err)
	}

}
