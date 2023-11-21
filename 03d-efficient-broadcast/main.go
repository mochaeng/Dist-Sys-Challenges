package main

import (
	"encoding/json"
	"log"
	"sync"
	"time"

	mapset "github.com/deckarep/golang-set/v2"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type Worker struct {
	values       mapset.Set[int]
	ValuesKnowBy map[string]mapset.Set[int]

	SeenValues map[int]bool
	ID         string
	Neighbors  []string

	ValMutex  sync.Mutex
	TopoMutex sync.Mutex
}

func (w *Worker) AddValue(value int) {
	w.values.Add(value)
}

func (w *Worker) AddValues(values []int) {
	for _, value := range values {
		w.values.Add(value)
	}
}

func (w *Worker) AddValuesKnowByNeighbor(neighborID string, values []int) {
	for _, value := range values {
		w.ValuesKnowBy[neighborID].Add(value)
	}
}

func (w *Worker) HandleValueSendByClient(neighboorID string, value int) {
	w.values.Add(value)

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

type BroadcastRequest struct {
	TP     string `json:"type"`
	Value  int    `json:"message"`
	Values []int  `json:"messages"`
	ID     *int   `json:"msg_id"`
}

type TopologyRequest struct {
	Topology map[string][]string `json:"topology"`
}

func (w *Worker) SendRPCWithRetries(numRetries int) {
	for i := 0; i < numRetries; i++ {
	}
}

func main() {
	node := maelstrom.NewNode()

	worker := Worker{
		values:       mapset.NewSet[int](),
		ValuesKnowBy: make(map[string]mapset.Set[int]),
		SeenValues:   make(map[int]bool),
		Neighbors:    []string{},
	}

	node.Handle("init", func(msg maelstrom.Message) error {
		go func() {

			ticker := time.Tick(time.Duration(115) * time.Millisecond)
			for range ticker {

				for _, neighborID := range worker.Neighbors {
					// worker.ValMutex.Lock()
					// if _, ok := worker.ValuesKnowBy[neighborID]; !ok {
					// 	continue
					// }
					diffSet := worker.values.Difference(worker.ValuesKnowBy[neighborID])
					// worker.ValMutex.Unlock()
					valuesToSend := diffSet.ToSlice()

					if len(valuesToSend) == 0 {
						continue
					}

					msgToGoosip := BroadcastRequest{
						TP:     "broadcast",
						Values: valuesToSend,
					}
					node.Send(neighborID, msgToGoosip)

					// node.RPC(neighborID, msgToGoosip, func(msg maelstrom.Message) error {
					// 	worker.ValMutex.Lock()
					// 	fmt.Fprintf(os.Stderr, "Meus vizinhos: [%v] o cara me respondeu [%v] deveria ser [%v]: \n", worker.Neighbors, neighborID, neighborID)
					// 	worker.AddValuesKnowByNeighbor(neighborID, valuesToSend)
					// 	worker.ValMutex.Unlock()
					// 	return nil
					// })
					// tentar utilizar RPC
					// quando chegar a confirmação de envio, salva no set
				}

				// for _, value := range values.ToSlice() {

				// 	worker.TopoMutex.Lock()
				// 	neighbors := worker.Neighbors
				// 	worker.TopoMutex.Unlock()

				// 	for _, neighboorID := range neighbors {
				// 		worker.ValMutex.Lock()
				// isValueKnowByNeighbor := worker.ValueKnowBy[neighboorID][value]
				// worker.ValMutex.Unlock()

				// if !isValueKnowByNeighbor {
				// 	// node.Send(neighboorID, msgToGoosip)
				// 	node.RPC(neighboorID, msgToGoosip, func(msg maelstrom.Message) error {
				// 		worker.ValMutex.Lock()
				// 		_, ok := worker.ValueKnowBy[neighboorID]
				// 		if !ok {
				// 			worker.ValueKnowBy[neighboorID] = make(map[int]bool)
				// 		}
				// 		worker.ValueKnowBy[neighboorID][value] = true
				// 		worker.ValMutex.Unlock()
				// 		return nil
				// 	})
				// }
			}
			// }
			// }
		}()
		return nil
	})

	node.Handle("broadcast", func(msg maelstrom.Message) error {
		broadcastMSG := BroadcastRequest{}
		json.Unmarshal(msg.Body, &broadcastMSG)

		if broadcastMSG.Values != nil {
			worker.HandleValuesSendByWorker(msg.Src, broadcastMSG.Values)
		} else {
			worker.HandleValueSendByClient(msg.Src, broadcastMSG.Value)
		}

		response := map[string]any{
			"type": "broadcast_ok",
		}

		if broadcastMSG.ID != nil {
			return node.Reply(msg, response)
		}
		return nil
	})

	node.Handle("read", func(msg maelstrom.Message) error {
		// worker.ValMutex.Lock()
		values := worker.values
		// worker.ValMutex.Unlock()

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
		worker.Neighbors = topologyMSG.Topology[node.ID()]
		worker.TopoMutex.Unlock()

		worker.ValMutex.Lock()
		for _, neighborID := range worker.Neighbors {
			worker.ValuesKnowBy[neighborID] = mapset.NewSet[int]()
		}
		worker.ValMutex.Unlock()

		response := map[string]any{
			"type": "topology_ok",
		}

		return node.Reply(msg, response)
	})

	if err := node.Run(); err != nil {
		log.Fatal("Error: ", err)
	}

}
