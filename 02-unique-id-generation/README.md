# Unique-ids

Clients ask servers to generate an ID, and the server should respond with an ID. The test verifies that those IDs are globally unique.

### Example

A node receives the request body:

```json
{"type" : "generate",
"msg_id": 2}
```

```json
{"type" : "generate_ok",
"in_reply_to": 2,
"id": 123}
```

### Running

```sh
maelstrom test -w unique-ids --bin ~/go/bin/maelstrom-unique-ids --time-limit 30 --rate 1000 --node-count 3 --availability total --nemesis partition
```

Will run a 3-node cluster for 30 seconds and reqest new IDs at the rate of 1000 requests per second. It checks for total availability and will induce network partitions during the test. It will also verify that all IDs are unique.
