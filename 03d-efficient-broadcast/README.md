[link-doc](https://github.com/jepsen-io/maelstrom/blob/main/doc/03-broadcast/02-performance.md)

# Running

I only got able to solve it by using a custom topology: `--topology tree4`

Results are on average:

- Median latency: 388~410ms
- Maximum latency: 590~600ms
- Messages per-op: 29

```sh
maelstrom test -w broadcast --bin ~/go/bin/maelstrom-broadcast --node-count 25 --time-limit 20 --rate 100 --latency 100 --topology tree4
```
