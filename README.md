# Chandy–Lamport Snapshot Lab

Three logical nodes exchange demo chips over six directed, reliable FIFO
channels. The Go service records global snapshots without pausing transfers or
reading another node's private balance.

Each node owns one private event loop and balance. A transfer is accepted only
for a positive integer amount; the sender debits its balance and enqueues the
correlated transfer atomically in that loop. The destination credits the
transfer only after delivery. Every transfer carries a unique correlation ID.

Snapshots use the Chandy–Lamport marker protocol:

1. A node first records its current local balance.
2. It then enqueues one marker on every outgoing channel.
3. Later transfers remain after those markers and cannot enter that cut.
4. After its own cut, a node records transfers received on each incoming
   channel until that channel's marker arrives.
5. The coordinator combines reported local and channel states. It cannot read
   live balances, freeze queues, or synthesize missing values.

Only one snapshot may be active at a time. A finished snapshot is retained and
can be queried; later snapshots get separate IDs and records. If a test barrier
holds a channel, querying that snapshot returns HTTP `202` with the real
partial state and pending nodes/channels. A complete snapshot returns `200` (or
`201` when initiated) and verifies:

```text
sum(recorded node balances) + sum(recorded in-flight transfers) = 300
```

Each in-flight amount in the result includes the full transfer records that
trace back to the original correlation ID.

## Run

```bash
docker compose up --build
```

The Compose file deploys only this experimental service on port `8080`; no
message broker or external middleware is started.

For local Go development:

```bash
go test -race ./...
go run .
```

## HTTP API

Node numbers are `1`, `2`, and `3`. Every node starts with `100`, so the
initial global total is `300`.

### Health check

```bash
curl -s http://localhost:8080/health
```

### Submit a transfer

An ID may be provided, or the service will generate one.

```bash
curl -s -X POST http://localhost:8080/transfers \
  -H 'Content-Type: application/json' \
  -d '{"id":"xfer-001","from":1,"to":2,"amount":30}'
```

Status codes:

- `201`: transfer accepted, sender has debited and enqueue is durable in the
  process model;
- `400`: invalid node or non-positive amount;
- `409`: duplicate correlation ID or insufficient sender balance.

### Start a snapshot

```bash
curl -s -X POST http://localhost:8080/snapshots \
  -H 'Content-Type: application/json' \
  -d '{"initiator":1}'
```

`201` means all markers had already completed when the response was produced.
`202` means collection is under way; the response contains partial, real state.
A second start before completion returns `409`.

### Query a snapshot

```bash
curl -s http://localhost:8080/snapshots/1 | jq
```

`200` is complete; `202` is incomplete. Completed results retain their recorded
cut and are not modified by later snapshots.

Example complete shape:

```json
{
  "id": 1,
  "complete": true,
  "initial_total": 300,
  "recorded_total": 300,
  "conservation_ok": true,
  "balances": [
    {"node": 1, "balance": 70},
    {"node": 2, "balance": 100},
    {"node": 3, "balance": 100}
  ],
  "channels": [
    {
      "from": 1,
      "to": 2,
      "amount": 30,
      "transfers": [
        {"id": "xfer-001", "from": 1, "to": 2, "amount": 30}
      ]
    }
  ]
}
```

Empty channels are included with zero amounts and empty transfer lists, so all
six directed channels are explicitly accounted for.

## Deterministic test barriers

Barriers control one directed dispatcher at a time and are intended for tests.
They delay delivery without blocking senders, freezing the system, or changing
the protocol.

```bash
# Hold 1 -> 2 deliveries
curl -s -X PUT http://localhost:8080/test/barriers \
  -H 'Content-Type: application/json' \
  -d '{"from":1,"to":2,"held":true}'

# Resume that FIFO channel
curl -s -X PUT http://localhost:8080/test/barriers \
  -H 'Content-Type: application/json' \
  -d '{"from":1,"to":2,"held":false}'

curl -s http://localhost:8080/test/barriers | jq
```

A controlled scenario for “debited but not received” is:

1. Hold `1 -> 2`.
2. Submit a `1 -> 2` transfer.
3. Start a snapshot at node `1`.
4. Query it and observe HTTP `202`, pending channel state, and no fabricated
   completion.
5. Release the barrier.
6. Query the same ID and observe the transfer as channel state with its
   correlation ID and a total of `300`.

## Automated coverage

`go test -race ./...` covers:

- debited-but-not-received transfers behind a held channel;
- incomplete results rather than forged complete snapshots;
- marker / transfer / marker interleaving and FIFO separation;
- two consecutive snapshots with separate IDs and no record mixing;
- rejection of a second concurrently active snapshot;
- conservation for every completed cut and traceability of in-flight transfers;
- HTTP transfer, snapshot, duplicate-ID, and barrier behavior.


## 重叠采集与取消
设置 `SNAPSHOT_OVERLAP=1` 允许最多两份尚未完成的快照，各自使用原 FIFO 标记规则；默认模式仍只允许一份。
`DELETE /snapshots/{id}` 取消未完成采集，保留最后接受的部分证据及 cancelled 终态。取消后 GET 和重复 DELETE 都返回同一终态，完成结果不被取消改写。
取消释放采集槽位与节点该编号的记录状态；队列里的迟到或重复标记不得重启该采集，不影响另一编号。
未完成不得显示守恒成功，转账继续使用节点自身余额，任一完整快照仍能逐笔证明初始总额。
