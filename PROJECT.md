# Final Project: Distributed KV-Cache with Sharding & Replication

**Author:** Indrajeet Yadav  
**Course:** HLD-101  
**Date:** February 2026

---

## Part A: Your Implementation

### Architecture

The distributed KV-Cache extends the single-node implementation from Assignment 2 into a **3-node cluster** with consistent hashing for sharding and synchronous replication for fault tolerance.

```
                    Client Request
                          │
                          ▼
                   ┌──────────────┐
                   │   Any Node   │ (Entry Point)
                   └──────┬───────┘
                          │
              ┌───────────┼───────────┐
              │           │           │
              ▼           ▼           ▼
       ┌──────────┐ ┌──────────┐ ┌──────────┐
       │  Node 1  │ │  Node 2  │ │  Node 3  │
       │ Port 7171│ │ Port 7172│ │ Port 7173│
       ├──────────┤ ├──────────┤ ├──────────┤
       │Primary: 0│ │Primary: 1│ │Primary: 2│
       │Replica: 1│ │Replica: 2│ │Replica: 0│
       └──────────┘ └──────────┘ └──────────┘
              │           │           │
              └───────────┴───────────┘
                    Inter-node
                  Communication
```

**Shard Distribution Table:**

| Shard | Primary Node | Replica Node |
| ----- | ------------ | ------------ |
| 0     | Node 1       | Node 3       |
| 1     | Node 2       | Node 1       |
| 2     | Node 3       | Node 2       |

Each node is primary for one shard and replica for another, ensuring balanced load distribution.

### Sharding

Keys are routed to shards using **consistent hashing** with the formula:

```python
shard_id = hash(key) % NUM_SHARDS  # NUM_SHARDS = 3
```

The hash function uses the sum of ASCII character values for deterministic, consistent routing:

```python
def get_shard_for_key(self, key: str) -> int:
    hash_value = sum(ord(c) for c in key)
    return hash_value % self.NUM_SHARDS
```

**Request Flow:**

1. Client connects to any node
2. Node calculates the shard for the key
3. If the node is the primary → process locally
4. If not → forward request to the correct primary node and relay the response

### Replication

All writes (PUT/DELETE) use **synchronous replication** to ensure durability:

```
Client                Primary Node              Replica Node
  │                        │                         │
  │───PUT key value───────▶│                         │
  │                        │───REPLICA PUT key val──▶│
  │                        │                         │
  │                        │◀──────OK stored─────────│
  │◀───────OK stored───────│                         │
```

**Key Implementation Details:**

- Writes succeed only after **both** primary and replica acknowledge
- `REPLICA` prefix marks internal replication commands to prevent infinite loops
- Replica nodes store data locally without further replication
- Reads are served by the primary node only (for simplicity and consistency)

### Challenges

1. **Avoiding Replication Loops:** The biggest challenge was preventing infinite replication. When the primary replicates to the replica, the replica shouldn't replicate again. I solved this by using a `REPLICA` prefix for internal commands that tells the receiving node to store locally without further replication.

2. **Connection Management:** Managing TCP connections between nodes for forwarding and replication required careful timeout handling. I implemented a timeout of 5 seconds to prevent blocking on unreachable nodes.

3. **Response Parsing:** When forwarding requests, the response from the remote node arrives as a raw string. I had to parse this string back into the appropriate response format to return to the client.

4. **Docker Networking:** Getting the three containers to communicate required setting up a custom Docker network and using container hostnames instead of localhost.

---

## Part B: Beyond the Basics (Research Write-up)

### 1. Failure Detection

**How would you detect that a node has crashed or become unreachable?**

Failure detection in distributed systems typically uses one of these approaches:

**Heartbeat Mechanism:** Nodes periodically send "I'm alive" messages to each other. If a node misses several consecutive heartbeats (e.g., 3 heartbeats at 1-second intervals), it's considered failed. This is simple but can have false positives during network partitions.

**Gossip Protocol:** Nodes share health information about other nodes they've contacted. If multiple nodes report that Node X is unreachable, it's likely actually down. This is more robust than simple heartbeats but adds complexity.

**The difference between a crashed node and a slow node** is fundamentally the "FLP impossibility" problem—in an asynchronous network, you cannot distinguish between a crashed node and one that's just very slow. This is why most systems use timeouts as a heuristic: if a node doesn't respond within X milliseconds, treat it as failed.

**Redis Cluster** handles this with a combination of heartbeats and gossip. Each node pings random nodes every second and shares PING/PONG metadata. After `cluster-node-timeout` milliseconds without a response, the node is marked as `PFAIL` (possibly failed). When the majority of master nodes agree a node is `PFAIL`, it becomes `FAIL` (definitely failed), triggering failover.

### 2. Handling Network Partitions

**What happens if Node 1 can talk to Node 2, but neither can reach Node 3?**

Network partitions create the most challenging scenarios in distributed systems. In our cluster:

```
  [Node 1] ◄──────► [Node 2]
      │                 │
      X                 X   (partition)
      │                 │
           [Node 3]
```

**Potential problems:**

- Node 3 thinks it's still primary for Shard 2, but clients can't reach it
- Node 1 and Node 2 might elect a new primary for Shard 2
- When partition heals, two nodes think they're primary (split-brain)

**Split-brain** occurs when network partition causes multiple nodes to believe they're the leader. This can cause data inconsistency—both nodes accept writes, creating divergent state.

**Prevention strategies:**

1. **Quorum-based decisions:** Require majority (2 of 3 nodes) to agree before making changes. The minority partition becomes read-only or unavailable.
2. **Leader leases:** Primary nodes hold time-limited "leases." If they can't renew (due to partition), they stop accepting writes.
3. **Fencing tokens:** Each leader gets a monotonically increasing token. Storage systems reject writes from stale tokens.

**The CAP theorem** states you can only guarantee two of: Consistency, Availability, Partition-tolerance. During partitions:

- **CP systems** (like our synchronous replication) sacrifice availability—writes fail if replica is unreachable
- **AP systems** (like DynamoDB in some modes) sacrifice consistency—both partitions accept writes, merge later

Redis Cluster is CP—it sacrifices availability during network partitions rather than risk inconsistent data.

### 3. Consistency vs Availability Trade-offs

**What are the trade-offs between synchronous and asynchronous replication?**

**Synchronous Replication (our implementation):**

```
Client ──PUT──▶ Primary ──replicate──▶ Replica
                   │                      │
                   │◀────────OK───────────│
Client ◀──OK───────│
```

| Pros                                  | Cons                                         |
| ------------------------------------- | -------------------------------------------- |
| Strong consistency (read your writes) | Higher latency (wait for replica)            |
| No data loss on primary failure       | Reduced availability (fails if replica down) |
| Simpler mental model                  | Lower throughput                             |

**Asynchronous Replication:**

```
Client ──PUT──▶ Primary ──OK──▶ Client
                   │
                   └──replicate──▶ Replica (later)
```

| Pros                | Cons                           |
| ------------------- | ------------------------------ |
| Lower latency       | Potential data loss on failure |
| Higher availability | Eventual consistency only      |
| Better throughput   | Stale reads from replica       |

**When might you lose data with async replication?**

Consider this timeline:

1. Client writes `key=A` to primary
2. Primary acknowledges immediately
3. Primary crashes **before** replicating to replica
4. Replica is promoted to primary
5. Write is lost forever

This "replication lag" window is the danger zone. Redis Cluster uses async replication by default, acknowledging that some recent writes may be lost during failover. The `WAIT` command provides optional synchronous semantics when needed.

**Redis Cluster's consistency guarantees:**

- No strong consistency guarantee
- Best-effort async replication
- Writes can be lost during failover
- The `WAIT numreplicas timeout` command can block until N replicas acknowledge, providing synchronous behavior when needed

For our project, we chose synchronous replication because durability was more important than latency, and the simplifying assumption that all nodes are always up made it practical.

---

## How to Run

### Start the Cluster

```bash
# Build and start all 3 nodes
docker-compose up --build

# Or run in background
docker-compose up -d --build
```

### Test the Cluster

```bash
# Connect to Node 1 (port 7171)
echo "PUT user:alice hello" | nc localhost 7171
# Output: OK stored

# Connect to Node 2 (port 7172) - request will be forwarded
echo "GET user:alice" | nc localhost 7172
# Output: OK hello

# Connect to Node 3 (port 7173) - request will be forwarded
echo "GET user:alice" | nc localhost 7173
# Output: OK hello
```

### Verify Sharding

```bash
# These keys hash to different shards
echo "PUT key_a value_a" | nc localhost 7171  # Shard varies by hash
echo "PUT key_b value_b" | nc localhost 7171
echo "PUT key_c value_c" | nc localhost 7171

# All can be retrieved from any node
echo "GET key_a" | nc localhost 7172
echo "GET key_b" | nc localhost 7173
echo "GET key_c" | nc localhost 7171
```

### Stop the Cluster

```bash
docker-compose down
```

---

## File Structure

```
kv_store-HLD/
├── src/
│   ├── cluster/                    # NEW: Distributed cluster module
│   │   ├── __init__.py
│   │   ├── config.py               # Cluster configuration & shard mapping
│   │   ├── node_client.py          # Inter-node communication
│   │   └── distributed_server.py   # Main distributed server
│   ├── cache/                      # From Assignment 2
│   ├── protocol/                   # From Assignment 2
│   ├── network/                    # From Assignment 2
│   └── server.py                   # Original single-node server
├── tests/
│   └── test_distributed.py         # NEW: Distributed cluster tests
├── docker-compose.yml              # NEW: 3-node cluster setup
├── Dockerfile                      # Updated for cluster
├── PROJECT.md                      # This file
└── README.md
```

---

## References

- [Redis Cluster Specification](https://redis.io/docs/reference/cluster-spec/)
- [Designing Data-Intensive Applications](https://dataintensive.net/) by Martin Kleppmann
- [CAP Theorem](https://en.wikipedia.org/wiki/CAP_theorem)
- [Raft Consensus Algorithm](https://raft.github.io/)
