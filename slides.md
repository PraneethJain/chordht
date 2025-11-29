## Chord with Replication

> Project 16

> Moida Praneeth Jain (2022101093) Yajat Rangnekar (2023114008)

---

### Architecture

1. **Chord Node**: Core logic and state
2. **Chord Client**: CLI for get/put
3. **Chord Proto**: Shared gRPC definitions
4. **Monitor**: Live visualization

---

### Core logic

- **Consistent Hashing**: SHA-1 (160-bit space)
- **Finger tables**: For $O(log(n))$ lookups

-v-

### Dynamic Membership (Churn)

- **Join**: Connect via existing node $\rightarrow$ Find successor
- **Leave**: Graceful exit & key transfer
- **Stabilize**: Periodic background task

-v-

### Fault Tolerance

- **Successor List**: $r$ nearest successors
- Periodic pings to detect failures
- Ring reconnects automatically

-v-

### Replication Strategy

- Primary + $k$ successors
- Active background maintenance
- Prioritize availability

---
### Benchmarks and Analysis

-v-

#### Scalability

<img src="chord_node/benchmark_results/scalability.png" style="width:800px;"/>

-v-

#### Load Balancing

<img src="chord_node/benchmark_results/load_balancing.png" style="width:800px;"/>

-v-

#### Concurrent Throughput

<img src="chord_node/benchmark_results/concurrent_throughput.png" style="width:800px;"/>

-v-

#### Replication Delay

<img src="chord_node/benchmark_results/replication_delay.png" style="width:800px;"/>

---

### Learnings and extensions

- Experienced tradeoff between availability and consistency
- Handling failures and race conditions is 80% of the complexity
- _Virtual nodes_ for better load balancing
