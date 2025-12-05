# Mars Distributed Storage System

## How to run the project
To run the project, you can use the following command:

```bash
./gradlew run
```
## Test Suite Documentation

This document details the automated test scenarios executed by the `Main.java` class. It demonstrates the system's ability to handle replication, consistency, topology changes, and failures (crashes) in a distributed environment.

**Configuration Used:**
- **N (Replication Factor):** 3
- **R (Read Quorum):** 2
- **W (Write Quorum):** 2


---

### Test 1: Single Write & Read
**Action:** `Client0` writes `Key=25` (Value: HelloWorld) to `Node 10`.
**Logic:**
* **Replication:** Key 25 falls into the responsibility of **Node 30, Node 40, and Node 0** (wrapping around).
* **Outcome:** Node 10 acts as coordinator. The data is replicated to the 3 responsible nodes.

~~~mermaid
graph LR
    classDef active fill:#dff,stroke:#333,stroke-width:2px,color:#000;
    classDef crash fill:#f99,stroke:#333,stroke-width:2px,stroke-dasharray: 5 5,color:#000;

    N0[("<b>Node 0</b><br/>---<br/>K:25 | v1:HelloWorld")]
    N10[("<b>Node 10</b><br/>---<br/>(coord)")]
    N20[("<b>Node 20</b><br/>---<br/>(empty)")]
    N30[("<b>Node 30</b><br/>---<br/>K:25 | v1:HelloWorld")]
    N40[("<b>Node 40</b><br/>---<br/>K:25 | v1:HelloWorld")]
    N0 --> N10 --> N20 --> N30 --> N40 --> N0

    class N0,N10,N20,N30,N40 active;
~~~

---

### Test 2: Node Join
**Action:** `Node 27` joins the network.
**Logic:**
* **Topology Change:** Node 27 inserts itself between Node 20 and Node 30.
* **Data Handoff:** Node 27 takes responsibility for `Key=25`. Node 0 (the furthest replica) is no longer responsible and drops the key to maintain $N=3$.

~~~mermaid
graph LR
    classDef active fill:#dff,stroke:#333,stroke-width:2px,color:#000;
    classDef crash fill:#f99,stroke:#333,stroke-width:2px,stroke-dasharray: 5 5,color:#000;

    N0[("<b>Node 0</b><br/>---<br/>(empty)")]
    N10[("<b>Node 10</b><br/>---<br/>(empty)")]
    N20[("<b>Node 20</b><br/>---<br/>(empty)")]
    N27[("<b>Node 27</b><br/>---<br/>K:25 | v1:HelloWorld")]
    N30[("<b>Node 30</b><br/>---<br/>K:25 | v1:HelloWorld")]
    N40[("<b>Node 40</b><br/>---<br/>K:25 | v1:HelloWorld")]

    N0 --> N10 --> N20 --> N27 --> N30 --> N40 --> N0

    class N0,N10,N20,N27,N30,N40 active;
~~~

---

### Test 3: Node Leave
**Action:** `Node 30` leaves the network.
**Logic:**
* **Handoff:** Before shutting down, Node 30 transfers data to the new responsible node.
* **New Replicas:** The replicas for `Key=25` become **27, 40, and 0**. Node 0 receives the data again.

~~~mermaid
graph LR
    classDef active fill:#dff,stroke:#333,stroke-width:2px,color:#000;
    classDef crash fill:#f99,stroke:#333,stroke-width:2px,stroke-dasharray: 5 5,color:#000;

    N0[("<b>Node 0</b><br/>---<br/>K:25 | v1:HelloWorld")]
    N10[("<b>Node 10</b><br/>---<br/>(empty)")]
    N20[("<b>Node 20</b><br/>---<br/>(empty)")]
    N27[("<b>Node 27</b><br/>---<br/>K:25 | v1:HelloWorld")]
    N40[("<b>Node 40</b><br/>---<br/>K:25 | v1:HelloWorld")]
    N0 --> N10 --> N20 --> N27 --> N40 --> N0

    class N0,N10,N20,N27,N40 active;
~~~

---

### Test 4: Node Crash
**Action:** `Node 40` crashes
**Logic:**
* **Crash:** System continues to serve requests as Quorum is met by Node 27 and Node 0.
~~~mermaid
graph LR
    classDef active fill:#dff,stroke:#333,stroke-width:2px,color:#000;
    classDef crash fill:#f99,stroke:#333,stroke-width:2px,stroke-dasharray: 5 5,color:#000;

    N0[("<b>Node 0</b><br/>---<br/>K:25 | v1:HelloWorld")]
    N10[("<b>Node 10</b><br/>---<br/>(empty)")]
    N20[("<b>Node 20</b><br/>---<br/>(empty)")]
    N27[("<b>Node 27</b><br/>---<br/>K:25 | v1:HelloWorld")]
    N40[("<b>Node 40</b><br/>---<br/>K:25 | v1:HelloWorld<br/>(CRASHED)")]

    N0 --> N10 --> N20 --> N27 --> N40 --> N0

    class N0,N10,N20,N27 active;
    class N40 crash;
~~~

---
### Test 5: Node Recover
**Action:** `Node 40` recovers from crash.
* **Recover:** Node 40 restarts, contacts neighbors, and pulls the latest data.

~~~mermaid
graph LR
    classDef active fill:#dff,stroke:#333,stroke-width:2px,color:#000;
    classDef crash fill:#f99,stroke:#333,stroke-width:2px,stroke-dasharray: 5 5,color:#000;

    N0[("<b>Node 0</b><br/>---<br/>K:25 | v1:HelloWorld")]
    N10[("<b>Node 10</b><br/>---<br/>(empty)")]
    N20[("<b>Node 20</b><br/>---<br/>(empty)")]
    N27[("<b>Node 27</b><br/>---<br/>K:25 | v1:HelloWorld")]
    N40[("<b>Node 40</b><br/>---<br/>K:25 | v1:HelloWorld<br/>(Recovered)")]
    N0 --> N10 --> N20 --> N27 --> N40 --> N0

    class N0,N10,N20,N27,N40 active;
~~~

---

### Test 6: Sequential Consistency
**Action:**
1. Write "First" (v1).
2. Write "Second" (v2).
3. Read Key 65.
**Logic:** The read operation returns "Second" (Version 2), proving sequential consistency across different clients.

~~~mermaid
graph LR
    classDef active fill:#dff,stroke:#333,stroke-width:2px,color:#000;
    classDef crash fill:#f99,stroke:#333,stroke-width:2px,stroke-dasharray: 5 5,color:#000;

    N0[("<b>Node 0</b><br/>---<br/>K:25 | v1:HelloWorld<br/>K:65 | v2:Second")]
    N10[("<b>Node 10</b><br/>---<br/>k:65 | v2:Second")]
    N20[("<b>Node 20</b><br/>---<br/>K:65 | v2:Second")]
    N27[("<b>Node 27</b><br/>---<br/>K:25 | v1:HelloWorld")]
    N40[("<b>Node 40</b><br/>---<br/>K:25 | v1:HelloWorld")]

    N0 --> N10 --> N20 --> N27 --> N40 --> N0

    class N0,N10,N20,N27,N40 active;
~~~

---

### Test 7: Recover Lost Writes
**Action:** Node 10 crashes. A write happens on Key 65 (v3). Node 10 recovers.
**Logic:** Node 10 missed the write while offline. Upon recovery, it synchronizes with neighbors and fetches Version 3.
1. Node 10 crashes.
~~~mermaid
graph LR
    classDef active fill:#dff,stroke:#333,stroke-width:2px,color:#000;
    classDef crash fill:#f99,stroke:#333,stroke-width:2px,stroke-dasharray: 5 5,color:#000;

    N0[("<b>Node 0</b><br/>---<br/>K:25 | v1:HelloWorld<br/>K:65 | v2:Second")]
    N10[("<b>Node 10</b><br/>---<br/>k:65 | v2:Second")]
    N20[("<b>Node 20</b><br/>---<br/>K:65 | v2:Second")]
    N27[("<b>Node 27</b><br/>---<br/>K:25 | v1:HelloWorld")]
    N40[("<b>Node 40</b><br/>---<br/>K:25 | v1:HelloWorld")]
    N0 --> N10 --> N20 --> N27 --> N40 --> N0

    class N0,N20,N27,N40 active;
    class N10 crash;
~~~
2. Write "Third" (v3) while Node 10 is crashed.
~~~mermaid
graph LR
    classDef active fill:#dff,stroke:#333,stroke-width:2px,color:#000;
    classDef crash fill:#f99,stroke:#333,stroke-width:2px,stroke-dasharray: 5 5,color:#000;

    N0[("<b>Node 0</b><br/>---<br/>K:25 | v1:HelloWorld<br/>K:65 | v3:Third")]
    N10[("<b>Node 10</b><br/>---<br/>k:65 | v2:Second")]
    N20[("<b>Node 20</b><br/>---<br/>K:65 | v3:Third")]
    N27[("<b>Node 27</b><br/>---<br/>K:25 | v1:HelloWorld")]
    N40[("<b>Node 40</b><br/>---<br/>K:25 | v1:HelloWorld")]

    N0 --> N10 --> N20 --> N27 --> N40 --> N0

    class N0,N20,N27,N40 active;
    class N10 crash;
~~~
3. Node 10 recovers and syncs to get Version 3.
~~~mermaid
graph LR
    classDef active fill:#dff,stroke:#333,stroke-width:2px,color:#000;
    classDef crash fill:#f99,stroke:#333,stroke-width:2px,stroke-dasharray: 5 5,color:#000;

    N0[("<b>Node 0</b><br/>---<br/>K:25 | v1:HelloWorld<br/>K:65 | v3:Third")]
    N10[("<b>Node 10</b><br/>---<br/>k:65 | v3:Third<br/>(Recovered)")]
    N20[("<b>Node 20</b><br/>---<br/>K:65 | v3:Third")]
    N27[("<b>Node 27</b><br/>---<br/>K:25 | v1:HelloWorld")]
    N40[("<b>Node 40</b><br/>---<br/>K:25 | v1:HelloWorld")]

    N0 --> N10 --> N20 --> N27 --> N40 --> N0

    class N0,N10,N20,N27,N40 active;
~~~

---

### Test 8: Reading no-existent Key
***Action:** Client tries to read `Key=999` which was never written.
**Logic:** The system returns a "Key Not Found" error after querying the responsible nodes.

---

### Test 9: Concurrent Writes
**Action:** Two clients concurrently write different values to `Key=18`.
**Logic:** Due to quorum based writes and mutex locks, one write succeeds and the other will fail.
~~~mermaid
graph LR
    classDef active fill:#dff,stroke:#333,stroke-width:2px,color:#000;
    classDef crash fill:#f99,stroke:#333,stroke-width:2px,stroke-dasharray: 5 5,color:#000;

    N0[("<b>Node 0</b><br/>---<br/>K:25 | v1:HelloWorld<br/>K:65 | v3:Third")]
    N10[("<b>Node 10</b><br/>---<br/>k:65 | v3:Third")]
    N20[("<b>Node 20</b><br/>---<br/>K:65 | v3:Third</br/>K:18 | v1:winner")]
    N27[("<b>Node 27</b><br/>---<br/>K:25 | v1:HelloWorld<br/>K:18 | v1:winner")]
    N40[("<b>Node 40</b><br/>---<br/>K:25 | v1:HelloWorld<br/>K:18 | v1:winner")]

    N0 --> N10 --> N20 --> N27 --> N40 --> N0

    class N0,N10,N20,N27,N40 active;
~~~

---

### Test 10: Start a client requests during a network change
**Action:** While `Node 70` is joining, a client tries to read
**Logic:** The testManager rejects the read request until the join operation is complete.
~~~mermaid
graph LR
    classDef active fill:#dff,stroke:#333,stroke-width:2px,color:#000;
    classDef crash fill:#f99,stroke:#333,stroke-width:2px,stroke-dasharray: 5 5,color:#000;

    N0[("<b>Node 0</b><br/>---<br/>K:65 | v3:Third")]
    N10[("<b>Node 10</b><br/>---<br/>k:65 | v3:Third")]
    N20[("<b>Node 20</b><br/>---<br/>K:18 | v1:winner")]
    N27[("<b>Node 27</b><br/>---<br/>K:25 | v1:HelloWorld<br/>K:18 | v1:winner")]
    N40[("<b>Node 40</b><br/>---<br/>K:25 | v1:HelloWorld<br/>K:18 | v1:winner")]
    N70[("<b>Node 70</b><br/>---<br/>K:65 | v3:Third<br/>K:25 | v1:HelloWorld<br/>(Joining)")]

    N0 --> N10 --> N20 --> N27 --> N40 --> N70 --> N0

    class N0,N10,N20,N27,N40,N70 active;
~~~

---

### Test 11: Start a network changes during client requests
**Action:** While a client is performing a read, `Node 25` joins the network.
**Logic:** The testManager blocks the join operation causing a fail message to the joining node.

---

### Test 12: Ask to Crashed Node
**Action:** Client tries to read from the crashed `Node 27` and write to `Node 40`.
**Logic:** The client detects the coordinator is unresponsive and throws a `Timeout` error.
~~~mermaid
graph LR
    classDef active fill:#dff,stroke:#333,stroke-width:2px,color:#000;
    classDef crash fill:#f99,stroke:#333,stroke-width:2px,stroke-dasharray: 5 5,color:#000;

    N0[("<b>Node 0</b><br/>---<br/>K:65 | v3:Third")]
    N10[("<b>Node 10</b><br/>---<br/>k:65 | v3:Third")]
    N20[("<b>Node 20</b><br/>---<br/>K:18 | v1:winner")]
    N27[("<b>Node 27</b><br/>---<br/>K:25 | v1:HelloWorld<br/>K:18 | v1:winner")]
    N40[("<b>Node 40</b><br/>---<br/>K:25 | v1:HelloWorld<br/>K:18 | v1:winner")]
    N70[("<b>Node 70</b><br/>---<br/>K:65 | v3:Third<br/>K:25 | v1:HelloWorld")]

    Client((Client)) -.-> N27
    Client -.-> N40

    N0 --> N10 --> N20 --> N27 --> N40 --> N70 --> N0

    class N0,N10,N20,N70 active;
    class N27,N40 crash;
~~~

---

### Test 13: Join with neighbors crashed
**Action:**   `Node 25` tries to join while its successor `Node 27` is crashed.
**Logic:** The successor of a joining node has all the possible key for the new node. Since the successor is crashed, the join fails.

~~~mermaid
graph LR
    classDef active fill:#dff,stroke:#333,stroke-width:2px,color:#000;
    classDef crash fill:#f99,stroke:#333,stroke-width:2px,stroke-dasharray: 5 5,color:#000;

    N0[("<b>Node 0</b><br/>---<br/>K:65 | v3:Third")]
    N10[("<b>Node 10</b><br/>---<br/>k:65 | v3:Third")]
    N20[("<b>Node 20</b><br/>---<br/>K:18 | v1:winner")]
    N25[("<b>Node 25</b><br/>---<br/>(Joining) FAILED")]
    N27[("<b>Node 27</b><br/>---<br/>K:25 | v1:HelloWorld<br/>K:18 | v1:winner")]
    N40[("<b>Node 40</b><br/>---<br/>K:25 | v1:HelloWorld<br/>K:18 | v1:winner")]
    N70[("<b>Node 70</b><br/>---<br/>K:65 | v3:Third<br/>K:25 | v1:HelloWorld")]

    N0 --> N10 --> N20 --> N25 --> N27 --> N40 --> N70 --> N0

    class N0,N10,N20,N70 active;
    class N27,N40 crash;
~~~

---

### Test 14: Recover with neighbors crashed
**Action:**   `Node 40` tries to recover while its predecessor `Node 27` is crashed.
**Logic:** The predecessor and successor are both needed to recover lost data. Since the predecessor is crashed, the recovery fails.
~~~mermaid
graph LR
    classDef active fill:#dff,stroke:#333,stroke-width:2px,color:#000;
    classDef crash fill:#f99,stroke:#333,stroke-width:2px,stroke-dasharray: 5 5,color:#000;

    N0[("<b>Node 0</b><br/>---<br/>K:65 | v3:Third")]
    N10[("<b>Node 10</b><br/>---<br/>k:65 | v3:Third")]
    N20[("<b>Node 20</b><br/>---<br/>K:18 | v1:winner")]
    N27[("<b>Node 27</b><br/>---<br/>K:25 | v1:HelloWorld<br/>K:18 | v1:winner")]
    N40[("<b>Node 40</b><br/>---<br/>K:25 | v1:HelloWorld<br/>K:18 | v1:winner")]
    N70[("<b>Node 70</b><br/>---<br/>K:65 | v3:Third<br/>K:25 | v1:HelloWorld")]

    N0 --> N10 --> N20 --> N27 --> N40 --> N70 --> N0

    class N0,N10,N20,N70 active;
    class N27,N40 crash;
~~~