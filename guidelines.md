
# ds1-project-description-2025



---

## Course project: Mars distributed storage system

Distributed Systems, 2024-2025

You have been enrolled by the European Space Agency (ESA) as a consultant for one of its most ambitious and technically demanding projects: the deployment of a distributed network of data collection stations on the surface of Mars. These stations are spread strategically across the Martian landscape to gather vital scientific data from various sources, including rovers and orbiting satellites. A fundamental requirement of this system is ensuring data availability and robustness. Given the extreme conditions on Mars, the risk of data loss is significant and must be minimized. To address this, data must be effectively replicated and shared among multiple stations. These stations act as nodes within a decentralized, peer-to-peer network, each equipped with a processing unit and capable of wireless communication via antenna arrays. Unlike a stable, cabled environment on Earth, the Martian context introduces considerable challenges. Harsh weather, hardware degradation, and fluctuating environmental conditions mean that stations can crash, disconnect, reboot, or join the network unpredictably. This dynamic and often unstable behavior creates a highly volatile environment for data storage and communication, making resilience and fault-tolerance critical system features.

Your mission is to develop a proof-of-concept for a peer-to-peer key-value storage system that meets these constraints. To demonstrate the system’s functionality and resilience, you are also required to implement a main program file that simulates key operations. This script must showcase typical scenarios such as uploading and requesting data, handling node failures and recoveries, and executing basic network management commands.

To distribute the load effectively, nodes divide the data they store using a key-based partitioning scheme. Keys, represented as unsigned integers, are arranged in a circular space called a "ring", i.e., the largest key value wraps around to the smallest key value like minutes on analog clocks. Each data item with a key K is assigned to the first N nodes found when moving clockwise from K on the ring. Here, N is a configurable replication factor. For instance, with N = 3 and nodes identified by 10, 20, 30, and 40, a data item with key 15 would be stored on nodes 20, 30, and 40. A data item with key 35 would be stored on nodes 40, 10, and 20. Every node in the system must be aware of all other nodes so it can independently determine which nodes are responsible for storing a particular data item. When nodes are added or removed, the system must automatically adjust and redistribute data to maintain balance and consistency.

The project that you are developing should support management services and create clients supporting data services. The data service consists of two commands: update(key, value) and get(key)->value. Any storage node in the network must be able to manage both requests, regardless of the key, by forwarding data to/from appropriate nodes. The node contacted by a client for a given user request is called the coordinator. The management service consists of a join operation that creates a new node in the storage network and a leave operation to remove a node.

---

### 🔁 Replication

Replication is based on quorums. Therefore, to support consistent reads, it is necessary to associate internally the versions with every data item.

The system-wide parameters W and R specify the write and read quorums, respectively.
It is important to remember that W ≤ N and R ≤ N. Moreover, if an operation cannot be completed within a given timeout T, the coordinator reports an error to the requesting client.

**Read.** When it receives a get command from a client, the coordinator requests the item from the N nodes responsible for it. To provide better availability, as soon as R replies arrive, the request coordinator sends the data item back to the requesting client.
Your approach needs to offer sequential consistency by taking advantage of version numbers. Consistent with the order of operations specified by each process (client), read and write operations must appear as if they were occurring in a certain global order.

**Write.** When a node receives an update request from the client, it first requests the currently stored version of the data item from the N nodes that have its replica. For better availability, the write coordinator reports success to the client as soon as it receives the first W replies required to support sequential consistency. It then sends the update to all N replicas, incrementing the version of the data item based on the W replies. Otherwise, on timeout, the coordinator informs the client of the failed attempt and stops processing the request. For simplicity, we assume that there are no failures within the request processing time.

**Local storage.** Every node should maintain a persistent storage containing key, version, and value for every data item the node is responsible for. You can simulate persistent storage in your implementation using a suitable data structure whose data does not get deleted upon crash failures.

---

### 📦🔄 Item repartitioning

When a node joins or leaves the network, repartitioning is required. Note that both operations are performed only upon external request. A crashed node should not be considered as leaving but only temporarily unavailable; therefore, there is no need to implement a crash detection mechanism or repartition the data when a node cannot be accessed.

**Joining.**
The actor reference of one of the nodes that is currently operating, which serves as its bootstrapping peer, and the key of the newly created node are both included in the join request. First, the joining node contacts the bootstrapping peer to retrieve the current set of nodes constituting the network. Having received this information, the joining node should request data items it is responsible for from its clockwise neighbor, which holds all items it needs. It should then perform read operations to ensure that its items are up to date. After receiving the updated data items, the node can finally announce its presence to every node in the system and start serving requests coming from clients. The others should remove the data items assigned to a new node that they are no longer in charge of as soon as they become aware of it.

**Leaving.**
It is possible to request a node to leave the network. In order to accomplish this, the leaving node notifies the others of its impending leaving and transfers its data items to the nodes that will be in charge of them once it leaves.

**Recovery.**
When a crashed node is restarted, it requests a node listed in the recovery request for the current set of nodes rather than executing the join operation. It should retrieve the items that are now under its control (because nodes left) and discard the ones that are no longer under its control (because other nodes joined while it was down).

---

### 📋🤔 Other requirements and assumptions

* The project should be implemented in Akka, with nodes being Akka actors, written in Java programming language. In exceptional circumstances, the project may be implemented using alternative frameworks or languages; however, this must first be discussed with the instructors.
* The clients execute commands passed through messages, print the reply from the coordinator, and only then accept further commands. A client may perform multiple read and write operations.
* A node must be able to serve requests from different clients, and the network must support concurrent requests (possibly affecting the same item key).
* Nodes join and leave, crash and recover one at a time, and only when there are no ongoing operations. Operations might resume while one or more nodes are still in a crashed state.
* For the sake of this project, the network is assumed to be FIFO and reliable.
* Use (unsigned) integers for keys, and strings (without spaces) as data items. The keys are set by the user to simplify testing, though in real systems, random-like hash values are used.
* The replication parameters N, R, W, and T should be configurable at compile time.
* To emulate network propagation delays, you are requested to insert small random intervals between the unicast transmissions.
* N must always be less than or equal to the number of nodes.
* There must always be at least one node in the distributed storage system that is not in crash state.
* It is important to state all the additional assumptions in the report.

---

### 📑🖋 Project report

You are asked to provide a short document (typically 3–4 pages) in English explaining the main architectural choices. Your report should include a discussion on how your implementation satisfies consistency requirements (sequential consistency).
In the project presentation slides, you can find a list of relevant questions your document should answer.

---

### 📊✅ Grading criteria

You are responsible for showing that your project works. The project will be evaluated for its technical content, i.e., algorithm correctness. Do not spend time implementing features other than the ones requested; focus on doing the core functionality, and doing it well.

* Correct implementation of all requested functionality → **6 points**
* Reduced implementation → **lower score**

You must work **in pairs**, but the evaluation is **individual**.

---

### 🎤 Presenting the project

* You MUST contact via email the instructor and teaching assistants **a couple of weeks before the presentation**.
* You may present at any time, even outside exam sessions (the mark will be frozen).
* Code must follow proper formatting guidelines.
* Both code and report must be submitted via email **at least one day before**.
  Submit a **single tarball** containing:

  * a folder named `<Surname1><Surname2>`
  * the full source code
  * the report in a single PDF/txt
* Plagiarism is not tolerated.

---

# ds1-project-presentation-2025



---

*(Diapositive trascritte fedelmente, titolo per titolo.)*

---

## DISTRIBUTED SYSTEMS – PROJECT 2025

Mars distributed storage system

[stefano.genetti@unitn.it](mailto:stefano.genetti@unitn.it)
[erik.nielsen@unitn.it](mailto:erik.nielsen@unitn.it)

---

## MISSION

• You have been enrolled by the European Space Agency (ESA).
• TASK: deployment of a distributed peer-to-peer network of data collection stations on the surface of Mars.
• The Martian extreme environment introduces considerable challenges! Stations can crash, disconnect, reboot or join the network unpredictably.

---

## PROJECT OVERVIEW

• You will implement a key-value store (database)
• Every data item is identified by a unique key
• Main operations:
– update(key, value)
– get(key) → value
• Replication: several nodes store the same item. For reliability and accessibility.
• Data partitioning: not all nodes store all items. For load balancing.

---

## NETWORK LOGICAL TOPOLOGY (4)

*(Schema con ring e nodi, testo seguente)*

• Both the storage nodes and data items have associated keys that form a circular space (ring).
• We will use integers for keys, strings for values.

---

## NETWORK LOGICAL TOPOLOGY (5)

• A data item with key K is stored by the N nearest clockwise nodes.
• For example, if N=2, a data item with key 25 will be stored at nodes 30 and 40.

---

## NETWORK LOGICAL TOPOLOGY (6)

• A data item with key K is stored by the N nearest clockwise nodes.
• For example, if N=3, a data item with key 25 will be stored at nodes 30, 40, 10.
• If we need to go beyond the maximum node key, we wrap around.

---

## TYPES OF ACTORS AND SUPPORTED OPERATIONS

• You must support the creation of clients, new storage nodes (join), and storage node removal (leave).
• A different type of actor, the client should be able to contact any node in the ring to read or write a value.

---

## REQUEST COORDINATOR

• Clients may contact any node to read/write data with any key: the chosen node is the coordinator.
• Nodes know all their peers and can compute who is responsible for the key and pass the request.

---

## REPLICATION

• The same data item is stored by N nodes
• When reading an item, the coordinator requests data from all N nodes, but answers to the client as soon as a read quorum R ≤ N of them reply
• When writing, the coordinator tries to contact all N nodes, but completes the write as soon as a write quorum W ≤ N of them reply
• Implement read/write operations that provide sequential consistency

---

## READ OPERATION

*(Schema + testo)*
• Let N=3, R=2
• Use version numbers to send back the most recent version!

---

## WRITE OPERATION

*(Schema + testo)*
• Let N=3, W=2
• Update the version!

---

## NO QUORUM?

• If not enough nodes reply within T seconds → timeout → error returned to client.

---

## SEQUENTIAL CONSISTENCY: HINTS

• Avoid write/write conflicts or you may have different values for the same data version!
• Once a client has observed some value, it can never observe a value prior to it.
• Reject reads that may break sequential consistency.

---

## ITEM REPARTITIONING

• When a node joins or leaves, the system should move data items accordingly.
• Crashes do not count as leaving.

---

## JOINING OPERATION (15–17)

1. Joining node asks any existing node for the list of nodes.
2. It identifies its clockwise neighbor and asks for relevant items.
3. It performs reads to ensure it gets the most updated versions.
4. It announces itself; other nodes drop items they no longer own.

---

## JOINING OPERATION (EXAMPLE N=2)

*(Schema + testo)*

---

## JOINING OPERATION (WHY READING?)

*(Esempio su versioni vecchie vs nuove)*

---

## LEAVING OPERATION

1. The main requests a node to leave
2. The node notifies all others
3. It transfers items only to the nodes that become responsible

---

## RECOVERY OPERATION

• Recovering node asks another node for current list
• Forgets items no longer under its control
• Requests items newly under its control

---

## MAIN ASSUMPTIONS

• Nodes may serve multiple clients concurrently
• Nodes join/leave/crash/recover one at a time when idle
• Replication parameters are static (compile time)
• Requests may fail if they compromise sequential consistency

---

## PROJECT REPORT

• Structure of the project
• Main design choices
• How do you keep track of ongoing operations?
• What are the main messages used?
• How do you simulate crash and recovery?
• Does it provide sequential consistency? How?
• Do join/leave strategies send data only to responsible nodes?
• Additional assumptions?

---

## GRADING

• Full implementation: 6 points
• Reduced implementation (no replication, no versions, no recovery): 3 points
• Work in pairs, grading is individual


