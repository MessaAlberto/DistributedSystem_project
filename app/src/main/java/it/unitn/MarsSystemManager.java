package it.unitn;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import scala.concurrent.duration.Duration;

/**
 * Central manager for the Mars distributed storage system.
 * Handles node lifecycle (join/leave/crash/recovery), client creation,
 * and orchestrates test scenarios according to project specifications.
 */
public class MarsSystemManager extends AbstractActor {
  
  private final int N, R, W; // Replication parameters
  private final Random random = new Random();
  
  // System state
  private final List<ActorRef> activeNodes = new ArrayList<>();
  private final List<ActorRef> crashedNodes = new ArrayList<>();
  private final Map<ActorRef, Integer> nodeIdMap = new HashMap<>();
  private final List<ActorRef> clients = new ArrayList<>();
  
  // Node ID counter for creating new nodes
  private int nextNodeId = 0;
  
  /** Constructs the manager with replication parameters. */
  public MarsSystemManager(int N, int R, int W) {
    this.N = N;
    this.R = R;
    this.W = W;
  }

  /** Akka factory method to create Props for this actor. */
  public static Props props(int N, int R, int W) {
    return Props.create(MarsSystemManager.class, () -> new MarsSystemManager(N, R, W));
  }

  // =====================
  // Message Classes
  // =====================

  /** Initialize the system with a given number of nodes. */
  public static class InitializeSystem implements Serializable {
    public final int initialNodeCount;
    public InitializeSystem(int initialNodeCount) { this.initialNodeCount = initialNodeCount; }
  }

  /** Create a new client actor. */
  public static class CreateClient implements Serializable {}

  /** Add a new node to the system (JOIN). */
  public static class AddNode implements Serializable {
    public final int nodeId; // Optional: specify nodeId, or -1 for auto-assign
    public AddNode(int nodeId) { this.nodeId = nodeId; }
    public AddNode() { this.nodeId = -1; } // Auto-assign
  }

  /** Remove a node from the system (LEAVE). */
  public static class RemoveNode implements Serializable {
    public final int nodeId; // -1 for random
    public RemoveNode(int nodeId) { this.nodeId = nodeId; }
    public RemoveNode() { this.nodeId = -1; } // Random
  }

  /** Crash a node temporarily. */
  public static class CrashNode implements Serializable {
    public final int nodeId; // -1 for random
    public CrashNode(int nodeId) { this.nodeId = nodeId; }
    public CrashNode() { this.nodeId = -1; } // Random
  }

  /** Recover a crashed node. */
  public static class RecoverNode implements Serializable {
    public final int nodeId; // -1 for random crashed node
    public RecoverNode(int nodeId) { this.nodeId = nodeId; }
    public RecoverNode() { this.nodeId = -1; } // Random crashed
  }

  /** Send a Put request via a random client. */
  public static class TestPut implements Serializable {
    public final int key;
    public final String value;
    public TestPut(int key, String value) { this.key = key; this.value = value; }
  }

  /** Send a Get request via a random client. */
  public static class TestGet implements Serializable {
    public final int key;
    public TestGet(int key) { this.key = key; }
  }

  /** Get current system status. */
  public static class GetSystemStatus implements Serializable {}

  /** Response with current system status. */
  public static class SystemStatus implements Serializable {
    public final int activeNodeCount;
    public final int crashedNodeCount;
    public final int clientCount;
    public final String nodeIds;
    
    public SystemStatus(int activeNodeCount, int crashedNodeCount, int clientCount, String nodeIds) {
      this.activeNodeCount = activeNodeCount;
      this.crashedNodeCount = crashedNodeCount;
      this.clientCount = clientCount;
      this.nodeIds = nodeIds;
    }
  }

  // =====================
  // Actor Lifecycle
  // =====================

  /** Message routing for the manager. */
  @Override
  public Receive createReceive() {
    return receiveBuilder()
        .match(InitializeSystem.class, this::onInitializeSystem)
        .match(CreateClient.class, this::onCreateClient)
        .match(AddNode.class, this::onAddNode)
        .match(RemoveNode.class, this::onRemoveNode)
        .match(CrashNode.class, this::onCrashNode)
        .match(RecoverNode.class, this::onRecoverNode)
        .match(TestPut.class, this::onTestPut)
        .match(TestGet.class, this::onTestGet)
        .match(GetSystemStatus.class, this::onGetSystemStatus)
        .match(UpdateClientNodeLists.class, this::onUpdateClientNodeLists)
        .build();
  }

  // =====================
  // Message Handlers
  // =====================

  /** Initializes the system with the specified number of initial nodes. */
  private void onInitializeSystem(InitializeSystem msg) {
    System.out.println("[MarsSystemManager] Initializing system with " + msg.initialNodeCount + " nodes");
    System.out.println("[MarsSystemManager] Configuration: N=" + N + ", R=" + R + ", W=" + W);
    
    // Clear existing state
    activeNodes.clear();
    crashedNodes.clear();
    nodeIdMap.clear();
    clients.clear();
    
    // Create initial nodes
    for (int i = 0; i < msg.initialNodeCount; i++) {
      int nodeId = i * 10; // Space out node IDs: 0, 10, 20, 30, ...
      String nodeName = "node" + i;
      ActorRef node = context().actorOf(Node.props(nodeId, N, R, W), nodeName);
      activeNodes.add(node);
      nodeIdMap.put(node, nodeId);
      nextNodeId = Math.max(nextNodeId, nodeId + 10);
    }
    
    // Initialize peer lists for all nodes
    updateAllNodePeers();
    
    System.out.println("[MarsSystemManager] System initialized with " + activeNodes.size() + " active nodes");
    logSystemStatus();
  }

  /** Creates a new client actor. */
  private void onCreateClient(CreateClient msg) {
    if (activeNodes.isEmpty()) {
      System.out.println("[MarsSystemManager] Cannot create client: no active nodes");
      return;
    }
    
    String clientName = "client" + clients.size();
    ActorRef client = context().actorOf(Props.create(Client.class, new ArrayList<>(activeNodes)), clientName);
    clients.add(client);
    
    System.out.println("[MarsSystemManager] Created " + clientName + " with access to " + activeNodes.size() + " nodes");
  }

  /** Adds a new node to the system via JOIN protocol. */
  private void onAddNode(AddNode msg) {
    if (activeNodes.isEmpty()) {
      System.out.println("[MarsSystemManager] Cannot add node: no bootstrap nodes available");
      return;
    }
    int nodeId = (msg.nodeId == -1) ? nextNodeId : msg.nodeId;
    if (msg.nodeId == -1) nextNodeId += 10;
    if (nodeIdMap.containsValue(nodeId)) {
      System.out.println("[MarsSystemManager] NodeId " + nodeId + " already exists, skipping add");
      return;
    }
    String nodeName = "node_join_" + nodeId;
    ActorRef newNode = context().actorOf(Node.props(nodeId, N, R, W), nodeName);
    ActorRef bootstrap = activeNodes.get(random.nextInt(activeNodes.size()));
    System.out.println("[MarsSystemManager] Adding node " + nodeId + " via bootstrap " + Node.shortName(bootstrap));
    newNode.tell(new Node.JoinNetwork(bootstrap), self());
    activeNodes.add(newNode);
    nodeIdMap.put(newNode, nodeId);

    // Aggiorna client e vista peer dopo il join
    context().system().scheduler().scheduleOnce(
        Duration.create(2, TimeUnit.SECONDS),
        self(),
        new UpdateClientNodeLists(),
        context().dispatcher(),
        self());
  }

  /** Removes a node from the system via LEAVE protocol. */
  private void onRemoveNode(RemoveNode msg) {
    if (activeNodes.isEmpty()) {
      System.out.println("[MarsSystemManager] No active nodes to remove");
      return;
    }
    if (activeNodes.size() <= 1) {
      System.out.println("[MarsSystemManager] Refusing to remove the last active node");
      return;
    }
    ActorRef nodeToRemove;
    if (msg.nodeId == -1) {
      nodeToRemove = activeNodes.get(random.nextInt(activeNodes.size()));
    } else {
      nodeToRemove = findNodeByNodeId(msg.nodeId);
      if (nodeToRemove == null) {
        System.out.println("[MarsSystemManager] Node " + msg.nodeId + " not found in active nodes");
        return;
      }
    }
    int nodeId = nodeIdMap.get(nodeToRemove);
    System.out.println("[MarsSystemManager] Requesting LEAVE for node " + nodeId);
    nodeToRemove.tell(new Node.LeaveNetwork(), self());
    activeNodes.remove(nodeToRemove);
    nodeIdMap.remove(nodeToRemove);

    context().system().scheduler().scheduleOnce(
        Duration.create(2, TimeUnit.SECONDS),
        self(),
        new UpdateClientNodeLists(),
        context().dispatcher(),
        self());
  }

  /** Crashes a node temporarily (simulates failure). */
  private void onCrashNode(CrashNode msg) {
    if (activeNodes.isEmpty()) {
      System.out.println("[MarsSystemManager] No active nodes to crash");
      return;
    }
    if (activeNodes.size() <= 1) {
      System.out.println("[MarsSystemManager] Refusing to crash the last active node");
      return;
    }
    ActorRef nodeToCrash;
    if (msg.nodeId == -1) {
      nodeToCrash = activeNodes.get(random.nextInt(activeNodes.size()));
    } else {
      nodeToCrash = findNodeByNodeId(msg.nodeId);
      if (nodeToCrash == null) {
        System.out.println("[MarsSystemManager] Node " + msg.nodeId + " not found in active nodes");
        return;
      }
    }
    int nodeId = nodeIdMap.get(nodeToCrash);
    System.out.println("[MarsSystemManager] Crashing node " + nodeId);
    nodeToCrash.tell(new Node.Crash(), self());
    activeNodes.remove(nodeToCrash);
    crashedNodes.add(nodeToCrash);

    // Aggiorna immediatamente i client (rimuove il nodo crashed dalla lista)
    updateClientNodeLists();
    // Aggiorna vista peer (opzionale, utile per test)
    updateAllNodePeers();
  }

  /** Recovers a crashed node. */
  private void onRecoverNode(RecoverNode msg) {
    if (crashedNodes.isEmpty()) {
      System.out.println("[MarsSystemManager] No crashed nodes to recover");
      return;
    }
    if (activeNodes.isEmpty()) {
      System.out.println("[MarsSystemManager] Cannot recover: no active nodes for bootstrap");
      return;
    }
    ActorRef nodeToRecover;
    if (msg.nodeId == -1) {
      nodeToRecover = crashedNodes.get(random.nextInt(crashedNodes.size()));
    } else {
      nodeToRecover = null;
      for (ActorRef node : crashedNodes) {
        if (nodeIdMap.get(node) == msg.nodeId) {
          nodeToRecover = node; break;
        }
      }
      if (nodeToRecover == null) {
        System.out.println("[MarsSystemManager] Crashed node " + msg.nodeId + " not found");
        return;
      }
    }
    int nodeId = nodeIdMap.get(nodeToRecover);
    ActorRef bootstrap = activeNodes.get(random.nextInt(activeNodes.size()));
    System.out.println("[MarsSystemManager] Recovering node " + nodeId + " via bootstrap " + Node.shortName(bootstrap));
    nodeToRecover.tell(new Node.RecoverNetwork(bootstrap), self());
    crashedNodes.remove(nodeToRecover);
    activeNodes.add(nodeToRecover);

    context().system().scheduler().scheduleOnce(
        Duration.create(2, TimeUnit.SECONDS),
        self(),
        new UpdateClientNodeLists(),
        context().dispatcher(),
        self());
  }

  /** Sends a Put request via a random client. */
  private void onTestPut(TestPut msg) {
    if (clients.isEmpty()) {
      System.out.println("[MarsSystemManager] No clients available for Put test");
      return;
    }
    
    ActorRef client = clients.get(random.nextInt(clients.size()));
    System.out.println("[MarsSystemManager] Sending Put(key=" + msg.key + ", value=\"" + msg.value + "\") via client");
    client.tell(new Client.Update(msg.key, msg.value), self());
  }

  /** Sends a Get request via a random client. */
  private void onTestGet(TestGet msg) {
    if (clients.isEmpty()) {
      System.out.println("[MarsSystemManager] No clients available for Get test");
      return;
    }
    
    ActorRef client = clients.get(random.nextInt(clients.size()));
    System.out.println("[MarsSystemManager] Sending Get(key=" + msg.key + ") via client");
    client.tell(new Client.Get(msg.key), self());
  }

  /** Returns current system status. */
  private void onGetSystemStatus(GetSystemStatus msg) {
    String nodeIds = activeNodes.stream()
        .map(node -> String.valueOf(nodeIdMap.get(node)))
        .reduce((a, b) -> a + "," + b)
        .orElse("none");

    SystemStatus status = new SystemStatus(
        activeNodes.size(),
        crashedNodes.size(),
        clients.size(),
        nodeIds
    );

    ActorRef replyTo = getSender();
    // Evita dead letters: se il sender è deadLetters, non rispondere
    if (!replyTo.equals(getContext().getSystem().deadLetters())) {
      replyTo.tell(status, self());
    }
    logSystemStatus();
  }

  // =====================
  // Helper Methods
  // =====================

  /** Internal message to update client node lists. */
  private static class UpdateClientNodeLists implements Serializable {}

  /** Updates all client actors with the current active node list. */
  private void onUpdateClientNodeLists(UpdateClientNodeLists msg) {
    updateClientNodeLists();
    updateAllNodePeers();
  }

  /** Updates all client actors with the current active node list. */
  private void updateClientNodeLists() {
    for (ActorRef client : clients) {
      client.tell(new Client.UpdateNodeList(new ArrayList<>(activeNodes)), self());
    }
  }

  /** Updates peer lists for all active nodes. */
  private void updateAllNodePeers() {
    Node.InitPeers initMsg = new Node.InitPeers(new ArrayList<>(activeNodes), new HashMap<>(nodeIdMap));
    for (ActorRef node : activeNodes) {
      node.tell(initMsg, self());
    }
  }

  /** Finds a node by its nodeId. */
  private ActorRef findNodeByNodeId(int nodeId) {
    for (Map.Entry<ActorRef, Integer> entry : nodeIdMap.entrySet()) {
      if (entry.getValue() == nodeId) {
        return entry.getKey();
      }
    }
    return null;
  }

  /** Logs current system status. */
  private void logSystemStatus() {
    System.out.println("[MarsSystemManager] === SYSTEM STATUS ===");
    System.out.println("[MarsSystemManager] Active nodes: " + activeNodes.size());
    System.out.println("[MarsSystemManager] Crashed nodes: " + crashedNodes.size());
    System.out.println("[MarsSystemManager] Clients: " + clients.size());
    
    if (!activeNodes.isEmpty()) {
      String activeIds = activeNodes.stream()
          .map(node -> String.valueOf(nodeIdMap.get(node)))
          .reduce((a, b) -> a + ", " + b)
          .orElse("");
      System.out.println("[MarsSystemManager] Active node IDs: [" + activeIds + "]");
    }
    
    if (!crashedNodes.isEmpty()) {
      String crashedIds = crashedNodes.stream()
          .map(node -> String.valueOf(nodeIdMap.get(node)))
          .reduce((a, b) -> a + ", " + b)
          .orElse("");
      System.out.println("[MarsSystemManager] Crashed node IDs: [" + crashedIds + "]");
    }
    
    System.out.println("[MarsSystemManager] =====================");
  }
}