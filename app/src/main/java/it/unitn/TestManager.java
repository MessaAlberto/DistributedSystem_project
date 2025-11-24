package it.unitn;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import it.unitn.Node.DataItem;
import it.unitn.Node.JoinGroupMsg;

/**
 * Central manager for the Mars distributed storage system.
 * Handles node lifecycle (join/leave/crash/recovery), and orchestrates test
 * scenarios according to project specifications.
 */
public class TestManager extends AbstractActor {

  private static final Logger logger = new Logger("TestManager");
  private final Random random = new Random();
  private CountDownLatch currentBatchLatch;

  /** System state **/
  private ActorSystem system;
  private final int N, R, W, timeoutSeconds, initialNodes;

  private final List<ActorRef> clients;
  private final List<ActorRef> activeNodes;
  private final List<ActorRef> crashedNodes;
  private final Map<ActorRef, Integer> nodeIdMap;

  // clientName -> list of requests: "READ key" or "WRITE key value"
  private final Map<String, List<String>> activeClientRequests;

  private boolean isViewChangedStable;

  private boolean isCommunicationTerminated() {
    return activeClientRequests.isEmpty();
  }

  // Printing store state
  private int printStore_counter = 0;
  private Map<Integer, Map<Integer, DataItem>> allNodeStores = new HashMap<>();

  /** Constructs the manager with replication parameters. */
  public TestManager(ActorSystem system, int N, int R, int W, int timeoutSeconds, int initialNodes) {
    this.system = system;
    this.N = N;
    this.R = R;
    this.W = W;
    this.timeoutSeconds = timeoutSeconds;
    this.initialNodes = initialNodes;

    this.clients = new ArrayList<>();
    this.activeNodes = new ArrayList<>();
    this.crashedNodes = new ArrayList<>();
    this.nodeIdMap = new HashMap<>();
    this.activeClientRequests = new HashMap<>();
    this.isViewChangedStable = true;
  }

  @Override
  public void preStart() {
    logger.log("Iniitalizing " + initialNodes + " nodes...");

    for (int i = 0; i < initialNodes; i++) {
      int nodeId = i * 10;
      ActorRef node = system.actorOf(Node.props(nodeId, N, R, W, self(), timeoutSeconds), "Node" + nodeId);
      activeNodes.add(node);
      nodeIdMap.put(node, nodeId);
    }

    JoinGroupMsg startMsg = new JoinGroupMsg(new ArrayList<>(activeNodes), new HashMap<>(nodeIdMap));
    for (ActorRef node : activeNodes) {
      node.tell(startMsg, self());
    }

    logger.log("All nodes initialized and joined the system.");
    logger.log("Creating multiple clients...");

    // Create 3 clients
    for (int i = 0; i < 3; i++) {
      String clientName = "client" + clients.size();
      ActorRef client = system.actorOf(Client.props(new ArrayList<>(activeNodes), clientName, self(), timeoutSeconds),
          clientName);
      clients.add(client);

      logger.log("Created " + clientName + " with access to " + activeNodes.size() + " nodes.");
    }

    logSystemStatus();
  }

  /** Akka factory method to create Props for this actor. */
  public static Props props(ActorSystem system, int N, int R, int W, int timeoutSeconds, int initialNodes) {
    return Props.create(TestManager.class, () -> new TestManager(system, N, R, W, timeoutSeconds, initialNodes));
  }

  // =====================
  // Message Classes
  // =====================

  public static class ClientRequest implements Serializable {
    public final int clientIndex; // index in clients list
    public final int key;
    public final String value; // null for READ, non-null for WRITE
    public CountDownLatch latch = null; // optional latch to signal batch completion

    public ClientRequest(int clientIndex, int key, String value) {
      this(clientIndex, key, value, null);
    }

    public ClientRequest(int clientIndex, int key, String value, CountDownLatch latch) {
      this.clientIndex = clientIndex;
      this.key = key;
      this.value = value;
      this.latch = latch;
    }
  }

  public static class NodeActionRequest implements Serializable {
    public final String action; // "join", "leave", "crash", "recover"
    public Integer nodeId = null; // optional specific node ID, null for random
    public final CountDownLatch latch;

    public NodeActionRequest(String action, CountDownLatch latch) {
      this.action = action;
      this.nodeId = null;
      this.latch = latch;
    }

    public NodeActionRequest(String action, int nodeId, CountDownLatch latch) {
      this.action = action;
      this.nodeId = nodeId;
      this.latch = latch;
    }
  }

  public static class NodeActionResponse implements Serializable {
    public final String action; // "join", "leave", "crash", "recover"
    public final int nodeId;
    public final boolean result;

    public NodeActionResponse(String action, int nodeId, boolean result) {
      this.action = action;
      this.nodeId = nodeId;
      this.result = result;
    }
  }

  public static class PrintStoreRequest implements Serializable {
  }

  public static class PrintStoreResponse implements Serializable {
    public final int nodeId;
    public final Map<Integer, DataItem> storeContent;

    public PrintStoreResponse(int nodeId, Map<Integer, DataItem> storeContent) {
      this.nodeId = nodeId;
      this.storeContent = new LinkedHashMap<>(storeContent);
    }
  }

  public static class LogSystemStatus implements Serializable {
  }

  // =====================
  // Message Handlers
  // =====================

  private void onClientRequest(ClientRequest msg) {
    if (!isViewChangedStable) {
      logger.logError("View change in progress, cannot process client request now.");
      if (msg.latch != null) {
        msg.latch.countDown();
      }
      return;
    }

    if (msg.latch != null) {
      currentBatchLatch = msg.latch;
    }

    String clientName = "client" + msg.clientIndex;
    activeClientRequests.computeIfAbsent(clientName, k -> new ArrayList<>())
        .add((msg.value == null ? "READ " : "WRITE ") + msg.key + (msg.value != null ? " " + msg.value : ""));

    if (msg.value == null) {
      clients.get(msg.clientIndex).tell(new Client.Read(msg.key), self());
    } else {
      clients.get(msg.clientIndex).tell(new Client.Write(msg.key, msg.value), self());
    }
  }

  private void onNodeActionRequest(NodeActionRequest msg) {
    if (!isViewChangedStable || !isCommunicationTerminated()) {
      if (!isViewChangedStable)
        logger.logError("View change in progress, cannot perform action " + msg.action + " now.");
      else
        logger.logError("Client operations in progress, cannot perform action " + msg.action + " now.");

      msg.latch.countDown();
      return;
    }

    currentBatchLatch = msg.latch;
    handleNodeAction(msg.action, msg.nodeId);
  }

  private void onNodeActionResponse(NodeActionResponse msg) {
    if (msg.result) {
      if (msg.action.equals("join")) {
        if (nodeIdMap.containsKey(getSender())) {
          logger.logError("Node already in the system after join: " + getSender().path().name());
          return;
        }
        nodeIdMap.put(getSelf(), msg.nodeId);
        activeNodes.add(getSender());
        logger.log("JOIN completed: Node " + msg.nodeId + " joined the system");
      } else if (msg.action.equals("leave")) {
        if (!nodeIdMap.containsKey(getSender())) {
          logger.logError("Node not found in the system after leave: " + getSender().path().name());
          return;
        }
        activeNodes.remove(getSender());
        nodeIdMap.remove(getSender());
        logger.log("LEAVE completed: Node " + msg.nodeId + " left the system");
      } else if (msg.action.equals("crash")) {
        if (!nodeIdMap.containsKey(getSender())) {
          logger.logError("Node not found in the system after crash: " + getSender().path().name());
          return;
        }
        activeNodes.remove(getSender());
        crashedNodes.add(getSender());
        logger.log("CRASH completed: Node " + msg.nodeId + " crashed");
      } else if (msg.action.equals("recover")) {
        if (!nodeIdMap.containsKey(getSender())) {
          logger.logError("Node not found in the system after recover: " + getSender().path().name());
          return;
        }
        crashedNodes.remove(getSender());
        activeNodes.add(getSender());
        logger.log("RECOVER completed: Node " + msg.nodeId + " recovered");
      } else {
        logger.logError("Unknown action in NodeActionResponse: " + msg.action);
        return;
      }

      updateNodeStateAndClients(msg.action, getSender(), msg.nodeId);
    } else {
      logger.logError("Node action failed: " + msg.action + " for Node " + msg.nodeId);
    }
    isViewChangedStable = true;

    if (currentBatchLatch != null) {
      currentBatchLatch.countDown();
      // Reset latch only if no pending client requests
      if (activeClientRequests.isEmpty()) {
        logger.log("Node action batch completed.");
        currentBatchLatch = null;
      }
    }
  }

  private void onClientResponse(Client.ClientResponse msg) {
    String clientName = getSender().path().name();
    List<String> requests = activeClientRequests.get(clientName);

    if (requests != null && !requests.isEmpty()) {
      requests.remove(msg.id);
      if (requests.isEmpty()) {
        activeClientRequests.remove(clientName);
      }
    }

    logger.log("Received client response: " + msg.id + " success=" + msg.success);

    // Check if the batch is finished
    if (currentBatchLatch != null) {
      if (activeClientRequests.isEmpty()) {
        // batch finished: reset for next batch
        logger.log("Client request batch completed.");
        currentBatchLatch.countDown();
        currentBatchLatch = null;
      } else {
        logger.log("Pending client requests remain, batch not yet complete.");
        currentBatchLatch.countDown();
      }
    }
  }

  private void onPrintStoreRequest(PrintStoreRequest msg) {
    printStore_counter = 0;
    allNodeStores.clear();

    logger.log("=== PRINTING STORE CONTENTS OF ALL ACTIVE NODES ===");
    for (ActorRef node : activeNodes) {
      node.tell(new Node.PrintStore(), self());
    }
  }

  private void onPrintStoreResponse(PrintStoreResponse msg) {
    printStore_counter++;
    allNodeStores.put(msg.nodeId, msg.storeContent);

    if (printStore_counter == activeNodes.size()) {
      logger.log("=== COMPLETED PRINTING STORE CONTENTS OF ALL ACTIVE NODES ===");
      allNodeStores = allNodeStores.entrySet().stream()
          .sorted(Map.Entry.comparingByKey())
          .collect(LinkedHashMap::new, (m, e) -> m.put(e.getKey(), e.getValue()), LinkedHashMap::putAll);

      for (Map.Entry<Integer, Map<Integer, DataItem>> entry : allNodeStores.entrySet()) {
        int nodeId = entry.getKey();
        Map<Integer, DataItem> store = entry.getValue();
        if (store.isEmpty()) {
          logger.log("Node " + nodeId + ": (empty)");
        } else {
          logger.log("Node " + nodeId + ":");
          for (Map.Entry<Integer, DataItem> item : store.entrySet()) {
            logger.log("  Key: " + item.getKey() + " -> Value: \"" + item.getValue().value + "\" (v"
                + item.getValue().version + ")");
          }
        }
      }
      printStore_counter = 0;
      allNodeStores.clear();
    }
  }

  // =====================
  // Helper Methods
  // =====================

  public class RingUtils {
    /**
     * Finds the successor peer for a given id (first peer with id >= given id, with
     * wrap).
     */
    public static ActorRef findSuccessorOfId(int id, List<ActorRef> list, Map<ActorRef, Integer> map) {
      List<ActorRef> sorted = new ArrayList<>(list);
      sorted.sort(Comparator.comparingInt(map::get));

      for (ActorRef p : sorted) {
        if (map.get(p) >= id)
          return p;
      }
      return sorted.isEmpty() ? null : sorted.get(0);
    }

    /**
     * Finds the predecessor peer for a given id (largest id < given id, or last on
     * wrap).
     */
    public static ActorRef findPredecessorOfId(int id, List<ActorRef> list, Map<ActorRef, Integer> map) {
      List<ActorRef> sorted = new ArrayList<>(list);
      sorted.sort(Comparator.comparingInt(map::get));

      ActorRef pred = null;
      int predId = Integer.MIN_VALUE;
      for (ActorRef p : sorted) {
        int pid = map.get(p);
        if (pid < id && pid > predId) {
          predId = pid;
          pred = p;
        }
      }
      if (pred != null)
        return pred;
      return sorted.isEmpty() ? null : sorted.get(sorted.size() - 1);
    }
  }

  /** Logs current system status. */
  private void logSystemStatus() {
    logger.log("\n");
    logger.log("=====================");
    logger.log("=== SYSTEM STATUS ===");
    logger.log("Active nodes: " + activeNodes.size());
    logger.log("Crashed nodes: " + crashedNodes.size());

    if (!activeNodes.isEmpty()) {
      String activeIds = activeNodes.stream()
          .map(node -> String.valueOf(nodeIdMap.get(node)))
          .reduce((a, b) -> a + ", " + b)
          .orElse("");
      logger.log("Active node IDs: [" + activeIds + "]");
    }

    if (!crashedNodes.isEmpty()) {
      String crashedIds = crashedNodes.stream()
          .map(node -> String.valueOf(nodeIdMap.get(node)))
          .reduce((a, b) -> a + ", " + b)
          .orElse("");
      logger.log("Crashed node IDs: [" + crashedIds + "]");
    }

    logger.log("Client list: " + clients.size());
    if (!clients.isEmpty()) {
      String clientNames = clients.stream()
          .map(client -> client.path().name())
          .reduce((a, b) -> a + ", " + b)
          .orElse("");
      logger.log("Clients: [" + clientNames + "]");
    }

    logger.log("=====================");
  }

  /**
   * // * Updates node lists and notifies clients for join/leave/crash/recover
   * // * @param action "join", "leave", "crash", "recover"
   * //
   */
  private void updateNodeStateAndClients(String action, ActorRef node, Integer nodeId) {
    switch (action.toLowerCase()) {
      case "join":
        activeNodes.add(node);
        nodeIdMap.put(node, nodeId);
        break;
      case "leave":
        activeNodes.remove(node);
        nodeIdMap.remove(node);
        break;
      case "crash":
        activeNodes.remove(node);
        crashedNodes.add(node);
        break;
      case "recover":
        crashedNodes.remove(node);
        activeNodes.add(node);
        break;
      default:
        throw new IllegalArgumentException("Unknown action: " + action);
    }

    // Notify clients
    List<ActorRef> snapshot = new ArrayList<>(activeNodes);
    for (ActorRef client : clients)
      client.tell(new Client.UpdateNodeList(snapshot), ActorRef.noSender());
  }

  private static ActorRef findNode(Integer id, List<ActorRef> nodeList, Map<ActorRef, Integer> nodeIdMap,
      Random random) {
    if (id != null) {
      return nodeList.stream().filter(n -> nodeIdMap.get(n).equals(id)).findFirst().orElse(null);
    } else if (!nodeList.isEmpty()) {
      return nodeList.get(random.nextInt(nodeList.size()));
    }

    logger.logError("No nodes available to select from.");
    return null;
  }

  private void handleNodeAction(String action, Integer nodeId) {
    ActorRef targetNode;
    int targetId;
    String act = action.toLowerCase();
    ActorRef bootstrap = null;

    switch (act) {
      case "join":
        targetId = (nodeId != null) ? nodeId : nodeIdMap.values().stream().max(Integer::compare).orElse(0) + 10;
        if (nodeIdMap.containsValue(targetId)) {
          logger.logError("Node ID " + targetId + " already exists.");
          return;
        }

        isViewChangedStable = false;
        bootstrap = activeNodes.get(random.nextInt(activeNodes.size()));
        targetNode = system.actorOf(Node.props(targetId, N, R, W, true, self(), bootstrap, timeoutSeconds),
            "Node" + targetId);

        logger.log("JOIN completed: Node " + targetId + " joined the system");
        break;

      case "leave":
      case "crash":
      case "recover":
        List<ActorRef> sourceList = switch (act) {
          case "leave", "crash" -> activeNodes;
          case "recover" -> crashedNodes;
          default -> List.of();
        };
        if (sourceList.isEmpty()) {
          logger.logError("No nodes available for action " + action);
          return;
        }
        targetNode = findNode(nodeId, sourceList, nodeIdMap, random);
        if (targetNode == null)
          return;

        isViewChangedStable = false;
        targetId = nodeIdMap.get(targetNode);
        if (act.equals("recover")) {

          bootstrap = activeNodes.get(random.nextInt(activeNodes.size()));
          targetNode.tell(new Node.NodeAction(act, bootstrap), ActorRef.noSender());
        } else {
          targetNode.tell(new Node.NodeAction(act), ActorRef.noSender());
        }
        logger.log(act.toUpperCase() + " completed: Node " + targetId);
        break;

      default:
        logger.logError("Unknown action: " + action);
    }
  }

  /**
   * Interactive mode for manual testing
   */
  // private static void runInteractiveMode(
  // ActorSystem system,
  // ActorRef manager,
  // Scanner scanner,
  // int N, int R, int W,
  // List<ActorRef> activeNodes,
  // List<ActorRef> crashedNodes,
  // Map<ActorRef, Integer> nodeIdMap,
  // List<ActorRef> clients) throws InterruptedException {
  // logger.log("\n=== INTERACTIVE MODE ===");
  // logger.log("Commands:");
  // logger.log(" put <key> <value> - Store a key-value pair");
  // logger.log(" get <key> - Retrieve a value");
  // logger.log(" join [nodeId] - Add a new node");
  // logger.log(" leave [nodeId] - Remove a node");
  // logger.log(" crash [nodeId] - Crash a node");
  // logger.log(" recover [nodeId] - Recover a crashed node");
  // logger.log(" client - Create a new client");
  // logger.log(" status - Show system status");
  // logger.log(" exit - Exit interactive mode");
  // logger.log("");

  // while (true) {
  // System.out.print("mars> ");
  // String input = scanner.nextLine().trim();

  // if (input.isEmpty())
  // continue;

  // String[] parts = input.split("\\s+");
  // String command = parts[0].toLowerCase();

  // try {
  // switch (command) {
  // case "put":
  // if (parts.length >= 3) {
  // int key = Integer.parseInt(parts[1]);
  // String value = String.join(" ", java.util.Arrays.copyOfRange(parts, 2,
  // parts.length));
  // clients.get(0).tell(new Client.Write(key, value), ActorRef.noSender());
  // logger.log("Put request sent: key=" + key + ", value=\"" + value + "\"");
  // } else {
  // logger.log("Usage: put <key> <value>");
  // }
  // break;

  // case "get":
  // if (parts.length >= 2) {
  // int key = Integer.parseInt(parts[1]);
  // clients.get(0).tell(new Client.Read(key), ActorRef.noSender());
  // logger.log("Read request sent: key=" + key);
  // } else {
  // logger.log("Usage: get <key>");
  // }
  // break;

  // case "join":
  // Integer nodeId = (parts.length >= 2) ? Integer.parseInt(parts[1]) : null;
  // handleNodeAction(system, manager, activeNodes, crashedNodes, nodeIdMap,
  // clients, "join", nodeId, N, R, W);
  // logger.log("Join request sent");
  // break;

  // case "leave":
  // nodeId = (parts.length >= 2) ? Integer.parseInt(parts[1]) : null;
  // handleNodeAction(system, manager, activeNodes, crashedNodes, nodeIdMap,
  // clients, "leave", nodeId, N, R, W);
  // logger.log("Leave request sent");
  // break;

  // case "crash":
  // nodeId = (parts.length >= 2) ? Integer.parseInt(parts[1]) : null;
  // handleNodeAction(system, manager, activeNodes, crashedNodes, nodeIdMap,
  // clients, "crash", nodeId, N, R, W);
  // logger.log("Crash request sent");
  // break;

  // case "recover":
  // nodeId = (parts.length >= 2) ? Integer.parseInt(parts[1]) : null;
  // handleNodeAction(system, manager, activeNodes, crashedNodes, nodeIdMap,
  // clients, "recover", nodeId, N, R,
  // W);
  // logger.log("Recovery request sent");
  // break;

  // case "client":
  // String clientName = "client" + clients.size();
  // ActorRef client = system.actorOf(Client.props(new ArrayList<>(activeNodes),
  // clientName), clientName);
  // clients.add(client);
  // logger.log("Client creation request sent");
  // break;

  // case "status":
  // manager.tell(new MarsSystemManager.GetSystemStatus(), ActorRef.noSender());
  // break;

  // case "exit":
  // logger.log("Exiting interactive mode...");
  // return;

  // default:
  // logger.log("Unknown command: " + command);
  // }

  // // Thread.sleep(500); // Give some time for the operation

  // } catch (NumberFormatException e) {
  // logger.log("Invalid number format");
  // } catch (Exception e) {
  // logger.log("Error: " + e.getMessage());
  // }
  // }
  // }

  // =====================
  // Tests
  // =====================

  /**
   * Test 4: Mixed operations stress test
   * - Rapid sequence of various operations
   * - Tests system under load
   * - Includes membership changes during operations
   */
  // private void testMixedOperationsScenario() {
  // logger.log("Starting mixed operations stress test...");

  // // Phase 1: Rapid data operations
  // logger.log("Phase 1: Rapid data operations...");
  // for (int i = 2000; i < 2010; i++) {
  // clients.get(0).tell(new Client.Write(i, "StressData" + i),
  // ActorRef.noSender());
  // // Thread.sleep(50); // Network propagation delay simulation
  // }

  // // Thread.sleep(1000);

  // // Phase 2: Mixed reads and writes
  // logger.log("Phase 2: Mixed reads and writes...");
  // for (int i = 2000; i < 2010; i++) {
  // if (i % 2 == 0) {
  // clients.get(0).tell(new Client.Read(i), ActorRef.noSender());
  // } else {
  // clients.get(0).tell(new Client.Write(i, "Writed" + i),
  // ActorRef.noSender());
  // }
  // // Thread.sleep(100); // Network propagation delay
  // }

  // // Thread.sleep(2000);

  // // Phase 3: Operations with membership changes
  // logger.log("Phase 3: Operations during membership changes...");

  // // Start some operations
  // clients.get(0).tell(new Client.Write(3000, "BeforeJoin"),
  // ActorRef.noSender());
  // // Thread.sleep(200);

  // // Add a node while operations are potentially ongoing
  // logger.log("Adding node during operations...");
  // handleNodeAction(system, activeNodes, crashedNodes, nodeIdMap, clients,
  // "join", null, N, R, W);

  // // Continue operations
  // clients.get(0).tell(new Client.Write(3001, "DuringJoin"),
  // ActorRef.noSender());
  // // Thread.sleep(200);
  // clients.get(0).tell(new Client.Read(3000), ActorRef.noSender());

  // // Thread.sleep(3000); // Wait for join to complete

  // // More operations after join
  // clients.get(0).tell(new Client.Write(3002, "AfterJoinCompleted"),
  // ActorRef.noSender());
  // // Thread.sleep(500);

  // // Crash a node
  // logger.log("Crashing node during operations...");
  // handleNodeAction(system, activeNodes, crashedNodes, nodeIdMap, clients,
  // "crash", null, N, R, W);

  // // Continue operations with crashed node
  // clients.get(0).tell(new Client.Read(3001), ActorRef.noSender());
  // // Thread.sleep(200);
  // clients.get(0).tell(new Client.Write(3003, "WithCrashedNode"),
  // ActorRef.noSender());

  // // Thread.sleep(2000);

  // logger.log("Mixed operations stress test completed.\n");
  // }

  // =====================
  // Actor Lifecycle
  // =====================

  /** Message routing for the manager. */
  @Override
  public Receive createReceive() {
    return receiveBuilder()
        .match(ClientRequest.class, this::onClientRequest)
        .match(NodeActionRequest.class, this::onNodeActionRequest)
        .match(NodeActionResponse.class, this::onNodeActionResponse)
        .match(Client.ClientResponse.class, this::onClientResponse)
        .match(PrintStoreRequest.class, this::onPrintStoreRequest)
        .match(PrintStoreResponse.class, this::onPrintStoreResponse)
        .match(LogSystemStatus.class, msg -> logSystemStatus())
        .build();
  }
}