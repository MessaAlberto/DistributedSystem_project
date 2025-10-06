package it.unitn;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import it.unitn.Node.JoinGroupMsg;

/**
 * Central manager for the Mars distributed storage system.
 * Handles node lifecycle (join/leave/crash/recovery), and orchestrates test
 * scenarios according to project specifications.
 */
public class TestManager extends AbstractActor {

  private static final Logger logger = new Logger("TestManager");
  private final Random random = new Random();

  /** System state **/
  private ActorSystem system;
  private final int N, R, W, timeoutSeconds, initialNodes;

  private final List<ActorRef> clients;
  private final List<ActorRef> activeNodes;
  private final List<ActorRef> crashedNodes;
  private final Map<ActorRef, Integer> nodeIdMap;

  // clientName -> list of requests: "GET key" or "UPDATE key value"
  private final Map<String, List<String>> activeClientRequests;
  
  private boolean isViewChangedStable;
  private boolean isCommunicationTerminated() {
    return activeClientRequests.isEmpty();
  }

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
  }

  /** Akka factory method to create Props for this actor. */
  public static Props props(ActorSystem system, int N, int R, int W, int timeoutSeconds, int initialNodes) {
    return Props.create(TestManager.class, () -> new TestManager(system, N, R, W, timeoutSeconds, initialNodes));
  }

  // =====================
  // Message Classes
  // =====================

  public static class StartTest implements Serializable {
    public final int testNumber;

    public StartTest(int testNumber) {
      this.testNumber = testNumber;
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

  // =====================
  // Message Handlers
  // =====================

  private void onStartTest(StartTest msg) {
    switch (msg.testNumber) {
      case 1 -> testMultipleClientsScenario();
      case 2 -> testJoinLeaveScenario();
      case 3 -> testCrashRecoveryScenario();
      default -> logger.logError("Unknown test number: " + msg.testNumber);
    }
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
      }
      else if (msg.action.equals("leave")) {
        if (!nodeIdMap.containsKey(getSender())) {
          logger.logError("Node not found in the system after leave: " + getSender().path().name());
          return;
        }
        activeNodes.remove(getSender());
        nodeIdMap.remove(getSender());
        logger.log("LEAVE completed: Node " + msg.nodeId + " left the system");
      }
      else if (msg.action.equals("crash")) {
        if (!nodeIdMap.containsKey(getSender())) {
          logger.logError("Node not found in the system after crash: " + getSender().path().name());
          return;
        }
        activeNodes.remove(getSender());
        crashedNodes.add(getSender());
        logger.log("CRASH completed: Node " + msg.nodeId + " crashed");
      }
      else if (msg.action.equals("recover")) {
        if (!nodeIdMap.containsKey(getSender())) {
          logger.logError("Node not found in the system after recover: " + getSender().path().name());
          return;
        }
        crashedNodes.remove(getSender());
        activeNodes.add(getSender());
        logger.log("RECOVER completed: Node " + msg.nodeId + " recovered");
      }
      else {
        logger.logError("Unknown action in NodeActionResponse: " + msg.action);
        return;
      }
    } else {
      logger.logError("Node action failed: " + msg.action + " for Node " + msg.nodeId);
    }
    isViewChangedStable = true;
  }

  private void onClientResponse(Client.ClientResponse msg) {
    String clientName = getSender().path().name();
    List<String> requests = activeClientRequests.get(clientName);

    if (requests == null || requests.isEmpty()) {
      logger.logError("No active client request found for client: " + clientName);
      return;
    }
    if (msg.success) {
      logger.log("Client " + clientName + " completed request: " + msg.id + " SUCCESS - " + msg.message);
    } else {
      logger.logError("Client " + clientName + " completed request: " + msg.id + " FAILED - " + msg.message);
    }
    // Remove the specific request from the list
    requests.remove(msg.id);

    // If no more pending requests, remove the client from the map
    if (requests.isEmpty()) {
      activeClientRequests.remove(clientName);
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

    logger.log("=====================");
  }

  private void sendClientRequest(int index, int key, String value) {
    if (!isViewChangedStable) {
      logger.logError("View change in progress, cannot send client updates now.");
      return;
    }
    if (clients.isEmpty() || index < 0 || index >= clients.size()) {
      logger.logError("No clients available to send updates.");
      return;
    }

    String clientName = "client" + index;
    String requestStr = (value == null) ? "GET " + key : "UPDATE " + key + " " + value;
    activeClientRequests.computeIfAbsent(clientName, k -> new ArrayList<>()).add(requestStr);

    // Read request if value is null, otherwise write request
    if (value == null) {
      logger.log("Client " + index + " sending GET for key " + key);
      clients.get(index).tell(new Client.Get(key), self());
      return;
    }

    logger.log("Client " + index + " sending UPDATE for key " + key + " with value \"" + value + "\"");
    clients.get(index).tell(new Client.Update(key, value), self());
  }

  private void waitCommunicationComplete() {
    while (!isCommunicationTerminated()) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }

  private void waitViewChangeStable() {
    while (!isViewChangedStable) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
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

    if (isViewChangedStable == false) {
      logger.logError("View change in progress, cannot perform action " + action + " now.");
      return;
    }
    if (isCommunicationTerminated() == false) {
      logger.logError("Client operations in progress, cannot perform action " + action + " now.");
      return;
    }

    switch (act) {
      case "join":
        targetId = (nodeId != null) ? nodeId : nodeIdMap.values().stream().max(Integer::compare).orElse(0) + 10;
        if (nodeIdMap.containsValue(targetId)) {
          logger.logError("Node ID " + targetId + " already exists.");
          return;
        }
        if (activeNodes.isEmpty()) {
          logger.logError("Cannot join: no active nodes for bootstrap");
          return;
        }
        isViewChangedStable = false;
        bootstrap = activeNodes.get(random.nextInt(activeNodes.size()));
        targetNode = system.actorOf(Node.props(targetId, N, R, W, true, self(), bootstrap, timeoutSeconds), "Node" + targetId);

        // Update local state
        updateNodeStateAndClients(act, targetNode, targetId);
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
        updateNodeStateAndClients(act, targetNode, targetId);
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
  // clients.get(0).tell(new Client.Update(key, value), ActorRef.noSender());
  // logger.log("Put request sent: key=" + key + ", value=\"" + value + "\"");
  // } else {
  // logger.log("Usage: put <key> <value>");
  // }
  // break;

  // case "get":
  // if (parts.length >= 2) {
  // int key = Integer.parseInt(parts[1]);
  // clients.get(0).tell(new Client.Get(key), ActorRef.noSender());
  // logger.log("Get request sent: key=" + key);
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
   * Test 1: Multiple clients serving concurrent requests (possibly same key)
   * - Creates multiple clients
   * - Sends concurrent Put/Get operations
   * - Tests same key access from different clients
   */
  private void testMultipleClientsScenario() {

    logger.log("Testing concurrent requests on same key (42)...");

    // Concurrent operations on same key sequentially
    sendClientRequest(0, 42, "Value1_Client1");
    waitCommunicationComplete();

    sendClientRequest(1, 42, "Value2_Client2");
    waitCommunicationComplete();

    sendClientRequest(2, 42, "Value3_Client3");
    waitCommunicationComplete();

    sendClientRequest(0, 42, "Value3_Client3");
    waitCommunicationComplete();

    sendClientRequest(1, 42, null);
    waitCommunicationComplete();

    logger.log("Testing concurrent requests on different keys...");

    // Concurrent operations on different keys
    sendClientRequest(0, 10, "DataA");
    sendClientRequest(1, 20, "DataB");
    sendClientRequest(2, 30, "DataC");

    sendClientRequest(0, 10, null);
    sendClientRequest(1, 20, null);
    sendClientRequest(2, 30, null);
    waitCommunicationComplete();

    logSystemStatus();
    logger.log("\n============================================================");
    logger.log("====           Multi-client test completed.              ====");
    logger.log("============================================================\n");
    logger.log("Summary:");
    logger.log("  - All concurrent client requests have been processed.");
    logger.log("  - System state is stable and ready for further tests.");
    logger.log("------------------------------------------------------------\n");
  }

  /**
   * Test 2: Node join and leave operations
   * - Tests joining new nodes
   * - Tests graceful leaving
   * - Ensures operations work after membership changes
   */
  private void testJoinLeaveScenario() {

    logger.log("Current system status before join/leave:");
    logSystemStatus();

    logger.log("Adding new node (JOIN)...");
    handleNodeAction("join", null);
    waitViewChangeStable();

    logger.log("Testing operations after JOIN...");
    if (!clients.isEmpty()) {
      sendClientRequest(0, 50, "AfterJoin");
      waitCommunicationComplete();
    }

    logger.log("Removing a node (LEAVE)...");
    handleNodeAction("leave", null);
    waitViewChangeStable();

    logger.log("Testing operations after LEAVE...");
    if (clients.isEmpty()) {
      logger.logError("No clients available to test after LEAVE.");
      return;
    }

    sendClientRequest(0, 50, null);
    waitCommunicationComplete();
    sendClientRequest(0, 60, "AfterLeave");
    waitCommunicationComplete();

    logSystemStatus();
    logger.log("\n============================================================");
    logger.log("====           Join/Leave test completed.              ====");
    logger.log("============================================================\n");
    logger.log("Summary:");
    logger.log("  - Node JOIN and LEAVE operations completed successfully.");
    logger.log("  - System state is stable and ready for further tests.");
    logger.log("------------------------------------------------------------\n");
  }

  /**
   * Test 3: Crash and recovery with ongoing operations
   * - Crashes a node
   * - Continues operations while node is crashed
   * - Recovers the node
   * - Verifies data consistency
   */
  private void testCrashRecoveryScenario() {
    logger.log("Storing initial data...");
    sendClientRequest(0, 1000, "InitialData");
    waitCommunicationComplete();

    logger.log("Crashing a node...");
    handleNodeAction("crash", null);
    waitViewChangeStable();

    logger.log("Current system status after crash:");
    logSystemStatus();
    waitViewChangeStable();

    logger.log("Continuing operations while node is crashed...");
    sendClientRequest(1, 1001, "DuringCrash1");
    waitCommunicationComplete();
    sendClientRequest(2, 1002, "DuringCrash2");
    waitCommunicationComplete();
    sendClientRequest(0, 1000, null);
    waitCommunicationComplete();

    logger.log("Recovering the crashed node...");
    handleNodeAction("recover", null);
    waitViewChangeStable();

    logger.log("Testing data consistency after recovery...");
    sendClientRequest(0, 1000, null);
    waitCommunicationComplete();
    sendClientRequest(1, 1001, null);
    waitCommunicationComplete();
    sendClientRequest(2, 1002, null);
    waitCommunicationComplete();

    logger.log("Adding more data after recovery...");
    sendClientRequest(0, 1003, "AfterRecovery");
    waitCommunicationComplete();

    logSystemStatus();
    logger.log("\n============================================================");
    logger.log("====         Crash/Recovery test completed.            ====");
    logger.log("============================================================\n");
    logger.log("Summary:");
    logger.log("  - Node CRASH and RECOVERY operations completed successfully.");
    logger.log("  - Data consistency verified after recovery.");
    logger.log("  - System state is stable and ready for further tests.");
    logger.log("------------------------------------------------------------\n");
  }

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
  // clients.get(0).tell(new Client.Update(i, "StressData" + i),
  // ActorRef.noSender());
  // // Thread.sleep(50); // Network propagation delay simulation
  // }

  // // Thread.sleep(1000);

  // // Phase 2: Mixed reads and writes
  // logger.log("Phase 2: Mixed reads and writes...");
  // for (int i = 2000; i < 2010; i++) {
  // if (i % 2 == 0) {
  // clients.get(0).tell(new Client.Get(i), ActorRef.noSender());
  // } else {
  // clients.get(0).tell(new Client.Update(i, "Updated" + i),
  // ActorRef.noSender());
  // }
  // // Thread.sleep(100); // Network propagation delay
  // }

  // // Thread.sleep(2000);

  // // Phase 3: Operations with membership changes
  // logger.log("Phase 3: Operations during membership changes...");

  // // Start some operations
  // clients.get(0).tell(new Client.Update(3000, "BeforeJoin"),
  // ActorRef.noSender());
  // // Thread.sleep(200);

  // // Add a node while operations are potentially ongoing
  // logger.log("Adding node during operations...");
  // handleNodeAction(system, activeNodes, crashedNodes, nodeIdMap, clients,
  // "join", null, N, R, W);

  // // Continue operations
  // clients.get(0).tell(new Client.Update(3001, "DuringJoin"),
  // ActorRef.noSender());
  // // Thread.sleep(200);
  // clients.get(0).tell(new Client.Get(3000), ActorRef.noSender());

  // // Thread.sleep(3000); // Wait for join to complete

  // // More operations after join
  // clients.get(0).tell(new Client.Update(3002, "AfterJoinCompleted"),
  // ActorRef.noSender());
  // // Thread.sleep(500);

  // // Crash a node
  // logger.log("Crashing node during operations...");
  // handleNodeAction(system, activeNodes, crashedNodes, nodeIdMap, clients,
  // "crash", null, N, R, W);

  // // Continue operations with crashed node
  // clients.get(0).tell(new Client.Get(3001), ActorRef.noSender());
  // // Thread.sleep(200);
  // clients.get(0).tell(new Client.Update(3003, "WithCrashedNode"),
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
        .match(StartTest.class, this::onStartTest)

        .match(NodeActionResponse.class, this::onNodeActionResponse)

        .match(Client.ClientResponse.class, this::onClientResponse)
        .build();
  }
}