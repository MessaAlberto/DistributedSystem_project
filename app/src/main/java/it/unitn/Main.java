package it.unitn;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Scanner;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import it.unitn.Node.JoinGroupMsg;

public class Main {

  private static final Logger logger = new Logger("Main");

  public static void main(String[] args) throws InterruptedException {

    Properties config = new Properties();
    try (InputStream input = Main.class.getClassLoader().getResourceAsStream("config.properties")) {
      if (input == null) {
        logger.log("Failed to load config.properties, using default values");
      } else {
        config.load(input);
      }
    } catch (IOException e) {
      logger.logError("Error loading config.properties");
    }

    final int N = Integer.parseInt(config.getProperty("N", "5")); // Replication factor
    final int R = Integer.parseInt(config.getProperty("R", "3"));
    final int W = Integer.parseInt(config.getProperty("W", "4"));

    logger.log("=== Mars Distributed Storage System ===");
    logger.log("Configuration: N=" + N + ", R=" + R + ", W=" + W);
    logger.log("========================================\n");

    final ActorSystem system = ActorSystem.create("MarsSystem");
    final ActorRef manager = system.actorOf(MarsSystemManager.props(), "marsManager");

    List<ActorRef> clients = new ArrayList<>();
    List<ActorRef> activeNodes = new ArrayList<>();
    List<ActorRef> crashedNodes = new ArrayList<>();
    Map<ActorRef, Integer> nodeIdMap = new HashMap<>();

    // Initialize nodes (initialNodes from config, default 10)
    int initialNodes = Integer.parseInt(config.getProperty("initialNodes", "10"));
    logger.log("Initializing system with " + initialNodes + " nodes...\n");

    if (initialNodes < 1)
      initialNodes = 10;

    for (int i = 0; i < initialNodes; i++) {
      int nodeId = i * 10;
      ActorRef node = system.actorOf(Node.props(nodeId, N, R, W, false, manager), "Node" + nodeId);
      activeNodes.add(node);
      nodeIdMap.put(node, nodeId);
    }

    JoinGroupMsg start = new JoinGroupMsg(activeNodes, nodeIdMap);

    manager.tell(start, ActorRef.noSender());
    for (ActorRef node : activeNodes) {
      node.tell(start, ActorRef.noSender());
    }

    Thread.sleep(2000); // Wait for initialization

    // Test Scenario 1: Multiple clients with concurrent requests
    logger.log("\n=== TEST 1: Multiple Clients + Concurrent Requests ===");
    testMultipleClientsScenario(system, manager, clients, activeNodes);
    System.exit(0);
    // Test Scenario 2: Join/Leave operations
    logger.log("\n=== TEST 2: Node Join/Leave Operations ===");
    // testJoinLeaveScenario(system, manager, activeNodes, clients, crashedNodes, nodeIdMap, N, R, W);

    // // Test Scenario 3: Crash/Recovery with ongoing operations
    // logger.log("\n=== TEST 3: Crash/Recovery + Concurrent Operations ===");
    // testCrashRecoveryScenario(system, manager, activeNodes, crashedNodes, nodeIdMap, N, R, W, clients);

    // // Test Scenario 4: Stress test with mixed operations
    // logger.log("\n=== TEST 4: Mixed Operations Stress Test ===");
    // testMixedOperationsScenario(system, manager, activeNodes, crashedNodes, nodeIdMap, N, R, W, clients);

    // Final status
    logger.log("\n=== FINAL SYSTEM STATUS ===");
    manager.tell(new MarsSystemManager.GetSystemStatus(), ActorRef.noSender());

    Thread.sleep(3000);

    // Interactive mode (optional)
    logger.log("\n=== INTERACTIVE MODE ===");
    logger.log("Press 'i' for interactive mode, or any other key to exit...");

    Scanner scanner = new Scanner(System.in);
    String input = scanner.nextLine();
    if ("i".equalsIgnoreCase(input.trim())) {
      runInteractiveMode(system, manager, scanner, N, R, W, activeNodes, crashedNodes, nodeIdMap, clients);
    }

    system.terminate();
    system.getWhenTerminated().toCompletableFuture().join();
    logger.log("\nSystem terminated. Mars mission complete!\n");
  }

  /**
   * Test 1: Multiple clients serving concurrent requests (possibly same key)
   * - Creates multiple clients
   * - Sends concurrent Put/Get operations
   * - Tests same key access from different clients
   */
  private static void testMultipleClientsScenario(
      ActorSystem system,
      ActorRef manager,
      List<ActorRef> clients,
      List<ActorRef> activeNodes) throws InterruptedException {

    logger.log("Creating multiple clients...");

    // Create 3 clients
    for (int i = 0; i < 3; i++) {
      String clientName = "client" + clients.size();
      ActorRef client = system.actorOf(Client.props(new ArrayList<>(activeNodes), clientName), clientName);
      clients.add(client);

      logger.log("Created " + clientName + " with access to " + activeNodes.size() + " nodes.");
      Thread.sleep(500);
    }

    logger.log("Testing concurrent requests on same key (42)...");

    // Concurrent operations on same key
    clients.get(0).tell(new Client.Update(42, "Value1_Client1"), ActorRef.noSender());
    Thread.sleep(100);

    clients.get(1).tell(new Client.Update(42, "Value2_Client2"), ActorRef.noSender());
    Thread.sleep(100);

    clients.get(2).tell(new Client.Get(42), ActorRef.noSender());
    Thread.sleep(100);

    clients.get(0).tell(new Client.Update(42, "Value3_Client3"), ActorRef.noSender());
    Thread.sleep(100);

    clients.get(1).tell(new Client.Get(42), ActorRef.noSender());

    Thread.sleep(2000); // Wait for operations to complete
    logger.log("Testing concurrent requests on different keys...");

    // Concurrent operations on different keys
    clients.get(0).tell(new Client.Update(10, "DataA"), ActorRef.noSender());
    clients.get(1).tell(new Client.Update(20, "DataB"), ActorRef.noSender());
    clients.get(2).tell(new Client.Update(30, "DataC"), ActorRef.noSender());

    Thread.sleep(1000);

    clients.get(0).tell(new Client.Get(10), ActorRef.noSender());
    clients.get(1).tell(new Client.Get(20), ActorRef.noSender());
    clients.get(2).tell(new Client.Get(30), ActorRef.noSender());

    Thread.sleep(3000);
    logger.log("Multi-client test completed.\n");
  }

  /**
   * Test 2: Node join and leave operations
   * - Tests joining new nodes
   * - Tests graceful leaving
   * - Ensures operations work after membership changes
   */
  private static void testJoinLeaveScenario(
      ActorSystem system,
      ActorRef manager,
      List<ActorRef> activeNodes,
      List<ActorRef> clients,
      List<ActorRef> crashedNodes,
      Map<ActorRef, Integer> nodeIdMap,
      int N, int R, int W) throws InterruptedException {

    logger.log("Current system status before join/leave:");
    manager.tell(new MarsSystemManager.GetSystemStatus(), ActorRef.noSender());
    Thread.sleep(1000);

    logger.log("Adding new node (JOIN)...");
    handleNodeAction(system, manager, activeNodes, crashedNodes, nodeIdMap, clients, "join", null, N, R, W);
    Thread.sleep(3000); // Wait for join to complete

    logger.log("Testing operations after JOIN...");
    if (!clients.isEmpty()) {
      clients.get(0).tell(new Client.Update(50, "AfterJoin"), ActorRef.noSender());
      Thread.sleep(2000);
    }

    logger.log("Removing a node (LEAVE)...");
    handleNodeAction(system, manager, activeNodes, crashedNodes, nodeIdMap, clients, "leave", null, N, R, W);
    Thread.sleep(3000); // Wait for leave to complete

    logger.log("Testing operations after LEAVE...");
    if (clients.isEmpty()) {
      logger.logError("No clients available to test after LEAVE.");
      return;
    }
    clients.get(0).tell(new Client.Get(50), ActorRef.noSender());
    Thread.sleep(1000);
    clients.get(0).tell(new Client.Update(60, "AfterLeave"), ActorRef.noSender());
    Thread.sleep(2000);

    logger.log("Join/Leave test completed.\n");
  }

  /**
   * Test 3: Crash and recovery with ongoing operations
   * - Crashes a node
   * - Continues operations while node is crashed
   * - Recovers the node
   * - Verifies data consistency
   */
  private static void testCrashRecoveryScenario(
      ActorSystem system,
      ActorRef manager,
      List<ActorRef> activeNodes,
      List<ActorRef> crashedNodes,
      Map<ActorRef, Integer> nodeIdMap,
      int N, int R, int W,
      List<ActorRef> clients) throws InterruptedException {
    logger.log("Storing initial data...");
    clients.get(0).tell(new Client.Update(1000, "InitialData"), ActorRef.noSender());
    Thread.sleep(2000);

    logger.log("Crashing a node...");
    handleNodeAction(system, manager, activeNodes, crashedNodes, nodeIdMap, clients, "crash", null, N, R, W);
    Thread.sleep(1000);

    logger.log("Current system status after crash:");
    manager.tell(new MarsSystemManager.GetSystemStatus(), ActorRef.noSender());
    Thread.sleep(1000);

    logger.log("Continuing operations while node is crashed...");
    clients.get(1).tell(new Client.Update(1001, "DuringCrash1"), ActorRef.noSender());
    Thread.sleep(500);
    clients.get(2).tell(new Client.Update(1002, "DuringCrash2"), ActorRef.noSender());
    Thread.sleep(500);
    clients.get(0).tell(new Client.Get(1000), ActorRef.noSender());
    Thread.sleep(2000);

    logger.log("Recovering the crashed node...");
    handleNodeAction(system, manager, activeNodes, crashedNodes, nodeIdMap, clients, "recover", null, N, R, W);
    Thread.sleep(3000); // Wait for recovery to complete

    logger.log("Testing data consistency after recovery...");
    clients.get(0).tell(new Client.Get(1000), ActorRef.noSender());
    Thread.sleep(500);
    clients.get(1).tell(new Client.Get(1001), ActorRef.noSender());
    Thread.sleep(500);
    clients.get(2).tell(new Client.Get(1002), ActorRef.noSender());
    Thread.sleep(2000);

    logger.log("Adding more data after recovery...");
    clients.get(0).tell(new Client.Update(1003, "AfterRecovery"), ActorRef.noSender());
    Thread.sleep(2000);

    logger.log("Crash/Recovery test completed.\n");
  }

  /**
   * Test 4: Mixed operations stress test
   * - Rapid sequence of various operations
   * - Tests system under load
   * - Includes membership changes during operations
   */
  private static void testMixedOperationsScenario(
      ActorSystem system,
      ActorRef manager,
      List<ActorRef> activeNodes,
      List<ActorRef> crashedNodes,
      Map<ActorRef, Integer> nodeIdMap,
      int N, int R, int W,
      List<ActorRef> clients) throws InterruptedException {
    logger.log("Starting mixed operations stress test...");

    // Phase 1: Rapid data operations
    logger.log("Phase 1: Rapid data operations...");
    for (int i = 2000; i < 2010; i++) {
      clients.get(0).tell(new Client.Update(i, "StressData" + i), ActorRef.noSender());
      Thread.sleep(50); // Network propagation delay simulation
    }

    Thread.sleep(1000);

    // Phase 2: Mixed reads and writes
    logger.log("Phase 2: Mixed reads and writes...");
    for (int i = 2000; i < 2010; i++) {
      if (i % 2 == 0) {
        clients.get(0).tell(new Client.Get(i), ActorRef.noSender());
      } else {
        clients.get(0).tell(new Client.Update(i, "Updated" + i), ActorRef.noSender());
      }
      Thread.sleep(100); // Network propagation delay
    }

    Thread.sleep(2000);

    // Phase 3: Operations with membership changes
    logger.log("Phase 3: Operations during membership changes...");

    // Start some operations
    clients.get(0).tell(new Client.Update(3000, "BeforeJoin"), ActorRef.noSender());
    Thread.sleep(200);

    // Add a node while operations are potentially ongoing
    logger.log("Adding node during operations...");
    handleNodeAction(system, manager, activeNodes, crashedNodes, nodeIdMap, clients, "join", null, N, R, W);

    // Continue operations
    clients.get(0).tell(new Client.Update(3001, "DuringJoin"), ActorRef.noSender());
    Thread.sleep(200);
    clients.get(0).tell(new Client.Get(3000), ActorRef.noSender());

    Thread.sleep(3000); // Wait for join to complete

    // More operations after join
    clients.get(0).tell(new Client.Update(3002, "AfterJoinCompleted"), ActorRef.noSender());
    Thread.sleep(500);

    // Crash a node
    logger.log("Crashing node during operations...");
    handleNodeAction(system, manager, activeNodes, crashedNodes, nodeIdMap, clients, "crash", null, N, R, W);

    // Continue operations with crashed node
    clients.get(0).tell(new Client.Get(3001), ActorRef.noSender());
    Thread.sleep(200);
    clients.get(0).tell(new Client.Update(3003, "WithCrashedNode"), ActorRef.noSender());

    Thread.sleep(2000);

    logger.log("Mixed operations stress test completed.\n");
  }

  /**
   * Updates node lists and notifies clients for join/leave/crash/recover
   * 
   * @param node         Node affected
   * @param nodeId       ID of the node
   * @param activeNodes  list of active nodes
   * @param crashedNodes list of crashed nodes
   * @param nodeIdMap    map of ActorRef -> nodeId
   * @param clients      list of clients
   * @param action       "join", "leave", "crash", "recover"
   */
  private static void updateNodeStateAndClients(
      ActorRef node,
      Integer nodeId,
      List<ActorRef> activeNodes,
      List<ActorRef> crashedNodes,
      Map<ActorRef, Integer> nodeIdMap,
      List<ActorRef> clients,
      String action) {
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

  private static void handleNodeAction(
      ActorSystem system,
      ActorRef manager,
      List<ActorRef> activeNodes,
      List<ActorRef> crashedNodes,
      Map<ActorRef, Integer> nodeIdMap,
      List<ActorRef> clients,
      String action,
      Integer nodeId,
      int N, int R, int W) {

    Random random = new Random();
    ActorRef targetNode;
    int targetId;
    String act = action.toLowerCase();

    switch (act) {
      case "join":
        targetId = (nodeId != null) ? nodeId : nodeIdMap.values().stream().max(Integer::compare).orElse(0) + 10;
        if (nodeIdMap.containsValue(targetId)) {
          logger.logError("Node ID " + targetId + " already exists.");
          return;
        }
        targetNode = system.actorOf(Node.props(targetId, N, R, W, true, manager), "Node" + targetId);

        // Update local state
        updateNodeStateAndClients(targetNode, targetId, activeNodes, crashedNodes, nodeIdMap, clients,
            act);
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

        targetId = nodeIdMap.get(targetNode);
        targetNode.tell(new Node.NodeAction(act), ActorRef.noSender());
        updateNodeStateAndClients(targetNode, targetId, activeNodes, crashedNodes, nodeIdMap, clients,
            act);
        logger.log(action.toUpperCase() + " completed: Node " + targetId);
        break;

      default:
        logger.logError("Unknown action: " + action);
    }
  }

  /**
   * Interactive mode for manual testing
   */
  private static void runInteractiveMode(
      ActorSystem system,
      ActorRef manager,
      Scanner scanner,
      int N, int R, int W,
      List<ActorRef> activeNodes,
      List<ActorRef> crashedNodes,
      Map<ActorRef, Integer> nodeIdMap,
      List<ActorRef> clients) throws InterruptedException {
    logger.log("\n=== INTERACTIVE MODE ===");
    logger.log("Commands:");
    logger.log("  put <key> <value> - Store a key-value pair");
    logger.log("  get <key>         - Retrieve a value");
    logger.log("  join [nodeId]     - Add a new node");
    logger.log("  leave [nodeId]    - Remove a node");
    logger.log("  crash [nodeId]    - Crash a node");
    logger.log("  recover [nodeId]  - Recover a crashed node");
    logger.log("  client            - Create a new client");
    logger.log("  status            - Show system status");
    logger.log("  exit              - Exit interactive mode");
    logger.log("");

    while (true) {
      System.out.print("mars> ");
      String input = scanner.nextLine().trim();

      if (input.isEmpty())
        continue;

      String[] parts = input.split("\\s+");
      String command = parts[0].toLowerCase();

      try {
        switch (command) {
          case "put":
            if (parts.length >= 3) {
              int key = Integer.parseInt(parts[1]);
              String value = String.join(" ", java.util.Arrays.copyOfRange(parts, 2, parts.length));
              clients.get(0).tell(new Client.Update(key, value), ActorRef.noSender());
              logger.log("Put request sent: key=" + key + ", value=\"" + value + "\"");
            } else {
              logger.log("Usage: put <key> <value>");
            }
            break;

          case "get":
            if (parts.length >= 2) {
              int key = Integer.parseInt(parts[1]);
              clients.get(0).tell(new Client.Get(key), ActorRef.noSender());
              logger.log("Get request sent: key=" + key);
            } else {
              logger.log("Usage: get <key>");
            }
            break;

          case "join":
            Integer nodeId = (parts.length >= 2) ? Integer.parseInt(parts[1]) : null;
            handleNodeAction(system, manager, activeNodes, crashedNodes, nodeIdMap, clients, "join", nodeId, N, R, W);
            logger.log("Join request sent");
            break;

          case "leave":
            nodeId = (parts.length >= 2) ? Integer.parseInt(parts[1]) : null;
            handleNodeAction(system, manager, activeNodes, crashedNodes, nodeIdMap, clients, "leave", nodeId, N, R, W);
            logger.log("Leave request sent");
            break;

          case "crash":
            nodeId = (parts.length >= 2) ? Integer.parseInt(parts[1]) : null;
            handleNodeAction(system, manager, activeNodes, crashedNodes, nodeIdMap, clients, "crash", nodeId, N, R, W);
            logger.log("Crash request sent");
            break;

          case "recover":
            nodeId = (parts.length >= 2) ? Integer.parseInt(parts[1]) : null;
            handleNodeAction(system, manager, activeNodes, crashedNodes, nodeIdMap, clients, "recover", nodeId, N, R,
                W);
            logger.log("Recovery request sent");
            break;

          case "client":
            String clientName = "client" + clients.size();
            ActorRef client = system.actorOf(Client.props(new ArrayList<>(activeNodes), clientName), clientName);
            clients.add(client);
            logger.log("Client creation request sent");
            break;

          case "status":
            manager.tell(new MarsSystemManager.GetSystemStatus(), ActorRef.noSender());
            break;

          case "exit":
            logger.log("Exiting interactive mode...");
            return;

          default:
            logger.log("Unknown command: " + command);
        }

        Thread.sleep(500); // Give some time for the operation

      } catch (NumberFormatException e) {
        logger.log("Invalid number format");
      } catch (Exception e) {
        logger.log("Error: " + e.getMessage());
      }
    }
  }
}
