package it.unitn;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.Scanner;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;

public class Main {
  public static void main(String[] args) throws InterruptedException {

    Properties config = new Properties();
    try (InputStream input = Main.class.getClassLoader().getResourceAsStream("config.properties")) {
      if (input == null) {
        System.err.println("Failed to load config.properties, using default values");
      } else {
        config.load(input);
      }
    } catch (IOException e) {
      System.err.println("Error loading config.properties");
    }

    final int N = Integer.parseInt(config.getProperty("N", "5")); // fattore di replica
    final int R = Integer.parseInt(config.getProperty("R", "3"));
    final int W = Integer.parseInt(config.getProperty("W", "2"));

    System.out.println("=== Mars Distributed Storage System ===");
    System.out.println("Configuration: N=" + N + ", R=" + R + ", W=" + W);
    System.out.println("========================================\n");

    ActorSystem system = ActorSystem.create("MarsSystem");
    ActorRef manager = system.actorOf(MarsSystemManager.props(N, R, W), "marsManager");

    // Imposta il totale dei nodi a 10 (override configurabile)
    int initialNodes = Integer.parseInt(config.getProperty("initialNodes", "10"));
    if (initialNodes < 1) initialNodes = 10;
    System.out.println("Initializing system with " + initialNodes + " nodes...\n");
    manager.tell(new MarsSystemManager.InitializeSystem(initialNodes), ActorRef.noSender());
    
    Thread.sleep(2000); // Wait for initialization
    
    // Test Scenario 1: Multiple clients with concurrent requests
    System.out.println("\n=== TEST 1: Multiple Clients + Concurrent Requests ===");
    testMultipleClientsScenario(manager);
    
    // Test Scenario 2: Join/Leave operations
    System.out.println("\n=== TEST 2: Node Join/Leave Operations ===");
    testJoinLeaveScenario(manager);
    
    // Test Scenario 3: Crash/Recovery with ongoing operations
    System.out.println("\n=== TEST 3: Crash/Recovery + Concurrent Operations ===");
    testCrashRecoveryScenario(manager);
    
    // Test Scenario 4: Stress test with mixed operations
    System.out.println("\n=== TEST 4: Mixed Operations Stress Test ===");
    testMixedOperationsScenario(manager);

    // Final status
    System.out.println("\n=== FINAL SYSTEM STATUS ===");
    manager.tell(new MarsSystemManager.GetSystemStatus(), ActorRef.noSender());
    
    Thread.sleep(3000);
    
    // Interactive mode (optional)
    System.out.println("\n=== INTERACTIVE MODE ===");
    System.out.println("Press 'i' for interactive mode, or any other key to exit...");
    
    Scanner scanner = new Scanner(System.in);
    String input = scanner.nextLine();
    if ("i".equalsIgnoreCase(input.trim())) {
      runInteractiveMode(manager, scanner);
    }
    
    scanner.close();
    system.terminate();
    system.getWhenTerminated().toCompletableFuture().join();
    System.out.println("\nSystem terminated. Mars mission complete!\n");
  }

  /**
   * Test 1: Multiple clients serving concurrent requests (possibly same key)
   * - Creates multiple clients
   * - Sends concurrent Put/Get operations
   * - Tests same key access from different clients
   */
  private static void testMultipleClientsScenario(ActorRef manager) throws InterruptedException {
    System.out.println("Creating multiple clients...");
    
    // Create 3 clients
    for (int i = 0; i < 3; i++) {
      manager.tell(new MarsSystemManager.CreateClient(), ActorRef.noSender());
      Thread.sleep(500);
    }
    
    System.out.println("Testing concurrent requests on same key (42)...");
    
    // Concurrent operations on same key
    manager.tell(new MarsSystemManager.TestPut(42, "Value1_Client1"), ActorRef.noSender());
    Thread.sleep(100); // Small delay to simulate network propagation
    
    manager.tell(new MarsSystemManager.TestPut(42, "Value2_Client2"), ActorRef.noSender());
    Thread.sleep(100);
    
    manager.tell(new MarsSystemManager.TestGet(42), ActorRef.noSender());
    Thread.sleep(100);
    
    manager.tell(new MarsSystemManager.TestPut(42, "Value3_Client3"), ActorRef.noSender());
    Thread.sleep(100);
    
    manager.tell(new MarsSystemManager.TestGet(42), ActorRef.noSender());
    
    Thread.sleep(2000); // Wait for operations to complete
    
    System.out.println("Testing concurrent requests on different keys...");
    
    // Concurrent operations on different keys
    manager.tell(new MarsSystemManager.TestPut(10, "DataA"), ActorRef.noSender());
    manager.tell(new MarsSystemManager.TestPut(20, "DataB"), ActorRef.noSender());
    manager.tell(new MarsSystemManager.TestPut(30, "DataC"), ActorRef.noSender());
    
    Thread.sleep(1000);
    
    manager.tell(new MarsSystemManager.TestGet(10), ActorRef.noSender());
    manager.tell(new MarsSystemManager.TestGet(20), ActorRef.noSender());
    manager.tell(new MarsSystemManager.TestGet(30), ActorRef.noSender());
    
    Thread.sleep(3000);
    System.out.println("Multi-client test completed.\n");
  }

  /**
   * Test 2: Node join and leave operations
   * - Tests joining new nodes
   * - Tests graceful leaving
   * - Ensures operations work after membership changes
   */
  private static void testJoinLeaveScenario(ActorRef manager) throws InterruptedException {
    System.out.println("Current system status before join/leave:");
    manager.tell(new MarsSystemManager.GetSystemStatus(), ActorRef.noSender());
    Thread.sleep(1000);
    
    System.out.println("Adding new node (JOIN)...");
    manager.tell(new MarsSystemManager.AddNode(), ActorRef.noSender());
    Thread.sleep(3000); // Wait for join to complete
    
    System.out.println("Testing operations after JOIN...");
    manager.tell(new MarsSystemManager.TestPut(50, "AfterJoin"), ActorRef.noSender());
    Thread.sleep(1000);
    manager.tell(new MarsSystemManager.TestGet(50), ActorRef.noSender());
    Thread.sleep(2000);
    
    System.out.println("Removing a node (LEAVE)...");
    manager.tell(new MarsSystemManager.RemoveNode(), ActorRef.noSender());
    Thread.sleep(3000); // Wait for leave to complete
    
    System.out.println("Testing operations after LEAVE...");
    manager.tell(new MarsSystemManager.TestGet(50), ActorRef.noSender());
    Thread.sleep(1000);
    manager.tell(new MarsSystemManager.TestPut(60, "AfterLeave"), ActorRef.noSender());
    Thread.sleep(2000);
    
    System.out.println("Join/Leave test completed.\n");
  }

  /**
   * Test 3: Crash and recovery with ongoing operations
   * - Crashes a node
   * - Continues operations while node is crashed
   * - Recovers the node
   * - Verifies data consistency
   */
  private static void testCrashRecoveryScenario(ActorRef manager) throws InterruptedException {
    System.out.println("Storing initial data...");
    manager.tell(new MarsSystemManager.TestPut(1000, "ImportantData"), ActorRef.noSender());
    Thread.sleep(2000);
    
    System.out.println("Crashing a node...");
    manager.tell(new MarsSystemManager.CrashNode(), ActorRef.noSender());
    Thread.sleep(1000);
    
    System.out.println("Current system status after crash:");
    manager.tell(new MarsSystemManager.GetSystemStatus(), ActorRef.noSender());
    Thread.sleep(1000);
    
    System.out.println("Continuing operations while node is crashed...");
    manager.tell(new MarsSystemManager.TestPut(1001, "DuringCrash1"), ActorRef.noSender());
    Thread.sleep(500);
    manager.tell(new MarsSystemManager.TestPut(1002, "DuringCrash2"), ActorRef.noSender());
    Thread.sleep(500);
    manager.tell(new MarsSystemManager.TestGet(1000), ActorRef.noSender());
    Thread.sleep(2000);
    
    System.out.println("Recovering the crashed node...");
    manager.tell(new MarsSystemManager.RecoverNode(), ActorRef.noSender());
    Thread.sleep(3000); // Wait for recovery to complete
    
    System.out.println("Testing data consistency after recovery...");
    manager.tell(new MarsSystemManager.TestGet(1000), ActorRef.noSender());
    Thread.sleep(500);
    manager.tell(new MarsSystemManager.TestGet(1001), ActorRef.noSender());
    Thread.sleep(500);
    manager.tell(new MarsSystemManager.TestGet(1002), ActorRef.noSender());
    Thread.sleep(2000);
    
    System.out.println("Adding more data after recovery...");
    manager.tell(new MarsSystemManager.TestPut(1003, "AfterRecovery"), ActorRef.noSender());
    Thread.sleep(2000);
    
    System.out.println("Crash/Recovery test completed.\n");
  }

  /**
   * Test 4: Mixed operations stress test
   * - Rapid sequence of various operations
   * - Tests system under load
   * - Includes membership changes during operations
   */
  private static void testMixedOperationsScenario(ActorRef manager) throws InterruptedException {
    System.out.println("Starting mixed operations stress test...");
    
    // Phase 1: Rapid data operations
    System.out.println("Phase 1: Rapid data operations...");
    for (int i = 2000; i < 2010; i++) {
      manager.tell(new MarsSystemManager.TestPut(i, "StressData" + i), ActorRef.noSender());
      Thread.sleep(50); // Network propagation delay simulation
    }
    
    Thread.sleep(1000);
    
    // Phase 2: Mixed reads and writes
    System.out.println("Phase 2: Mixed reads and writes...");
    for (int i = 2000; i < 2010; i++) {
      if (i % 2 == 0) {
        manager.tell(new MarsSystemManager.TestGet(i), ActorRef.noSender());
      } else {
        manager.tell(new MarsSystemManager.TestPut(i, "Updated" + i), ActorRef.noSender());
      }
      Thread.sleep(100); // Network propagation delay
    }
    
    Thread.sleep(2000);
    
    // Phase 3: Operations with membership changes
    System.out.println("Phase 3: Operations during membership changes...");
    
    // Start some operations
    manager.tell(new MarsSystemManager.TestPut(3000, "BeforeMembershipChange"), ActorRef.noSender());
    Thread.sleep(200);
    
    // Add a node while operations are potentially ongoing
    System.out.println("Adding node during operations...");
    manager.tell(new MarsSystemManager.AddNode(), ActorRef.noSender());
    
    // Continue operations
    manager.tell(new MarsSystemManager.TestPut(3001, "DuringJoin"), ActorRef.noSender());
    Thread.sleep(200);
    manager.tell(new MarsSystemManager.TestGet(3000), ActorRef.noSender());
    
    Thread.sleep(3000); // Wait for join to complete
    
    // More operations after join
    manager.tell(new MarsSystemManager.TestPut(3002, "AfterJoinCompleted"), ActorRef.noSender());
    Thread.sleep(500);
    
    // Crash a node
    System.out.println("Crashing node during operations...");
    manager.tell(new MarsSystemManager.CrashNode(), ActorRef.noSender());
    
    // Continue operations with crashed node
    manager.tell(new MarsSystemManager.TestGet(3001), ActorRef.noSender());
    Thread.sleep(200);
    manager.tell(new MarsSystemManager.TestPut(3003, "WithCrashedNode"), ActorRef.noSender());
    
    Thread.sleep(2000);
    
    System.out.println("Mixed operations stress test completed.\n");
  }

  /**
   * Interactive mode for manual testing
   */
  private static void runInteractiveMode(ActorRef manager, Scanner scanner) throws InterruptedException {
    System.out.println("\n=== INTERACTIVE MODE ===");
    System.out.println("Commands:");
    System.out.println("  put <key> <value> - Store a key-value pair");
    System.out.println("  get <key>         - Retrieve a value");
    System.out.println("  join [nodeId]     - Add a new node");
    System.out.println("  leave [nodeId]    - Remove a node");
    System.out.println("  crash [nodeId]    - Crash a node");
    System.out.println("  recover [nodeId]  - Recover a crashed node");
    System.out.println("  client            - Create a new client");
    System.out.println("  status            - Show system status");
    System.out.println("  exit              - Exit interactive mode");
    System.out.println();

    while (true) {
      System.out.print("mars> ");
      String input = scanner.nextLine().trim();
      
      if (input.isEmpty()) continue;
      
      String[] parts = input.split("\\s+");
      String command = parts[0].toLowerCase();
      
      try {
        switch (command) {
          case "put":
            if (parts.length >= 3) {
              int key = Integer.parseInt(parts[1]);
              String value = String.join(" ", java.util.Arrays.copyOfRange(parts, 2, parts.length));
              manager.tell(new MarsSystemManager.TestPut(key, value), ActorRef.noSender());
              System.out.println("Put request sent: key=" + key + ", value=\"" + value + "\"");
            } else {
              System.out.println("Usage: put <key> <value>");
            }
            break;
            
          case "get":
            if (parts.length >= 2) {
              int key = Integer.parseInt(parts[1]);
              manager.tell(new MarsSystemManager.TestGet(key), ActorRef.noSender());
              System.out.println("Get request sent: key=" + key);
            } else {
              System.out.println("Usage: get <key>");
            }
            break;
            
          case "join":
            if (parts.length >= 2) {
              int nodeId = Integer.parseInt(parts[1]);
              manager.tell(new MarsSystemManager.AddNode(nodeId), ActorRef.noSender());
            } else {
              manager.tell(new MarsSystemManager.AddNode(), ActorRef.noSender());
            }
            System.out.println("Join request sent");
            break;
            
          case "leave":
            if (parts.length >= 2) {
              int nodeId = Integer.parseInt(parts[1]);
              manager.tell(new MarsSystemManager.RemoveNode(nodeId), ActorRef.noSender());
            } else {
              manager.tell(new MarsSystemManager.RemoveNode(), ActorRef.noSender());
            }
            System.out.println("Leave request sent");
            break;
            
          case "crash":
            if (parts.length >= 2) {
              int nodeId = Integer.parseInt(parts[1]);
              manager.tell(new MarsSystemManager.CrashNode(nodeId), ActorRef.noSender());
            } else {
              manager.tell(new MarsSystemManager.CrashNode(), ActorRef.noSender());
            }
            System.out.println("Crash request sent");
            break;
            
          case "recover":
            if (parts.length >= 2) {
              int nodeId = Integer.parseInt(parts[1]);
              manager.tell(new MarsSystemManager.RecoverNode(nodeId), ActorRef.noSender());
            } else {
              manager.tell(new MarsSystemManager.RecoverNode(), ActorRef.noSender());
            }
            System.out.println("Recovery request sent");
            break;
            
          case "client":
            manager.tell(new MarsSystemManager.CreateClient(), ActorRef.noSender());
            System.out.println("Client creation request sent");
            break;
            
          case "status":
            manager.tell(new MarsSystemManager.GetSystemStatus(), ActorRef.noSender());
            break;
            
          case "exit":
            System.out.println("Exiting interactive mode...");
            return;
            
          default:
            System.out.println("Unknown command: " + command);
        }
        
        Thread.sleep(500); // Give some time for the operation
        
      } catch (NumberFormatException e) {
        System.out.println("Invalid number format");
      } catch (Exception e) {
        System.out.println("Error: " + e.getMessage());
      }
    }
  }
}
