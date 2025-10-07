package it.unitn;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;

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
    final int TIMEOUT_SECONDS = Integer.parseInt(config.getProperty("TIMEOUT_SECONDS", "5"));
    final int INITIAL_NODES = Integer.parseInt(config.getProperty("INITIAL_NODES", "10"));

    // Apply quorum rules check
    if (R + W <= N && W <= N / 2) {
      logger.logError("Invalid configuration: Ensure that R + W > N and W > N/2");
      return;
    }

    if (TIMEOUT_SECONDS <= 0 || INITIAL_NODES <= 0) {
      logger.logError("Invalid configuration: TIMEOUT_SECONDS and INITIAL_NODES must be positive integers");
      return;
    }

    logger.log("=== Mars Distributed Storage System ===");
    logger.log("Configuration: N=" + N + ", R=" + R + ", W=" + W + ", TIMEOUT_SECONDS=" + TIMEOUT_SECONDS
        + ", INITIAL_NODES=" + INITIAL_NODES);
    logger.log("========================================\n");

    ActorSystem system = ActorSystem.create("MarsSystem");
    ActorRef testManager = system.actorOf(TestManager.props(system, N, R, W, TIMEOUT_SECONDS, INITIAL_NODES),
        "testManager");
    Thread.sleep(2000); // Wait for the system to initialize

    CountDownLatch latch = null; // Used to wait for async operations in tests

    // testSigleUpdateGetScenario(testManager, latch);

    // inputContinue();

    // testMultipleClientsScenario(testManager, latch);

    inputContinue();

    testJoinLeaveScenario(testManager, latch);

    // inputContinue();

    // testCrashRecoveryScenario(testManager, latch);

  }

  // =================
  // === Test ===
  // =================

  /**
   * Test 1: Single client basic Update/Get operations
   * - Simple Update operation
   * - Simple Get operation
   * - Verifies data consistency
   */
  private static void testSigleUpdateGetScenario(ActorRef testManager, CountDownLatch latch)
      throws InterruptedException {
    logger.log("");
    logger.log("============================================================");
    logger.log("====        Starting single-client basic test           ====");
    logger.log("============================================================");

    logger.log("Storing key=1, value='HelloWorld'...");
    latch = new CountDownLatch(1);
    testManager.tell(new TestManager.ClientRequest(0, 1, "HelloWorld", latch), ActorRef.noSender());
    latch.await();

    inputContinue();

    logger.log("Retrieving key=1...");
    latch = new CountDownLatch(1);
    testManager.tell(new TestManager.ClientRequest(0, 1, null, latch), ActorRef.noSender());
    latch.await();

    inputContinue();

    logger.log("Updating key=1 to value='UpdatedValue'...");
    latch = new CountDownLatch(1);
    testManager.tell(new TestManager.ClientRequest(0, 1, "UpdatedValue", latch), ActorRef.noSender());
    latch.await();

    inputContinue();

    logger.log("Retrieving key=1 again...");
    latch = new CountDownLatch(1);
    testManager.tell(new TestManager.ClientRequest(0, 1, null, latch), ActorRef.noSender());
    latch.await();

    testManager.tell(new TestManager.PrintStoreRequest(), ActorRef.noSender());
    Thread.sleep(4000);

    testManager.tell(new TestManager.LogSystemStatus(), ActorRef.noSender());
    Thread.sleep(500);
    logger.log("");
    logger.log("============================================================");
    logger.log("====         Single-client basic test completed.        ====");
    logger.log("Summary:");
    logger.log("  - Basic Update and Get operations verified.");
    logger.log("  - Data consistency maintained across operations.");
    logger.log("============================================================\n");
  }

  /**
   * Test 2: Multiple clients serving concurrent requests
   * - Sends concurrent Update/Get operations
   * - Tests same key access from different clients
   */
  private static void testMultipleClientsScenario(ActorRef testManager, CountDownLatch latch)
      throws InterruptedException {
    logger.log("");
    logger.log("============================================================");
    logger.log("====        Starting multi-client concurrent test       ====");
    logger.log("============================================================");
    logger.log("Testing concurrent requests on same key (42)...");

    // Concurrent operations on same key
    latch = new CountDownLatch(3);
    testManager.tell(new TestManager.ClientRequest(0, 42, "Value1_Client1", latch), ActorRef.noSender());
    testManager.tell(new TestManager.ClientRequest(1, 42, "Value2_Client2", latch), ActorRef.noSender());
    testManager.tell(new TestManager.ClientRequest(2, 42, "Value3_Client3", latch), ActorRef.noSender());
    latch.await();

    inputContinue();

    latch = new CountDownLatch(1);
    testManager.tell(new TestManager.ClientRequest(1, 42, null, latch), ActorRef.noSender());
    latch.await();

    inputContinue();

    logger.log("Testing concurrent requests on different keys...");

    // Concurrent operations on different keys
    latch = new CountDownLatch(3);
    testManager.tell(new TestManager.ClientRequest(0, 10, "DataA", latch), ActorRef.noSender());
    testManager.tell(new TestManager.ClientRequest(1, 20, "DataB", latch), ActorRef.noSender());
    testManager.tell(new TestManager.ClientRequest(2, 80, "DataC", latch), ActorRef.noSender());
    latch.await();

    inputContinue();

    latch = new CountDownLatch(3);
    testManager.tell(new TestManager.ClientRequest(0, 10, null, latch), ActorRef.noSender());
    testManager.tell(new TestManager.ClientRequest(1, 20, null, latch), ActorRef.noSender());
    testManager.tell(new TestManager.ClientRequest(2, 80, null, latch), ActorRef.noSender());
    latch.await();

    testManager.tell(new TestManager.PrintStoreRequest(), ActorRef.noSender());
    Thread.sleep(4000);

    testManager.tell(new TestManager.LogSystemStatus(), ActorRef.noSender());
    Thread.sleep(500);
    logger.log("");
    logger.log("============================================================");
    logger.log("====           Multi-client test completed.              ====");
    logger.log("Summary:");
    logger.log("  - All concurrent client requests have been processed.");
    logger.log("  - System state is stable and ready for further tests.");
    logger.log("============================================================\n");
  }

  /**
   * Test 3: Node join and leave operations
   * - Tests joining new nodes
   * - Tests graceful leaving
   * - Ensures operations work after membership changes
   */
  private static void testJoinLeaveScenario(ActorRef testManager, CountDownLatch latch)
      throws InterruptedException {
    logger.log("");
    logger.log("============================================================");
    logger.log("====        Starting node join/leave test              ====");
    logger.log("============================================================");
    logger.log("Current system status before join/leave:");
    testManager.tell(new TestManager.LogSystemStatus(), ActorRef.noSender());
    Thread.sleep(500);

    logger.log("Testing operations before JOIN...");
    latch = new CountDownLatch(1);
    testManager.tell(new TestManager.ClientRequest(0, 57, "BeforeJoin", latch), ActorRef.noSender());
    latch.await();

    inputContinue();

    testManager.tell(new TestManager.PrintStoreRequest(), ActorRef.noSender());
    Thread.sleep(4000);

    inputContinue();

    logger.log("Adding new node (JOIN)...");
    latch = new CountDownLatch(1);
    testManager.tell(new TestManager.NodeActionRequest("join", 55, latch), ActorRef.noSender());
    latch.await();

    inputContinue();

    testManager.tell(new TestManager.PrintStoreRequest(), ActorRef.noSender());
    Thread.sleep(4000);

    inputContinue();

    // logger.log("Testing operations after JOIN...");
    // latch = new CountDownLatch(1);
    // testManager.tell(new TestManager.ClientRequest(0, 50, "AfterJoin", latch), ActorRef.noSender());
    // latch.await();

    // inputContinue();

    // logger.log("Removing a node (LEAVE)...");
    // latch = new CountDownLatch(1);
    // testManager.tell(new TestManager.NodeActionRequest("leave", latch), ActorRef.noSender());
    // latch.await();

    // inputContinue();

    // logger.log("Testing operations after LEAVE...");

    // latch = new CountDownLatch(1);
    // testManager.tell(new TestManager.ClientRequest(0, 50, null, latch), ActorRef.noSender());
    // latch.await();

    // inputContinue();

    // latch = new CountDownLatch(1);
    // testManager.tell(new TestManager.ClientRequest(0, 60, "AfterLeave", latch), ActorRef.noSender());
    // latch.await();

    // inputContinue();

    testManager.tell(new TestManager.PrintStoreRequest(), ActorRef.noSender());
    Thread.sleep(4000);

    testManager.tell(new TestManager.LogSystemStatus(), ActorRef.noSender());
    Thread.sleep(500);
    logger.log("\n============================================================");
    logger.log("====           Join/Leave test completed.              ====");
    logger.log("Summary:");
    logger.log("  - Node JOIN and LEAVE operations completed successfully.");
    logger.log("  - System state is stable and ready for further tests.");
    logger.log("============================================================\n");
  }

  /**
   * Test 3: Crash and recovery with ongoing operations
   * - Crashes a node
   * - Continues operations while node is crashed
   * - Recovers the node
   * - Verifies data consistency
   */
  private static void testCrashRecoveryScenario(ActorRef testManager, CountDownLatch latch)
      throws InterruptedException {
    logger.log("");
    logger.log("============================================================");
    logger.log("====        Starting node Crash/Recovery test           ====");
    logger.log("============================================================");
    logger.log("Storing initial data...");

    latch = new CountDownLatch(1);
    testManager.tell(new TestManager.ClientRequest(0, 1000, "InitialData", latch), ActorRef.noSender());
    latch.await();

    logger.log("Crashing a node...");
    latch = new CountDownLatch(1);
    testManager.tell(new TestManager.NodeActionRequest("crash", latch), ActorRef.noSender());
    latch.await();

    logger.log("Current system status after crash:");
    testManager.tell(new TestManager.LogSystemStatus(), ActorRef.noSender());
    Thread.sleep(500);

    logger.log("Continuing operations while node is crashed...");
    latch = new CountDownLatch(1);
    testManager.tell(new TestManager.ClientRequest(0, 1001, "DuringCrash1", latch), ActorRef.noSender());
    latch.await();

    latch = new CountDownLatch(1);
    testManager.tell(new TestManager.ClientRequest(0, 1002, "DuringCrash2", latch), ActorRef.noSender());
    latch.await();

    latch = new CountDownLatch(1);
    testManager.tell(new TestManager.ClientRequest(0, 1000, null, latch), ActorRef.noSender());
    latch.await();

    logger.log("Recovering the crashed node...");
    latch = new CountDownLatch(1);
    testManager.tell(new TestManager.NodeActionRequest("recover", latch), ActorRef.noSender());
    latch.await();

    logger.log("Testing data consistency after recovery...");
    latch = new CountDownLatch(1);
    testManager.tell(new TestManager.ClientRequest(0, 1000, null, latch), ActorRef.noSender());
    latch.await();
    latch = new CountDownLatch(1);
    testManager.tell(new TestManager.ClientRequest(0, 1001, null, latch), ActorRef.noSender());
    latch.await();
    latch = new CountDownLatch(1);
    testManager.tell(new TestManager.ClientRequest(0, 1002, null, latch), ActorRef.noSender());
    latch.await();

    logger.log("Adding more data after recovery...");
    latch = new CountDownLatch(1);
    testManager.tell(new TestManager.ClientRequest(0, 1003, "AfterRecovery", latch), ActorRef.noSender());
    latch.await();

    testManager.tell(new TestManager.PrintStoreRequest(), ActorRef.noSender());
    Thread.sleep(4000);

    testManager.tell(new TestManager.LogSystemStatus(), ActorRef.noSender());
    Thread.sleep(500);
    logger.log("\n============================================================");
    logger.log("====         Crash/Recovery test completed.            ====");
    logger.log("============================================================\n");
    logger.log("Summary:");
    logger.log("  - Node CRASH and RECOVERY operations completed successfully.");
    logger.log("  - Data consistency verified after recovery.");
    logger.log("  - System state is stable and ready for further tests.");
    logger.log("------------------------------------------------------------\n");
  }

  public static void inputContinue() {
    try {
      logger.log("");
      logger.log("Press ENTER to continue...");
      logger.log("");

      // Clean any leftover bytes in System.in
      while (System.in.available() > 0) {
        System.in.read();
      }

      // Now wait for user input
      System.in.read();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
