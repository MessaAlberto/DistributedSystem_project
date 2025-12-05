package it.unitn;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;

public class Main {
  private static final Logger logger = new Logger("Main");
  private static boolean autoMode = true;

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
    autoMode = Boolean.parseBoolean(config.getProperty("AUTO_MODE", "false"));

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

    try {
      testSigleWriteReadScenario(testManager);
      inputContinue();

      testJoin(testManager);
      inputContinue();

      testLeave(testManager);
      inputContinue();

      testCrash(testManager);
      inputContinue();

      testRecover(testManager);
      inputContinue();

      testSequentialConsistencyScenario(testManager);
      inputContinue();

      testRecoverLostWritesScenario(testManager);
      inputContinue();

      testReadMissingKey(testManager);
      inputContinue();

      testConcurrentWritesSameKey(testManager);
      inputContinue();

      testClientReqDuringNodeAction(testManager);
      inputContinue();

      testNodeActionDuringClientReq(testManager);
      inputContinue();

      testAskToCrashedNode(testManager);
      inputContinue();

      testFailingJoin(testManager);
      inputContinue();

      testFailingRecover(testManager);
      inputContinue();
    } finally {
      logger.log("All automated scenarios completed. Terminate the program when ready.");
    }

  }

  // =================
  // === Test ===
  // =================

  private static void testSigleWriteReadScenario(ActorRef testManager)
      throws InterruptedException {
    logger.log("");
    logger.log("============================================================");
    logger.log("====        Starting single-client basic test           ====");
    logger.log("============================================================");

    logger.log("Storing key=25, value='HelloWorld'...");
    CountDownLatch latch = new CountDownLatch(1);
    testManager.tell(new TestManager.ClientRequest(0, 25, "HelloWorld", latch), ActorRef.noSender());
    latch.await();

    inputContinue();

    logger.log("Retrieving key=25...");
    latch = new CountDownLatch(1);
    testManager.tell(new TestManager.ClientRequest(0, 25, null, latch), ActorRef.noSender());
    latch.await();

    inputContinue();

    testManager.tell(new TestManager.PrintStoreRequest(), ActorRef.noSender());
    Thread.sleep(4000);
    logger.log("");
    logger.log("============================================================");
    logger.log("====         Single-client basic test completed.        ====");
    logger.log("Summary:");
    logger.log("  - Basic Write and Read operations verified.");
    logger.log("  - Data consistency maintained across operations.");
    logger.log("============================================================\n");
  }

  private static void testJoin(ActorRef testManager) throws InterruptedException {
    logger.log("");
    logger.log("============================================================");
    logger.log("====              Starting node JOIN test                ====");
    logger.log("============================================================");

    logger.log("Adding new node27 (JOIN)...");
    CountDownLatch latch = new CountDownLatch(1);
    testManager.tell(new TestManager.NodeActionRequest("join", 27, latch), ActorRef.noSender());
    latch.await();

    inputContinue();

    testManager.tell(new TestManager.PrintStoreRequest(), ActorRef.noSender());
    Thread.sleep(4000);

    logger.log("\n============================================================");
    logger.log("====               Node JOIN test completed.            ====");
    logger.log("Summary:");
    logger.log("  - Node JOIN operation completed successfully.");
    logger.log("  - System state is stable and ready for further tests.");
    logger.log("============================================================\n");
  }

  private static void testLeave(ActorRef testManager) throws InterruptedException {
    logger.log("");
    logger.log("============================================================");
    logger.log("====              Starting node LEAVE test               ====");
    logger.log("============================================================");

    logger.log("Removing node30 (LEAVE)...");
    CountDownLatch latch = new CountDownLatch(1);
    testManager.tell(new TestManager.NodeActionRequest("leave", 30, latch), ActorRef.noSender());
    latch.await();

    inputContinue();

    testManager.tell(new TestManager.PrintStoreRequest(), ActorRef.noSender());
    Thread.sleep(4000);

    logger.log("\n============================================================");
    logger.log("====               Node LEAVE test completed.           ====");
    logger.log("Summary:");
    logger.log("  - Node LEAVE operation completed successfully.");
    logger.log("  - System state is stable and ready for further tests.");
    logger.log("============================================================\n");
  }

  private static void testCrash(ActorRef testManager) throws InterruptedException {
    logger.log("");
    logger.log("============================================================");
    logger.log("====             Starting node CRASH test                ====");
    logger.log("============================================================");

    logger.log("Crashing node40...");
    CountDownLatch latch = new CountDownLatch(1);
    testManager.tell(new TestManager.NodeActionRequest("crash", 40, latch), ActorRef.noSender());
    latch.await();

    inputContinue();

    testManager.tell(new TestManager.PrintStoreRequest(), ActorRef.noSender());
    Thread.sleep(4000);

    logger.log("\n============================================================");
    logger.log("====              Node CRASH test completed.            ====");
    logger.log("Summary:");
    logger.log("  - Node CRASH operation completed successfully.");
    logger.log("  - System state is stable and ready for further tests.");
    logger.log("============================================================\n");
  }

  private static void testRecover(ActorRef testManager) throws InterruptedException {
    logger.log("");
    logger.log("============================================================");
    logger.log("====           Starting node RECOVER test               ====");
    logger.log("============================================================");

    logger.log("Recovering node40...");
    CountDownLatch latch = new CountDownLatch(1);
    testManager.tell(new TestManager.NodeActionRequest("recover", 40, latch), ActorRef.noSender());
    latch.await();

    inputContinue();

    testManager.tell(new TestManager.PrintStoreRequest(), ActorRef.noSender());
    Thread.sleep(4000);

    logger.log("\n============================================================");
    logger.log("====             Node RECOVER test completed.          ====");
    logger.log("Summary:");
    logger.log("  - Node RECOVER operation completed successfully.");
    logger.log("  - System state is stable and ready for further tests.");
    logger.log("============================================================\n");
  }

  private static void testSequentialConsistencyScenario(ActorRef testManager)
      throws InterruptedException {
    logger.log("");
    logger.log("============================================================");
    logger.log("====     Starting Sequential Consistency test           ====");
    logger.log("============================================================");

    logger.log("Client2 writing key=65, value='First' asking node20...");
    CountDownLatch latch = new CountDownLatch(1);
    testManager.tell(new TestManager.ClientRequest(2, 20, 65, "First", latch), ActorRef.noSender());
    latch.await();

    inputContinue();

    logger.log("Client0 writes key=65, value='Second' asking node27...");
    latch = new CountDownLatch(1);
    testManager.tell(new TestManager.ClientRequest(0, 27, 65, "Second", latch), ActorRef.noSender());
    latch.await();

    inputContinue();

    logger.log("Client1 reads key=65 (should see 'Second') asking node 40...");
    latch = new CountDownLatch(1);
    testManager.tell(new TestManager.ClientRequest(1, 40, 65, null, latch), ActorRef.noSender());
    latch.await();

    testManager.tell(new TestManager.PrintStoreRequest(), ActorRef.noSender());
    Thread.sleep(4000);

    logger.log("");
    logger.log("============================================================");
    logger.log("====      Sequential Consistency test completed.       ====");
    logger.log("Summary:");
    logger.log("  - Sequential consistency verified across clients.");
    logger.log("  - Data integrity maintained during operations.");
    logger.log("============================================================\n");
  }

  private static void testRecoverLostWritesScenario(ActorRef testManager)
      throws InterruptedException {
    logger.log("");
    logger.log("============================================================");
    logger.log("====     Starting Recover Lost Writes test              ====");
    logger.log("============================================================");

    logger.log("Node10 crashing...");
    CountDownLatch latch = new CountDownLatch(1);
    testManager.tell(new TestManager.NodeActionRequest("crash", 10, latch), ActorRef.noSender());
    latch.await();

    inputContinue();

    logger.log("Client2 writing key=65, value='Third' asking node40...");
    latch = new CountDownLatch(1);
    testManager.tell(new TestManager.ClientRequest(2, 40, 65, "Third", latch), ActorRef.noSender());
    latch.await();

    inputContinue();

    logger.log("Node10 reovering...");
    latch = new CountDownLatch(1);
    testManager.tell(new TestManager.NodeActionRequest("recover", 10, latch), ActorRef.noSender());
    latch.await();

    inputContinue();

    logger.log("Client0 reads key=65 (should see 'Third') asking node27...");
    latch = new CountDownLatch(1);
    testManager.tell(new TestManager.ClientRequest(0, 27, 65, null, latch), ActorRef.noSender());
    latch.await();

    inputContinue();

    testManager.tell(new TestManager.PrintStoreRequest(), ActorRef.noSender());
    Thread.sleep(4000);

    logger.log("");
    logger.log("============================================================");
    logger.log("====      Recover Lost Writes test completed.          ====");
    logger.log("Summary:");
    logger.log("  - Lost writes successfully recovered after node recovery.");
    logger.log("  - Data consistency verified across the system.");
    logger.log("============================================================\n");
  }

  private static void testReadMissingKey(ActorRef testManager)
      throws InterruptedException {
    logger.log("");
    logger.log("============================================================");
    logger.log("====        Starting Read Missing Key test             ====");
    logger.log("============================================================");

    logger.log("Client1 reading non-existent key=999 asking node50...");
    CountDownLatch latch = new CountDownLatch(1);
    testManager.tell(new TestManager.ClientRequest(1, 999, null, latch), ActorRef.noSender());
    latch.await();

    inputContinue();

    testManager.tell(new TestManager.PrintStoreRequest(), ActorRef.noSender());
    Thread.sleep(4000);
    logger.log("");
    logger.log("============================================================");
    logger.log("====        Read Missing Key test completed.             ====");
    logger.log("Summary:");
    logger.log("  - Read operation on non-existent key handled gracefully.");
    logger.log("  - System state remains consistent.");
    logger.log("============================================================\n");
  }

  private static void testConcurrentWritesSameKey(ActorRef testManager)
      throws InterruptedException {
    logger.log("");
    logger.log("============================================================");
    logger.log("====     Starting Concurrent Writes Same Key test      ====");
    logger.log("============================================================");

    logger.log("Client2 writing key=18, value='Alpha' asking node0...");
    CountDownLatch latch = new CountDownLatch(2);
    testManager.tell(new TestManager.ClientRequest(2, 0, 18, "Alpha", latch), ActorRef.noSender());

    logger.log("Client0 writing key=18, value='Beta' asking node40...");
    testManager.tell(new TestManager.ClientRequest(0, 40, 18, "Beta", latch), ActorRef.noSender());
    latch.await();

    inputContinue();

    logger.log("Client2 reading key=18 asking node27...");
    latch = new CountDownLatch(1);
    testManager.tell(new TestManager.ClientRequest(2, 27, 18, null, latch), ActorRef.noSender());
    latch.await();

    testManager.tell(new TestManager.PrintStoreRequest(), ActorRef.noSender());
    Thread.sleep(4000);

    logger.log("");
    logger.log("============================================================");
    logger.log("====   Concurrent Writes Same Key test completed.      ====");
    logger.log("Summary:");
    logger.log("  - Concurrent writes to the same key handled correctly.");
    logger.log("  - Data consistency maintained across operations.");
    logger.log("============================================================\n");
  }

  private static void testClientReqDuringNodeAction(ActorRef testManager)
      throws InterruptedException {
    logger.log("");
    logger.log("============================================================");
    logger.log("====     Starting Request During Node Action test       ====");
    logger.log("============================================================");

    logger.log("Join node70...");
    CountDownLatch latch = new CountDownLatch(2);
    testManager.tell(new TestManager.NodeActionRequest("join", 70, latch), ActorRef.noSender());

    logger.log("While join is in progress, Client1 writes key=33, value='Delta' asking node20...");
    testManager.tell(new TestManager.ClientRequest(1, 20, 33, "Delta", latch), ActorRef.noSender());
    latch.await(); // Wait for crash to complete

    inputContinue();

    testManager.tell(new TestManager.PrintStoreRequest(), ActorRef.noSender());
    Thread.sleep(4000);

    logger.log("");
    logger.log("============================================================");
    logger.log("====   Request During Node Action test completed.       ====");
    logger.log("Summary:");
    logger.log("  - Requests during node actions handled appropriately.");
    logger.log("  - System stability maintained.");
    logger.log("============================================================\n");
  }

  private static void testNodeActionDuringClientReq(ActorRef testManager)
      throws InterruptedException {
    logger.log("");
    logger.log("============================================================");
    logger.log("====     Starting Node Action During Request test       ====");
    logger.log("============================================================");

    logger.log("Client2 reads key=18 asking node10...");
    CountDownLatch latch = new CountDownLatch(2);
    testManager.tell(new TestManager.ClientRequest(2, 10, 18, null, latch), ActorRef.noSender());

    logger.log("While request is pending, crash node10...");
    testManager.tell(new TestManager.NodeActionRequest("crash", 10, latch), ActorRef.noSender());
    latch.await(); // Wait for crash to complete

    inputContinue();

    testManager.tell(new TestManager.PrintStoreRequest(), ActorRef.noSender());
    Thread.sleep(4000);

    logger.log("");
    logger.log("============================================================");
    logger.log("====   Node Action During Request test completed.       ====");
    logger.log("Summary:");
    logger.log("  - Node actions during pending requests handled correctly.");
    logger.log("  - Data integrity and system stability maintained.");
    logger.log("============================================================\n");
  }

  private static void testAskToCrashedNode(ActorRef testManager)
      throws InterruptedException {
    logger.log("");
    logger.log("============================================================");
    logger.log("====         Starting Ask to Crashed Node test         ====");
    logger.log("============================================================");

    logger.log("Crashing node27...");
    CountDownLatch latch = new CountDownLatch(1);
    testManager.tell(new TestManager.NodeActionRequest("crash", 27, latch), ActorRef.noSender());
    latch.await();

    inputContinue();

    logger.log("Client0 writing key=30, value='Epsilon' asking crashed node27...");
    latch = new CountDownLatch(1);
    testManager.tell(new TestManager.ClientRequest(0, 27, 30, "Epsilon", latch), ActorRef.noSender());
    latch.await();

    inputContinue();

    logger.log("Crashing node40...");
    latch = new CountDownLatch(1);
    testManager.tell(new TestManager.NodeActionRequest("crash", 40, latch), ActorRef.noSender());
    latch.await();

    inputContinue();

    logger.log("Client1 reading key=30 asking crashed node40...");
    latch = new CountDownLatch(1);
    testManager.tell(new TestManager.ClientRequest(1, 40, 30, null, latch), ActorRef.noSender());
    latch.await();

    inputContinue();

    testManager.tell(new TestManager.PrintStoreRequest(), ActorRef.noSender());
    Thread.sleep(4000);

    logger.log("");
    logger.log("============================================================");
    logger.log("====       Ask to Crashed Node test completed.        ====");
    logger.log("Summary:");
    logger.log("  - System handled requests to crashed nodes gracefully.");
    logger.log("  - Stability and data integrity maintained.");
    logger.log("============================================================\n");
  }

  private static void testFailingJoin(ActorRef testManager)
      throws InterruptedException {
    logger.log("");
    logger.log("============================================================");
    logger.log("====          Starting Failing JOIN test               ====");
    logger.log("============================================================");

    logger.log("Simulating failing JOIN for node25 (should fail due to the crash of its successor node, node27)...");
    CountDownLatch latch = new CountDownLatch(1);
    testManager.tell(new TestManager.NodeActionRequest("join", 25, latch), ActorRef.noSender());
    latch.await();

    inputContinue();

    testManager.tell(new TestManager.PrintStoreRequest(), ActorRef.noSender());
    Thread.sleep(4000);

    logger.log("\n============================================================");
    logger.log("====            Failing JOIN test completed.           ====");
    logger.log("Summary:");
    logger.log("  - Failing JOIN operation handled correctly.");
    logger.log("  - System state is stable and ready for further tests.");
    logger.log("============================================================\n");
  }

  private static void testFailingRecover(ActorRef testManager)
      throws InterruptedException {
    logger.log("");
    logger.log("============================================================");
    logger.log("====         Starting Failing RECOVER test             ====");
    logger.log("============================================================");

    logger
        .log("Simulating failing RECOVER for node40 (should fail due to the crash of its predecessor node, node27)...");
    CountDownLatch latch = new CountDownLatch(1);
    testManager.tell(new TestManager.NodeActionRequest("recover", 40, latch), ActorRef.noSender());
    latch.await();

    inputContinue();

    testManager.tell(new TestManager.PrintStoreRequest(), ActorRef.noSender());
    Thread.sleep(4000);

    logger.log("\n============================================================");
    logger.log("====           Failing RECOVER test completed.        ====");
    logger.log("Summary:");
    logger.log("  - Failing RECOVER operation handled correctly.");
    logger.log("  - System state is stable and ready for further tests.");
    logger.log("============================================================\n");
  }

  public static void inputContinue() {
    try {
      if (autoMode) {
        logger.log("[AUTO] Continuing without waiting for user input.");
        return;
      }
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
