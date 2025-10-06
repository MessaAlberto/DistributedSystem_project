package it.unitn;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import akka.actor.ActorSystem;

public class Main {
  private static final Logger logger = new Logger("Main");

  public static void main(String[] args) {

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
    system.actorOf(TestManager.props(system, N, R, W, TIMEOUT_SECONDS, INITIAL_NODES), "testManager");
  }

  public static void inputContinue() {
    try {
      System.out.println(">>> Press ENTER to continue <<<");
      System.in.read();
    } catch (IOException ignored) {
    }
  }
}
