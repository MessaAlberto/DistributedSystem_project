package it.unitn;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;

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

    final int N = Integer.parseInt(config.getProperty("N", "5"));
    final int R = Integer.parseInt(config.getProperty("R", "3"));
    final int W = Integer.parseInt(config.getProperty("W", "2"));

    System.out.println("\nConfiguration loaded: N=" + N + ", R=" + R + ", W=" + W + "\n");

    ActorSystem system = ActorSystem.create("MarsSystem");
    List<ActorRef> nodes = new ArrayList<>();

    // Create N nodes
    for (int i = 0; i < N; i++) {
      ActorRef node = system.actorOf(Node.props(i, N, R, W), "node" + i);
      nodes.add(node);
    }

    // Create a map for InitPeers (ActorRef -> nodeId)
    java.util.Map<ActorRef, Integer> nodeIdMap = new java.util.HashMap<>();
    for (int i = 0; i < N; i++) {
      nodeIdMap.put(nodes.get(i), i);
    }

    // Send initial peer list to all nodes with nodeIdMap
    for (ActorRef node : nodes) {
      node.tell(new Node.InitPeers(nodes, nodeIdMap), ActorRef.noSender());
    }

    // Create client
    ActorRef client = system.actorOf(Props.create(Client.class, nodes), "client");

    inputContinue();

    System.out.println("\n[Client] Sending a PutRequest to store key=42 with value=\"TestWord\"\n");
    client.tell(new Client.Update(42, "TestWord"), ActorRef.noSender());

    inputContinue();

    System.out.println("\n[Client] Sending a GetRequest to retrieve key=42\n");
    client.tell(new Client.Get(42), ActorRef.noSender());

    inputContinue();

    system.terminate();
    system.getWhenTerminated().toCompletableFuture().join();

    System.out.println("\nSystem terminated, exiting main.\n");
  }

  public static void inputContinue() {
    try {
      System.out.println("Waiting 3 seconds...\n");
      Thread.sleep(3000); // 3 seconds pause
    } catch (InterruptedException ignored) {
    }
  }
}
