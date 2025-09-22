package it.unitn;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;

public class Main {
  private static final Random random = new Random();
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

    System.out.println("\nConfiguration loaded: N=" + N + ", R=" + R + ", W=" + W + "\n");

    ActorSystem system = ActorSystem.create("MarsSystem");
    List<ActorRef> nodes = new ArrayList<>();

    // Crea inizialmente 10 nodi (esempio) mantenendo N come fattore di replica
    int initialNodes = 10;
    for (int i = 0; i < initialNodes; i++) {
      ActorRef node = system.actorOf(Node.props(i*10, N, R, W), "node" + i);
      nodes.add(node);
    }

    // Mappa (ActorRef -> nodeId)
    java.util.Map<ActorRef, Integer> nodeIdMap = new java.util.HashMap<>();
    for (int i = 0; i < initialNodes; i++) {
      nodeIdMap.put(nodes.get(i), i*10);
    }

    // Inizializza peer list
    for (ActorRef node : nodes) {
      node.tell(new Node.InitPeers(nodes, nodeIdMap), ActorRef.noSender());
    }

    // Client
    ActorRef client = system.actorOf(Props.create(Client.class, nodes), "client");

    inputContinue();

    System.out.println("\n[Client] Sending a PutRequest to store key=42 with value=\"TestWord\"\n");
    client.tell(new Client.Update(9, "TestWord"), ActorRef.noSender());

    inputContinue();

    System.out.println("\n[Client] Sending a GetRequest to retrieve key=42\n");
    client.tell(new Client.Get(42), ActorRef.noSender());

    inputContinue();

    // ESEMPIO: JOIN di un nuovo nodo con id=25
    System.out.println("\n[Main] Spawning new node with id=25 and performing JOIN\n");
    ActorRef newNode = system.actorOf(Node.props(25, N, R, W), "nodeJoin");
    // Avvia la procedura di JOIN usando nodes.get(0) come bootstrap
    newNode.tell(new Node.JoinNetwork(nodes.get(0)), ActorRef.noSender());

    inputContinue();

    // ESEMPIO: LEAVE del nodo con id=0 (node0)
    System.out.println("\n[Main] Requesting LEAVE for node0 (id=0)\n");
    nodes.get(random.nextInt(nodes.size())).tell(new Node.LeaveNetwork(), ActorRef.noSender());

    inputContinue();

    // ESEMPIO: RECOVERY del nodo che "rinasce" con lo stesso id=0
    System.out.println("\n[Main] Spawning recovered node with id=0 and performing RECOVERY\n");
    ActorRef recoveredNode = system.actorOf(Node.props(0, N, R, W), "node0_recovered");
    recoveredNode.tell(new Node.RecoverNetwork(newNode /* usa un nodo vivo come bootstrap */), ActorRef.noSender());

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
