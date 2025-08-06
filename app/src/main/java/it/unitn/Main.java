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
  public static void main(String[] args) {

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

    System.out.println("Configuration loaded: N=" + N + ", R=" + R + ", W=" + W);

    ActorSystem system = ActorSystem.create("MarsSystem");
    List<ActorRef> nodes = new ArrayList<>();

    // Create N nodes
    for (int i = 0; i < N; i++) {
      ActorRef node = system.actorOf(Node.props(i, N, R, W), "node" + i);
      nodes.add(node);
    }

    // Send initial peer list to all nodes
    for (ActorRef node : nodes) {
      node.tell(new Node.InitPeers(nodes), ActorRef.noSender());
    }

    // Start client and pass quorum values for read/write quorums
    ActorRef client = system.actorOf(Props.create(Client.class, nodes), "client");
  }
}
