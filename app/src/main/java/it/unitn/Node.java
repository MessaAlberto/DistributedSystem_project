// File: Node.java

package it.unitn;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;

public class Node extends AbstractActor {
  private final int nodeId;
  private List<ActorRef> peers = new ArrayList<>();
  private final Map<Integer, String> store = new HashMap<>();

  public Node(int nodeId, int N, int R, int W) {
    this.nodeId = nodeId;
  }

  public static Props props(int nodeId, int N, int R, int W) {
    return Props.create(Node.class, () -> new Node(nodeId, N, R, W));
  }

  // Initialize peers
  public static class InitPeers implements Serializable {
    public final List<ActorRef> peers;

    public InitPeers(List<ActorRef> peers) {
      this.peers = peers;
    }
  }

  // Client update (write) request
  public static class PutRequest implements Serializable {
    public final int key;
    public final String value;

    public PutRequest(int key, String value) {
      this.key = key;
      this.value = value;
    }
  }

  // Client get (read) request
  public static class GetRequest implements Serializable {
    public final int key;

    public GetRequest(int key) {
      this.key = key;
    }
  }

  // Internal replica propagation
  public static class PutReplica implements Serializable {
    public final int key;
    public final String value;

    public PutReplica(int key, String value) {
      this.key = key;
      this.value = value;
    }
  }

  // UNUSED: used for future quorum reads
  /*
  public static class GetReplica implements Serializable {
    public final int key;

    public GetReplica(int key) {
      this.key = key;
    }
  }

  public static class GetReplicaResponse implements Serializable {
    public final int key;
    public final String value;

    public GetReplicaResponse(int key, String value) {
      this.key = key;
      this.value = value;
    }
  }

  public static class PutAck implements Serializable {
  }
  */

  @Override
  public Receive createReceive() {
    return receiveBuilder()
        .match(InitPeers.class, this::onInitPeers)
        .match(PutRequest.class, this::onPutRequest)
        .match(GetRequest.class, this::onGetRequest)
        .match(PutReplica.class, this::onPutReplica)
        .build();
  }

  // Store peer references
  private void onInitPeers(InitPeers msg) {
    this.peers = msg.peers;
  }

  // Handle update from client
  private void onPutRequest(PutRequest msg) {
    store.put(msg.key, msg.value);
    System.out.println("[Node " + nodeId + "] Update(" + msg.key + ", " + msg.value + ")");

    // Send replica to all peers (except self)
    for (ActorRef peer : peers) {
      if (!peer.equals(getSelf())) {
        peer.tell(new PutReplica(msg.key, msg.value), getSelf());
      }
    }
  }

  // Handle get from client
  private void onGetRequest(GetRequest msg) {
    String val = store.get(msg.key);
    if (val != null) {
      System.out.println("[Node " + nodeId + "] Get(" + msg.key + ") → " + val);
    } else {
      System.out.println("[Node " + nodeId + "] Get(" + msg.key + ") → NOT FOUND");
    }
  }

  // Handle replica message from another node
  private void onPutReplica(PutReplica msg) {
    store.put(msg.key, msg.value);
    System.out.println("[Node " + nodeId + "] PutReplica(" + msg.key + ", " + msg.value + ")");
  }
}
