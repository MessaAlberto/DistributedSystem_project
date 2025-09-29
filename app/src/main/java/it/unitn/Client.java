package it.unitn;

import java.io.Serializable;
import java.util.List;
import java.util.Random;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;

public class Client extends AbstractActor {

  private final Logger logger;
  private final List<ActorRef> nodes;
  private final Random random = new Random();

  public Client(List<ActorRef> nodes, String name) {
    this.nodes = nodes;
    this.logger = new Logger(name);
  }

  public static Props props(List<ActorRef> nodes, String name) {
    return Props.create(Client.class, () -> new Client(nodes, name));
  }

  private static String shortName(ActorRef nodeRef) {
    if (nodeRef == null)
      return "null";
    return nodeRef.path().name(); // just the name, e.g., "Node10"
  }

  // Message to trigger a write/update operation
  public static class Update {
    public final int key;
    public final String value;

    public Update(int key, String value) {
      this.key = key;
      this.value = value;
    }
  }

  // Message to trigger a read/get operation
  public static class Get {
    public final int key;

    public Get(int key) {
      this.key = key;
    }
  }

  // Message to update the list of nodes
  public static class UpdateNodeList implements Serializable {
    public final List<ActorRef> newNodes;

    public UpdateNodeList(List<ActorRef> newNodes) {
      this.newNodes = newNodes;
    }
  }

  private void onUpdate(Update msg) {
    if (nodes.isEmpty()) {
      logger.logError("No available nodes to handle UpdateRequest.");
      return;
    }
    ActorRef target = nodes.get(random.nextInt(nodes.size()));
    logger.log("Sending UpdateRequest key=" + msg.key + ", value=\"" + msg.value + "\" to " + shortName(target));
    target.tell(new Node.UpdateRequest(msg.key, msg.value), getSelf());
  }

  private void onUpdateAck(Node.UpdateAck msg) {
    logger.log("Received UpdateAck for key=" + msg.key + ", version=" + msg.version + " from "
        + shortName(msg.responder));
  }

  private void onGet(Get msg) {
    if (nodes.isEmpty()) {
      logger.logError("No available nodes to handle GetRequest.");
      return;
    }
    ActorRef target = nodes.get(random.nextInt(nodes.size()));
    logger.log("Sending GetRequest key=" + msg.key + " to " + shortName(target));
    target.tell(new Node.GetRequest(msg.key), getSelf());
  }

  private void onGetVersionResponse(Node.GetVersionResponse msg) {
    if (msg.version == 0 || msg.value == null) {
      logger.logError("Key " + msg.key + " not found.");
    } else {
      logger.log("Received value for key=" + msg.key + ": \"" + msg.value + "\" with version="
          + msg.version + " from " + shortName(msg.responder));
    }
  }

  private void onUpdateNodeList(UpdateNodeList msg) {
    // Aggiorna in place per mantenere il riferimento finale alla lista
    synchronized (nodes) {
      nodes.clear();
      nodes.addAll(msg.newNodes);
    }
    logger.log("Updated node list. Active targets: " + nodes.size());
  }

  private void onOperationFailed(Node.OperationFailed msg) {
    logger.logError("Operation failed for key=" + msg.key + ": " + msg.reason);
  }

  @Override
  public Receive createReceive() {
    return receiveBuilder()
        .match(Update.class, this::onUpdate)
        .match(Node.UpdateAck.class, this::onUpdateAck)
        .match(Get.class, this::onGet)
        .match(Node.GetVersionResponse.class, this::onGetVersionResponse)
        .match(UpdateNodeList.class, this::onUpdateNodeList)
        .match(Node.OperationFailed.class, this::onOperationFailed)
        .build();
  }
}
