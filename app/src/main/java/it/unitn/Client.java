package it.unitn;

import java.util.List;
import java.util.Random;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;

public class Client extends AbstractActor {
  private final List<ActorRef> nodes;
  private final Random random = new Random();

  public Client(List<ActorRef> nodes) {
    this.nodes = nodes;
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

  @Override
  public Receive createReceive() {
    return receiveBuilder()
        .match(Update.class, this::onUpdate)
        .match(Get.class, this::onGet)
        .match(Node.PutAck.class, this::onPutAck)
        .match(Node.GetVersionResponse.class, this::onGetVersionResponse)
        .match(Node.OperationFailed.class, this::onOperationFailed)
        .build();
  }

  private void onUpdate(Update msg) {
    ActorRef target = nodes.get(random.nextInt(nodes.size()));
    System.out.println("[Client] Sending PutRequest key=" + msg.key + ", value=\"" + msg.value + "\" to " + Node.shortName(target));
    target.tell(new Node.PutRequest(msg.key, msg.value), getSelf());
  }

  private void onGet(Get msg) {
    ActorRef target = nodes.get(random.nextInt(nodes.size()));
    System.out.println("[Client] Sending GetRequest key=" + msg.key + " to " + Node.shortName(target));
    target.tell(new Node.GetRequest(msg.key), getSelf());
  }

  private void onPutAck(Node.PutAck msg) {
    System.out.println("[Client] Received PutAck for key=" + msg.key + ", version=" + msg.version + " from " + Node.shortName(msg.responder));
  }

  private void onGetVersionResponse(Node.GetVersionResponse msg) {
    if (msg.version == 0 || msg.value == null) {
      System.out.println("[Client] Key " + msg.key + " not found.");
    } else {
      System.out.println("[Client] Received value for key=" + msg.key + ": \"" + msg.value + "\" with version=" + msg.version + " from " + Node.shortName(msg.responder));
    }
  }

  private void onOperationFailed(Node.OperationFailed msg) {
    System.out.println("[Client] Operation failed for key=" + msg.key + ": " + msg.reason);
  }
}
