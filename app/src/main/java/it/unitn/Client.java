// File: Client.java

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

  // Message to trigger an update from outside
  public static class Update {
    public final int key;
    public final String value;

    public Update(int key, String value) {
      this.key = key;
      this.value = value;
    }
  }

  // Message to trigger a get from outside
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
        .build();
  }

  // Handle Update message
  private void onUpdate(Update msg) {
    ActorRef target = nodes.get(random.nextInt(nodes.size()));
    target.tell(new Node.PutRequest(msg.key, msg.value), getSelf());
  }

  // Handle Get message
  private void onGet(Get msg) {
    ActorRef target = nodes.get(random.nextInt(nodes.size()));
    target.tell(new Node.GetRequest(msg.key), getSelf());
  }
}
