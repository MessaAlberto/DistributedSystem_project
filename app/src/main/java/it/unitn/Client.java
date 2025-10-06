package it.unitn;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Cancellable;
import akka.actor.Props;
import scala.concurrent.duration.Duration;

public class Client extends AbstractActor {

  private final Logger logger;
  private final List<ActorRef> nodes;
  private final Random random = new Random();
  private final Map<String, PendingRequest> pendingRequests = new HashMap<>();
  private final int timeoutSeconds;

  private static class PendingRequest {
    final Object request;
    final Cancellable timeout;

    PendingRequest(Object request, Cancellable timeout) {
      this.request = request;
      this.timeout = timeout;
    }
  }

  private final ActorRef manager; // reference to the main actor (controller/test)

  public Client(List<ActorRef> nodes, String name, ActorRef manager, int timeoutSeconds) {
    this.nodes = nodes;
    this.logger = new Logger(name);
    this.manager = manager;
    this.timeoutSeconds = timeoutSeconds;
  }

  public static Props props(List<ActorRef> nodes, String name, ActorRef manager, int timeoutSeconds) {
    return Props.create(Client.class, () -> new Client(nodes, name, manager, timeoutSeconds));
  }

  private static String shortName(ActorRef nodeRef) {
    if (nodeRef == null)
      return "null";
    return nodeRef.path().name();
  }

  // === Messages ===

  public static class Update implements Serializable {
    public ActorRef node = null; // Optional specific node to target
    public final int key;
    public final String value;

    public Update(int key, String value) {
      this.key = key;
      this.value = value;
    }

    public Update(ActorRef node, int key, String value) {
      this.node = node;
      this.key = key;
      this.value = value;
    }
  }

  public static class Get implements Serializable {
    public ActorRef node = null; // Optional specific node to target
    public final int key;

    public Get(int key) {
      this.key = key;
    }

    public Get(ActorRef node, int key) {
      this.node = node;
      this.key = key;
    }
  }

  public static class UpdateResponse implements Serializable {
    public final int key;
    public final String value;
    public final int version;

    public UpdateResponse(int key, String value, int version) {
      this.key = key;
      this.value = value;
      this.version = version;
    }
  }

  public static class GetResponse implements Serializable {
    public final int key;
    public final String value;
    public final int version;

    public GetResponse(int key, String value, int version) {
      this.key = key;
      this.value = value;
      this.version = version;
    }
  }

  public static class UpdateNodeList implements Serializable {
    public final List<ActorRef> newNodes;

    public UpdateNodeList(List<ActorRef> newNodes) {
      this.newNodes = newNodes;
    }
  }

  // === Responses back to manager ===
  public static class ClientResponse implements Serializable {
    public final String id;
    public final boolean success;
    public final String message;

    public ClientResponse(String id, boolean success, String message) {
      this.id = id;
      this.success = success;
      this.message = message;
    }
  }

  // === Timeout message ===
  private static class OperationTimeout implements Serializable {
    final String requestId;

    OperationTimeout(String requestId) {
      this.requestId = requestId;
    }
  }

  // === Handlers ===

  private void onUpdate(Update msg) {
    String requestId = "UPDATE " + msg.key + " " + msg.value;
    if (nodes.isEmpty()) {
      sendResponse(requestId, false, "No available nodes to handle Update.");
      return;
    }
    if (pendingRequests.containsKey(requestId)) {
      logger.log("Request already in progress: " + requestId);
      return;
    }

    ActorRef target = msg.node != null ? msg.node : nodes.get(random.nextInt(nodes.size()));
    logger.log("Sending UpdateRequest key=" + msg.key + ", value=\"" + msg.value + "\" to " + shortName(target));
    target.tell(new Node.UpdateRequest(msg.key, msg.value), getSelf());

    startRequest(requestId, msg);
  }

  private void onUpdateResponse(UpdateResponse msg) {
    logger.log("Received UpdateResponse for key=" + msg.key + " value=\"" + msg.value + "\" version=" + msg.version);
    cancelRequest("UPDATE " + msg.key + " " + msg.value);
    sendResponse("UPDATE " + msg.key + " " + msg.value, true,
        "Update successful for key=" + msg.key + " version=" + msg.version);
  }

  private void onGet(Get msg) {
    String requestId = "GET " + msg.key;
    if (nodes.isEmpty()) {
      sendResponse(requestId, false, "No available nodes to handle Get.");
      return;
    }
    if (pendingRequests.containsKey(requestId)) {
      logger.log("Request already in progress: " + requestId);
      return;
    }

    ActorRef target = msg.node != null ? msg.node : nodes.get(random.nextInt(nodes.size()));
    logger.log("Sending GetRequest key=" + msg.key + " to " + shortName(target));
    target.tell(new Node.GetRequest(msg.key), getSelf());

    startRequest(requestId, msg);
  }

  private void onGetResponse(GetResponse msg) {
    logger.log("Received GetResponse for key=" + msg.key + " value=\"" + msg.value + "\" version=" + msg.version);
    cancelRequest("GET " + msg.key);
    sendResponse("GET " + msg.key, true, "Value=\"" + msg.value + "\" version=" + msg.version);
  }

  private void onUpdateNodeList(UpdateNodeList msg) {
    synchronized (nodes) {
      nodes.clear();
      nodes.addAll(msg.newNodes);
    }
    logger.log("Updated node list. Active targets: " + nodes.size());
  }

  private void onOperationFailed(Node.OperationFailed msg) {
    cancelRequest("UPDATE " + msg.key + " " + msg.value);
    logger.logError("Operation failed for key=" + msg.key + ": " + msg.reason);
    sendResponse("UPDATE " + msg.key + " " + msg.value, false, "Operation failed: " + msg.reason);
  }

  private void onTimeout(OperationTimeout msg) {
    PendingRequest pr = pendingRequests.remove(msg.requestId);
    if (pr != null) {
      logger.logError("Timeout reached for request: " + msg.requestId);
      sendResponse(msg.requestId, false, "Timeout waiting for response.");
    }
  }

  // === Helper methods ===

  private void startRequest(String requestId, Object request) {
    // Schedule timeout
    Cancellable timeout = context().system().scheduler().scheduleOnce(
        Duration.create(timeoutSeconds, TimeUnit.SECONDS),
        getSelf(),
        new OperationTimeout(requestId),
        context().dispatcher(),
        ActorRef.noSender());

    pendingRequests.put(requestId, new PendingRequest(request, timeout));
  }

  // Cancel a request by ID
  private void cancelRequest(String requestId) {
    PendingRequest pr = pendingRequests.remove(requestId);
    if (pr != null && !pr.timeout.isCancelled()) {
      pr.timeout.cancel();
    }
  }

  private void sendResponse(String id, boolean success, String message) {
    manager.tell(new ClientResponse(id, success, message), getSelf());
  }

  @Override
  public Receive createReceive() {
    return receiveBuilder()
        .match(Update.class, this::onUpdate)
        .match(Get.class, this::onGet)
        .match(UpdateResponse.class, this::onUpdateResponse)
        .match(GetResponse.class, this::onGetResponse)
        .match(UpdateNodeList.class, this::onUpdateNodeList)
        .match(Node.OperationFailed.class, this::onOperationFailed)
        .match(OperationTimeout.class, this::onTimeout)
        .build();
  }
}
