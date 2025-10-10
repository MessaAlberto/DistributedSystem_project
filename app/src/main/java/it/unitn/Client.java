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
  private final Map<String, Cancellable> pendingRequests = new HashMap<>();

  private final int timeoutSeconds;

  private final ActorRef manager; // reference to the main actor (controller/test)

  public Client(List<ActorRef> nodes, String name, ActorRef manager, int timeoutSeconds) {
    this.nodes = nodes;
    this.logger = new Logger(name);
    this.manager = manager;
    this.timeoutSeconds = timeoutSeconds * 2; // Safety margin
  }

  public static Props props(List<ActorRef> nodes, String name, ActorRef manager, int timeoutSeconds) {
    return Props.create(Client.class, () -> new Client(nodes, name, manager, timeoutSeconds));
  }

  // === Messages ===

  public static class Write implements Serializable {
    public ActorRef node = null; // Optional specific node to target
    public final int key;
    public final String value;

    public Write(int key, String value) {
      this.key = key;
      this.value = value;
    }

    public Write(ActorRef node, int key, String value) {
      this.node = node;
      this.key = key;
      this.value = value;
    }
  }

  public static class Read implements Serializable {
    public ActorRef node = null; // Optional specific node to target
    public final int key;

    public Read(int key) {
      this.key = key;
    }

    public Read(ActorRef node, int key) {
      this.node = node;
      this.key = key;
    }
  }

  public static class WriteResponse implements Serializable {
    public final int key;
    public final String value;
    public final int version;

    public WriteResponse(int key, String value, int version) {
      this.key = key;
      this.value = value;
      this.version = version;
    }
  }

  public static class ReadResponse implements Serializable {
    public final int key;
    public final String value;
    public final int version;

    public ReadResponse(int key, String value, int version) {
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

  private void onWrite(Write msg) {
    String requestId = "WRITE " + msg.key + " " + msg.value;
    if (nodes.isEmpty()) {
      sendResponse(requestId, false, "No available nodes to handle Write.");
      return;
    }
    if (pendingRequests.containsKey(requestId)) {
      logger.log("Request already in progress: " + requestId);
      return;
    }

    ActorRef target = msg.node != null ? msg.node : nodes.get(random.nextInt(nodes.size()));
    logger.log("Sending WriteRequest key=" + msg.key + ", value=\"" + msg.value + "\" to " + target.path().name());
    target.tell(new Node.WriteRequest(msg.key, msg.value), getSelf());

    startRequest(requestId, msg);
  }

  private void onRead(Read msg) {
    String requestId = "READ " + msg.key;
    if (nodes.isEmpty()) {
      sendResponse(requestId, false, "No available nodes to handle Read.");
      return;
    }
    if (pendingRequests.containsKey(requestId)) {
      logger.log("Request already in progress: " + requestId);
      return;
    }

    ActorRef target = msg.node != null ? msg.node : nodes.get(random.nextInt(nodes.size()));
    logger.log("Sending ReadRequest key=" + msg.key + " to " + target.path().name());
    target.tell(new Node.ReadRequest(msg.key), getSelf());

    startRequest(requestId, msg);
  }

  private void onWriteResponse(WriteResponse msg) {
    logger.log("Received WriteResponse for key=" + msg.key + " value=\"" + msg.value + "\" version=" + msg.version);
    cancelRequest("WRITE " + msg.key + " " + msg.value);
    sendResponse("WRITE " + msg.key + " " + msg.value, true,
        "Write successful for key=" + msg.key + " version=" + msg.version);
  }


  private void onReadResponse(ReadResponse msg) {
    logger.log("Received ReadResponse for key=" + msg.key + " value=\"" + msg.value + "\" version=" + msg.version);
    cancelRequest("READ " + msg.key);
    sendResponse("READ " + msg.key, true, "Value=\"" + msg.value + "\" version=" + msg.version);
  }

  private void onUpdateNodeList(UpdateNodeList msg) {
    synchronized (nodes) {
      nodes.clear();
      nodes.addAll(msg.newNodes);
    }
    logger.log("Updated node list. Active targets: " + nodes.size());
  }

  private void onOperationFailed(Node.OperationFailed msg) {
    String requestId = "WRITE " + msg.key + " " + msg.value;

    // Try to cancel the request if still pending
    Cancellable pr = pendingRequests.remove(requestId);
    if (pr != null) {
      if (!pr.isCancelled()) {
        pr.cancel();
      }
      logger.logError("Operation failed for key=" + msg.key + ": " + msg.reason);
      sendResponse(requestId, false, "Operation failed: " + msg.reason);
    } else {
      // The request was already completed (likely timed out)
      // logger.log("Ignored late OperationFailed for key=" + msg.key + " (request already handled)");
    }
  }

  private void onOperationTimeout(OperationTimeout msg) {
    Cancellable pr = pendingRequests.remove(msg.requestId);
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

    pendingRequests.put(requestId, timeout);
  }

  // Cancel a request by ID
  private void cancelRequest(String requestId) {
    Cancellable pr = pendingRequests.remove(requestId);
    if (pr != null && !pr.isCancelled()) {
      pr.cancel();
    }
  }

  private void sendResponse(String id, boolean success, String message) {
    logger.log("Sending response to manager: " + id + " success=" + success + " message=" + message);
    manager.tell(new ClientResponse(id, success, message), getSelf());
  }

  @Override
  public Receive createReceive() {
    return receiveBuilder()
        .match(Write.class, this::onWrite)
        .match(Read.class, this::onRead)
        .match(WriteResponse.class, this::onWriteResponse)
        .match(ReadResponse.class, this::onReadResponse)
        .match(UpdateNodeList.class, this::onUpdateNodeList)
        .match(Node.OperationFailed.class, this::onOperationFailed)
        .match(OperationTimeout.class, this::onOperationTimeout)
        .build();
  }
}
