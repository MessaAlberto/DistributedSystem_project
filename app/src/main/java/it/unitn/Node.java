package it.unitn;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import scala.concurrent.duration.Duration;

public class Node extends AbstractActor {
  private final int nodeId;
  private List<ActorRef> peers = new ArrayList<>();

  private final Map<Integer, DataItem> store = new HashMap<>();

  private final int N; // Total number of replicas
  private final int R; // Read quorum size
  private final int W; // Write quorum size

  private final Map<ActorRef, Integer> nodeIdMap = new HashMap<>();

  // Timeout duration in seconds
  private static final int TIMEOUT_SECONDS = 5;

  public Node(int nodeId, int N, int R, int W) {
    this.nodeId = nodeId;
    this.N = N;
    this.R = R;
    this.W = W;
  }

  public static Props props(int nodeId, int N, int R, int W) {
    return Props.create(Node.class, () -> new Node(nodeId, N, R, W));
  }

  // Data structure to hold stored values
  public static class DataItem implements Serializable {
    public final int version;
    public final String value;

    public DataItem(int version, String value) {
      this.version = version;
      this.value = value;
    }
  }

  // =====================
  // Helper Methods
  // =====================

  private List<ActorRef> getResponsibleNodes(int key) {
    List<ActorRef> sortedPeers = new ArrayList<>(peers);
    Collections.sort(sortedPeers, Comparator.comparingInt(a -> nodeIdMap.get(a)));

    long unsignedKey = key & 0xFFFFFFFFL;
    int startIndex = 0;
    boolean found = false;

    for (int i = 0; i < sortedPeers.size(); i++) {
      long nodeKey = nodeIdMap.get(sortedPeers.get(i)) & 0xFFFFFFFFL;
      if (nodeKey >= unsignedKey) {
        startIndex = i;
        found = true;
        break;
      }
    }
    if (!found) {
      startIndex = 0;
    }

    List<ActorRef> responsible = new ArrayList<>();
    int size = sortedPeers.size();
    for (int i = 0; i < N && i < size; i++) {
      responsible.add(sortedPeers.get((startIndex + i) % size));
    }

    return responsible;
  }

  public static String shortName(ActorRef actorRef) {
    String name = actorRef.path().name(); // e.g. "node3"
    if (name.startsWith("node")) {
      return "Node " + name.substring(4);
    }
    return name;
  }

  // =====================
  // Message Classes
  // =====================

  // All message classes: InitPeers, PutRequest, GetRequest, PutReplica,
  // GetVersionRequest,
  // GetVersionResponse, PutValue, PutAck, WriteTimeout, ReadTimeout,
  // OperationFailed

  public static class InitPeers implements Serializable {
    public final List<ActorRef> peers;
    public final Map<ActorRef, Integer> nodeIds;

    public InitPeers(List<ActorRef> peers, Map<ActorRef, Integer> nodeIds) {
      this.peers = peers;
      this.nodeIds = nodeIds;
    }
  }

  public static class PutRequest implements Serializable {
    public final int key;
    public final String value;

    public PutRequest(int key, String value) {
      this.key = key;
      this.value = value;
    }
  }

  public static class GetRequest implements Serializable {
    public final int key;

    public GetRequest(int key) {
      this.key = key;
    }
  }

  public static class PutReplica implements Serializable {
    public final int key;
    public final int version;
    public final String value;

    public PutReplica(int key, int version, String value) {
      this.key = key;
      this.version = version;
      this.value = value;
    }
  }

  public static class GetVersionRequest implements Serializable {
    public final int key;
    public final ActorRef requester;

    public GetVersionRequest(int key, ActorRef requester) {
      this.key = key;
      this.requester = requester;
    }
  }

  public static class GetVersionResponse implements Serializable {
    public final int key;
    public final int version;
    public final String value;
    public final ActorRef responder;

    public GetVersionResponse(int key, int version, String value, ActorRef responder) {
      this.key = key;
      this.version = version;
      this.value = value;
      this.responder = responder;
    }
  }

  public static class PutValue implements Serializable {
    public final int key;
    public final int version;
    public final String value;

    public PutValue(int key, int version, String value) {
      this.key = key;
      this.version = version;
      this.value = value;
    }
  }

  public static class PutAck implements Serializable {
    public final int key;
    public final int version;
    public final ActorRef responder;

    public PutAck(int key, int version, ActorRef responder) {
      this.key = key;
      this.version = version;
      this.responder = responder;
    }
  }

  // Timeout messages for write and read operations
  public static class WriteTimeout implements Serializable {
  }

  public static class ReadTimeout implements Serializable {
  }

  // Failure message to send to client on timeout
  public static class OperationFailed implements Serializable {
    public final int key;
    public final String reason;

    public OperationFailed(int key, String reason) {
      this.key = key;
      this.reason = reason;
    }
  }

  // =====================
  // Coordinator State Classes
  // =====================

  private static class CoordinatorState {
    public final int key;
    public final String newValue;
    public final ActorRef client;
    public final List<ActorRef> responsibleNodes;

    public final Map<ActorRef, GetVersionResponse> versionReplies = new HashMap<>();
    public final Map<ActorRef, PutAck> putAcks = new HashMap<>();

    public int newVersion = 0;

    public CoordinatorState(int key, String newValue, ActorRef client, List<ActorRef> responsibleNodes) {
      this.key = key;
      this.newValue = newValue;
      this.client = client;
      this.responsibleNodes = responsibleNodes;
    }
  }

  // Coordinator state for reads
  private static class ReadCoordinatorState {
    public final int key;
    public final ActorRef client;

    public final Map<ActorRef, GetVersionResponse> versionReplies = new HashMap<>();

    public ReadCoordinatorState(int key, ActorRef client) {
      this.key = key;
      this.client = client;
    }
  }

  // Active coordinator states for ongoing write and read operations
  private final Map<Integer, CoordinatorState> activeCoordinators = new HashMap<>();
  private final Map<Integer, ReadCoordinatorState> activeReadCoordinators = new HashMap<>();

  // =====================
  // Actor Lifecycle: createReceive
  // =====================

  @Override
  public Receive createReceive() {
    return receiveBuilder()
        .match(InitPeers.class, this::onInitPeers)
        .match(PutRequest.class, this::onPutRequest)
        .match(GetRequest.class, this::onGetRequest)
        .match(GetVersionResponse.class, this::onGetVersionResponse)
        .match(PutAck.class, this::onPutAck)
        .match(PutReplica.class, this::onPutReplica)
        .match(GetVersionRequest.class, this::onGetVersionRequest)
        .match(PutValue.class, this::onPutValue)
        .match(WriteTimeout.class, this::onWriteTimeout)
        .match(ReadTimeout.class, this::onReadTimeout)
        .build();
  }

  // =====================
  // Message Handlers
  // =====================

  private void onInitPeers(InitPeers msg) {
    this.peers = new ArrayList<>(msg.peers);
    this.nodeIdMap.clear();
    this.nodeIdMap.putAll(msg.nodeIds);
  }

  private void onPutRequest(PutRequest msg) {
    System.out.println("[Node " + nodeId + "] Received PutRequest for key=" + msg.key + " value=\"" + msg.value + "\"");

    List<ActorRef> responsibleNodes = getResponsibleNodes(msg.key);
    CoordinatorState state = new CoordinatorState(msg.key, msg.value, getSender(), responsibleNodes);
    activeCoordinators.put(msg.key, state);

    for (ActorRef node : responsibleNodes) {
      node.tell(new GetVersionRequest(msg.key, getSelf()), getSelf());
    }

    // Schedule timeout for write operation
    context().system().scheduler().scheduleOnce(
        Duration.create(TIMEOUT_SECONDS, TimeUnit.SECONDS),
        getSelf(),
        new WriteTimeout(),
        context().dispatcher(),
        ActorRef.noSender());
  }

  private void onGetRequest(GetRequest msg) {
    System.out.println("[Node " + nodeId + "] Received GetRequest for key=" + msg.key);

    List<ActorRef> responsibleNodes = getResponsibleNodes(msg.key);
    ReadCoordinatorState state = new ReadCoordinatorState(msg.key, getSender());
    activeReadCoordinators.put(msg.key, state);

    for (ActorRef node : responsibleNodes) {
      node.tell(new GetVersionRequest(msg.key, getSelf()), getSelf());
    }

    // Schedule timeout for read operation
    context().system().scheduler().scheduleOnce(
        Duration.create(TIMEOUT_SECONDS, TimeUnit.SECONDS),
        getSelf(),
        new ReadTimeout(),
        context().dispatcher(),
        ActorRef.noSender());
  }

  private void onGetVersionResponse(GetVersionResponse msg) {
    CoordinatorState writeState = activeCoordinators.get(msg.key);
    if (writeState != null) {
      writeState.versionReplies.put(msg.responder, msg);

      String versionsSummary = writeState.versionReplies.entrySet().stream()
          .map(e -> shortName(e.getKey()) + "=" + e.getValue().version)
          .sorted()
          .reduce((a, b) -> a + ", " + b)
          .orElse("");

      System.out.println("[Node " + nodeId + "] (Write) Version responses: " + versionsSummary);

      if (writeState.versionReplies.size() >= W && writeState.newVersion == 0) {
        int maxVersion = writeState.versionReplies.values().stream()
            .mapToInt(v -> v.version)
            .max()
            .orElse(0);
        writeState.newVersion = maxVersion + 1;

        System.out.println("[Node " + nodeId + "] Computed new version " + writeState.newVersion);

        for (ActorRef node : writeState.responsibleNodes) {
          node.tell(new PutValue(writeState.key, writeState.newVersion, writeState.newValue), getSelf());
        }
      }
      return;
    }

    ReadCoordinatorState readState = activeReadCoordinators.get(msg.key);
    if (readState != null) {
      readState.versionReplies.put(msg.responder, msg);

      String versionsSummary = readState.versionReplies.entrySet().stream()
          .map(e -> shortName(e.getKey()) + "=" + e.getValue().version)
          .sorted()
          .reduce((a, b) -> a + ", " + b)
          .orElse("");

      System.out.println("[Node " + nodeId + "] (Read) Version responses: " + versionsSummary);

      if (readState.versionReplies.size() >= R) {
        GetVersionResponse maxVersionResponse = readState.versionReplies.values().stream()
            .max(Comparator.comparingInt(v -> v.version))
            .orElse(null);

        if (maxVersionResponse != null) {
          System.out
              .println("[Node " + nodeId + "] (Read) Returning value v=" + maxVersionResponse.version + " to client");
          readState.client.tell(maxVersionResponse, getSelf());
        } else {
          readState.client.tell(new GetVersionResponse(msg.key, 0, null, getSelf()), getSelf());
        }

        activeReadCoordinators.remove(msg.key);
      }
    }
  }

  private void onPutAck(PutAck msg) {
    CoordinatorState state = activeCoordinators.get(msg.key);
    if (state == null)
      return;

    state.putAcks.put(msg.responder, msg);
    System.out.println("[Node " + nodeId + "] Received PutAck from " + shortName(msg.responder));

    if (state.putAcks.size() >= W) {
      state.client.tell(new PutAck(state.key, state.newVersion, getSelf()), getSelf());
      System.out
          .println("[Node " + nodeId + "] Write quorum reached for key " + state.key + ", replying success to client");

      activeCoordinators.remove(state.key);
    }
  }

  private void onPutReplica(PutReplica msg) {
    DataItem current = store.get(msg.key);
    if (current == null || msg.version >= current.version) {
      store.put(msg.key, new DataItem(msg.version, msg.value));
      System.out.println(
          "[Node " + nodeId + "] Stored key=" + msg.key + " version=" + msg.version + " value=\"" + msg.value + "\"");
    } else {
      System.out.println("[Node " + nodeId + "] Ignored older PutReplica for key=" + msg.key);
    }
  }

  private void onGetVersionRequest(GetVersionRequest msg) {
    DataItem item = store.get(msg.key);
    int version = 0;
    String value = null;
    if (item != null) {
      version = item.version;
      value = item.value;
    }
    msg.requester.tell(new GetVersionResponse(msg.key, version, value, getSelf()), getSelf());
  }

  private void onPutValue(PutValue msg) {
    DataItem current = store.get(msg.key);
    if (current == null || msg.version >= current.version) {
      store.put(msg.key, new DataItem(msg.version, msg.value));
      System.out.println(
          "[Node " + nodeId + "] Stored key=" + msg.key + " version=" + msg.version + " value=\"" + msg.value + "\"");
    } else {
      System.out.println("[Node " + nodeId + "] Ignored older PutValue for key=" + msg.key);
    }
    getSender().tell(new PutAck(msg.key, msg.version, getSelf()), getSelf());
  }

  // Handle timeout for write operations
  private void onWriteTimeout(WriteTimeout msg) {
    List<Integer> toRemove = new ArrayList<>();
    for (Map.Entry<Integer, CoordinatorState> entry : activeCoordinators.entrySet()) {
      CoordinatorState state = entry.getValue();
      if (state.putAcks.size() < W) {
        System.out.println("[Node " + nodeId + "] WriteTimeout for key " + state.key + ": quorum not reached");
        state.client.tell(new OperationFailed(state.key, "Write quorum not reached"), getSelf());
        toRemove.add(entry.getKey());
      }
    }
    toRemove.forEach(activeCoordinators::remove);
  }

  // Handle timeout for read operations
  private void onReadTimeout(ReadTimeout msg) {
    List<Integer> toRemove = new ArrayList<>();
    for (Map.Entry<Integer, ReadCoordinatorState> entry : activeReadCoordinators.entrySet()) {
      ReadCoordinatorState state = entry.getValue();
      if (state.versionReplies.size() < R) {
        System.out.println("[Node " + nodeId + "] ReadTimeout for key " + state.key + ": quorum not reached");
        state.client.tell(new OperationFailed(state.key, "Read quorum not reached"), getSelf());
        toRemove.add(entry.getKey());
      }
    }
    toRemove.forEach(activeReadCoordinators::remove);
  }
}