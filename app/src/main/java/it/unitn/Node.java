package it.unitn;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Cancellable;
import akka.actor.Props;
import it.unitn.TestManager.RingUtils;
import scala.concurrent.duration.Duration;

public class Node extends AbstractActor {

  private final Logger logger;

  private final int nodeId;
  private final List<ActorRef> nodes = new ArrayList<>();
  private final Map<ActorRef, Integer> nodeIdMap = new HashMap<>(); // Mappa ActorRef -> nodeId dei peer conosciuti
  private boolean joining;
  private final ActorRef testManager;
  private ActorRef bootstrap; // Bootstrap node for joining/recovery
  private Mode mode; // Current mode of the node
  private boolean serving;

  private final Map<Integer, DataItem> store = new HashMap<>();
  private final Map<Integer, ActorRef> writeLocks = new HashMap<>();

  private final int N; // Replication factor (numero di repliche)
  private final int R; // Read quorum size
  private final int W; // Write quorum size

  // Timeout duration in seconds
  private final int timeoutSeconds;
  private Cancellable joinTimeout;  //TODO
  private Cancellable recoverTimeout;

  // Keys that need to be recovered during JOIN/RECOVERY
  private final Set<Integer> recoveryKeysBuffer = new HashSet<>();
  private int receivedResponseForRecovery = 0;

  // Active coordinator states for ongoing write and read operations
  private final Map<Integer, WriteCoordinatorState> activeWriteCoordinators = new HashMap<>();
  private final Map<String, ReadCoordinatorState> activeReadCoordinators = new HashMap<>();

  /** Constructs a node with its id and quorum parameters (N,R,W). */
  public Node(int nodeId, int N, int R, int W, boolean joining, ActorRef testManager, ActorRef bootstrap,
      int timeoutSeconds) {
    this.nodeId = nodeId;
    this.N = N;
    this.R = R;
    this.W = W;

    this.joining = joining;
    if (joining) {
      this.mode = Mode.JOINING;
      this.serving = false;
    } else {
      this.mode = Mode.IDLE;
      this.serving = true;
    }
    this.testManager = testManager;
    this.bootstrap = bootstrap;
    this.timeoutSeconds = timeoutSeconds;

    logger = new Logger("Node " + nodeId);
  }

  /** Akka factory method to create Props for this actor. */
  public static Props props(int nodeId, int N, int R, int W, ActorRef testManager,
      int timeoutSeconds) {
    return Props.create(Node.class, () -> new Node(nodeId, N, R, W, false, testManager, null, timeoutSeconds));
  }

  public static Props props(int nodeId, int N, int R, int W, boolean joining, ActorRef testManager, ActorRef bootstrap,
      int timeoutSeconds) {
    return Props.create(Node.class, () -> new Node(nodeId, N, R, W, joining, testManager, bootstrap, timeoutSeconds));
  }

  @Override
  public void preStart() {
    // joining nodes contact the testManager
    if (joining) {
      getContext().become(joiningRecoveringReceive());

      // Ask to bootstrap for current peers
      bootstrap.tell(new GetPeersRequest(), getSelf());

      // Schedule a timeout in case bootstrap doesn't respond
      joinTimeout = getContext().system().scheduler().scheduleOnce(
          Duration.create(timeoutSeconds, TimeUnit.SECONDS),
          getSelf(),
          new JoinTimeout(),
          getContext().system().dispatcher(),
          getSelf());
    }
  }

  // =====================
  // Actor Receive Methods
  // =====================
  private enum Mode {
    IDLE, JOINING, RECOVERING, CRASHED
  }

  // =====================
  // Coordinator State Classes
  // =====================

  private class WriteCoordinatorState {
    public final int key;
    public final String newValue;
    public final ActorRef client;
    public int newVersion = 0;

    public final List<ActorRef> responsibleNodes;
    public final Map<ActorRef, VersionResponse> versionReplies = new HashMap<>();
    public Cancellable timeout;

    public WriteCoordinatorState(int key, String newValue, ActorRef client, List<ActorRef> responsibleNodes) {
      this.key = key;
      this.newValue = newValue;
      this.client = client;
      this.responsibleNodes = responsibleNodes;

      this.timeout = context().system().scheduler().scheduleOnce(
          Duration.create(timeoutSeconds, TimeUnit.SECONDS),
          getSelf(),
          new UpdateTimeout(key),
          context().dispatcher(),
          ActorRef.noSender());
    }
  }

  // Coordinator state for reads
  private class ReadCoordinatorState {
    public final int key;
    public final ActorRef client;
    public final Map<ActorRef, VersionResponse> versionReplies = new HashMap<>();
    public final Cancellable timeout;

    public ReadCoordinatorState(int key, ActorRef client) {
      this.key = key;
      this.client = client;

      this.timeout = context().system().scheduler().scheduleOnce(
          Duration.create(timeoutSeconds, TimeUnit.SECONDS),
          getSelf(),
          new ReadTimeout(key, client),
          context().dispatcher(),
          ActorRef.noSender());
    }
  }

  // =====================
  // Data structure to hold stored values
  // =====================
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

  /**
   * Returns the N responsible replica nodes for a key (ring order, wrap-around).
   */
  private List<ActorRef> getResponsibleNodes(int key, List<ActorRef> nodes) {
    List<ActorRef> sortedPeers = new ArrayList<>(nodes);
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

  /** Removes local keys for which this node is no longer responsible. */
  private void pruneKeysNotResponsible() {
    List<Integer> toRemove = new ArrayList<>();
    for (Map.Entry<Integer, DataItem> e : store.entrySet()) {
      int key = e.getKey();
      List<ActorRef> resp = getResponsibleNodes(key, nodes);
      if (!resp.contains(getSelf())) {
        toRemove.add(key);
      }
    }
    for (Integer k : toRemove) {
      store.remove(k);
      logger.log("Pruned key=" + k + " (no longer responsible)");
    }
  }

  // === Helpers per maintenance gating ===

  private void startMaintenanceReads(Set<Integer> keys) {
    if (keys == null || keys.isEmpty()) {
      completeMaintenanceIfDone();
      return;
    }
    // Update pending keys set and start reads
    for (Integer key : keys) {
      ReadCoordinatorState state = new ReadCoordinatorState(key, getSelf());
      activeReadCoordinators.put(key + " " + getSelf().path().name(), state);

      for (ActorRef node : getResponsibleNodes(key, nodes)) {
        node.tell(new GetVersion(key, getSelf()), getSelf());
      }
    }
  }

  private boolean hasActiveMaintenanceCoordinators() {
    for (ReadCoordinatorState state : activeReadCoordinators.values()) {
      if (state.client == getSelf()) {
        return true;
      }
    }
    return false;
  }

  private void completeMaintenanceIfDone() {
    if (hasActiveMaintenanceCoordinators()) {
      return;
    }

    if (mode == Mode.JOINING) {
      nodeIdMap.put(getSelf(), nodeId);
      nodes.add(getSelf());
      // Announce join to other nodes
      for (ActorRef p : nodes) {
        if (p != getSelf()) {
          p.tell(new AnnounceJoin(getSelf(), nodeId), getSelf());
        }
      }
      testManager.tell(new TestManager.NodeActionResponse("join", nodeId, true), getSelf());
      this.serving = true;
      this.mode = Mode.IDLE;
      getContext().become(activeReceive());
      logger.log("JOIN completed (maintenance done) and serving.");
    } else if (mode == Mode.RECOVERING) {
      testManager.tell(new TestManager.NodeActionResponse("recover", nodeId, true), getSelf());
      this.serving = true;
      this.mode = Mode.IDLE;
      getContext().become(activeReceive());
      logger.log("RECOVERY completed (maintenance done) and serving.");
    }
  }

  // =====================
  // Message Classes
  // =====================

  public static class JoinGroupMsg implements Serializable {
    public final List<ActorRef> nodes; // an array of group members
    public final Map<ActorRef, Integer> nodeIdMap;

    public JoinGroupMsg(List<ActorRef> nodes, Map<ActorRef, Integer> nodeIdMap) {
      this.nodes = Collections.unmodifiableList(new ArrayList<>(nodes));
      this.nodeIdMap = Collections.unmodifiableMap(new HashMap<>(nodeIdMap));
    }
  }

  public static class GetRequest implements Serializable {
    public final int key;

    public GetRequest(int key) {
      this.key = key;
    }
  }

  public static class UpdateRequest implements Serializable {
    public final int key;
    public final String value;

    public UpdateRequest(int key, String value) {
      this.key = key;
      this.value = value;
    }
  }

  public static class GetVersion implements Serializable {
    public final int key;
    public final ActorRef requester;

    public GetVersion(int key, ActorRef requester) {
      this.key = key;
      this.requester = requester;
    }
  }

  public static class GetVersionResponse implements Serializable {
    public final int key;
    public final int version;
    public final String value;
    public final ActorRef requester;

    public GetVersionResponse(int key, int version, String value, ActorRef requester) {
      this.key = key;
      this.version = version;
      this.value = value;
      this.requester = requester;
    }
  }

  public static class UpdateVersion implements Serializable {
    public final int key;
    public final ActorRef requester;

    public UpdateVersion(int key, ActorRef requester) {
      this.key = key;
      this.requester = requester;
    }
  }

  public static class UpdateVersionResponse implements Serializable {
    public final int key;
    public final int version;
    public final String value;

    public UpdateVersionResponse(int key, int version, String value) {
      this.key = key;
      this.version = version;
      this.value = value;
    }
  }

  public static class VersionResponse implements Serializable {
    public final int key;
    public final int version;
    public final String value;
    public final ActorRef responder;

    public VersionResponse(int key, int version, String value, ActorRef responder) {
      this.key = key;
      this.version = version;
      this.value = value;
      this.responder = responder;
    }
  }

  public static class UpdateValue implements Serializable {
    public final int key;
    public final int version;
    public final String value;

    public UpdateValue(int key, int version, String value) {
      this.key = key;
      this.version = version;
      this.value = value;
    }
  }

  // Timeout messages for write and read operations
  public static class UpdateTimeout implements Serializable {
    public final int key;

    public UpdateTimeout(int key) {
      this.key = key;
    }
  }

  public static class ReadTimeout implements Serializable {
    public final int key;
    public final ActorRef requester;

    public ReadTimeout(int key, ActorRef requester) {
      this.key = key;
      this.requester = requester;
    }
  }

  public static class JoinTimeout implements Serializable {
  }

  public static class RecoverTimeout implements Serializable {
  }

  // Failure message to send to client on timeout
  public static class OperationFailed implements Serializable {
    public final int key;
    public String value = null;
    public final String reason;

    public OperationFailed(int key, String reason) {
      this.key = key;
      this.reason = reason;
    }

    public OperationFailed(int key, String value, String reason) {
      this.key = key;
      this.value = value;
      this.reason = reason;
    }
  }

  // ===== Membership & transfer messages =====
  public static class NodeAction implements Serializable {
    public final String action; // "leave", "crash", "recover"
    public ActorRef bootstrap = null;

    public NodeAction(String action) {
      this.action = action;
    }

    public NodeAction(String action, ActorRef bootstrap) {
      this.action = action;
      this.bootstrap = bootstrap;
    }
  }

  public static class GetPeersRequest implements Serializable {
  }

  public static class PeerSet implements Serializable {
    public final List<ActorRef> nodes;
    public final Map<ActorRef, Integer> nodeIds;

    public PeerSet(List<ActorRef> nodes, Map<ActorRef, Integer> nodeIds) {
      this.nodes = nodes;
      this.nodeIds = nodeIds;
    }
  }

  public static class ItemRequest implements Serializable {
    public final ActorRef targetNode;

    public ItemRequest(ActorRef targetNode) {
      this.targetNode = targetNode;
    }
  }

  public static class DataItemsBatch implements Serializable {
    public final Map<Integer, DataItem> items;

    public DataItemsBatch(Map<Integer, DataItem> items) {
      this.items = items;
    }
  }

  public static class AnnounceJoin implements Serializable {
    public final ActorRef node;
    public final int nodeId;

    public AnnounceJoin(ActorRef node, int nodeId) {
      this.node = node;
      this.nodeId = nodeId;
    }
  }

  public static class AnnounceLeave implements Serializable {
    public final ActorRef node;
    public final int nodeId;

    public AnnounceLeave(ActorRef node, int nodeId) {
      this.node = node;
      this.nodeId = nodeId;
    }
  }

  public static class TransferData implements Serializable {
    public final int key;
    public final int version;
    public final String value;

    public TransferData(int key, int version, String value) {
      this.key = key;
      this.version = version;
      this.value = value;
    }
  }

  // =====================
  // Message Handlers
  // =====================

  private void onJoinGroupMsg(JoinGroupMsg msg) {
    this.nodes.clear();
    this.nodes.addAll(msg.nodes);
    this.nodeIdMap.clear();
    this.nodeIdMap.putAll(msg.nodeIdMap);
    this.serving = true;
  }

  /**
   * Handles client Get: acts as coordinator, collects versions, returns the
   * highest with timeout.
   */
  private void onGetRequest(GetRequest msg) {
    if (!serving) {
      getSender().tell(new OperationFailed(msg.key, "Node not serving yet"), getSelf());
      return;
    }
    logger.log("Received GetRequest for key=" + msg.key);

    if (writeLocks.containsKey(msg.key)) {
      getSender().tell(new OperationFailed(msg.key, "Another write in progress for this key"), getSelf());
      logger.log("Rejected GetRequest for key=" + msg.key + " due to ongoing write");
      return;
    }

    // Start version gathering phase
    List<ActorRef> responsibleNodes = getResponsibleNodes(msg.key, nodes);
    for (ActorRef node : responsibleNodes) {
      node.tell(new GetVersion(msg.key, getSelf()), getSelf());
    }

    // Initialize coordinator state + start timeout
    ReadCoordinatorState state = new ReadCoordinatorState(msg.key, getSender());
    activeReadCoordinators.put(msg.key + " " + getSender().path().name(), state);
  }

  /**
   * Handles client Put: acts as coordinator, gathers versions, computes new
   * version, writes to replicas with timeout.
   */
  private void onUpdateRequest(UpdateRequest msg) {
    if (!serving) {
      getSender().tell(new OperationFailed(msg.key, msg.value, "Node not serving yet"), getSelf());
      return;
    }
    logger.log("Received UpdateRequest for key=" + msg.key + " value=\"" + msg.value + "\"");

    // Check if some node is already coordinating a write for this key
    if (writeLocks.containsKey(msg.key)) {
      getSender().tell(new OperationFailed(msg.key, msg.value, "Another write in progress for this key"), getSelf());
      logger.log("Rejected UpdateRequest for key=" + msg.key + " due to ongoing write");
      return;
    }
    writeLocks.put(msg.key, getSelf());

    // Start version gathering phase
    List<ActorRef> responsibleNodes = getResponsibleNodes(msg.key, nodes);
    for (ActorRef node : responsibleNodes) {
      node.tell(new UpdateVersion(msg.key, getSelf()), getSelf());
    }

    // Initialize coordinator state + start timeout
    WriteCoordinatorState state = new WriteCoordinatorState(msg.key, msg.value, getSender(), responsibleNodes);
    activeWriteCoordinators.put(msg.key, state);
  }

  /** Replies with the current version/value of a key to the requester. */
  private void onGetVersion(GetVersion msg) {
    if (writeLocks.containsKey(msg.key)) {
      // Write in progress, reject
      msg.requester.tell(new GetVersionResponse(msg.key, -1, "", msg.requester), getSelf());
      return;
    }

    int version = 0;
    String value = null;
    DataItem item = store.get(msg.key);
    if (item != null) {
      version = item.version;
      value = item.value;
    }
    msg.requester.tell(new GetVersionResponse(msg.key, version, value, msg.requester), getSelf());
  }

  private void onGetVersionResponse(GetVersionResponse msg) {
    if (msg.version == -1) {
      // The sender node is busy with another write
      if (mode == Mode.JOINING || mode == Mode.RECOVERING) {
        logger.logError("Maintenance GetVersionResponse for key=" + msg.key
            + " rejected by node " + nodeIdMap.get(getSender()) + " due to ongoing write");
      }
      logger.log("Ignored GetVersionResponse for key=" + msg.key + " due to ongoing write");
      return;
    }

    ReadCoordinatorState state = activeReadCoordinators.get(msg.key + " " + msg.requester.path().name());
    if (state == null) {
      // No active read for this key (maybe quorum already reached)
      return;
    }

    // Register response
    state.versionReplies.put(getSender(), new VersionResponse(msg.key, msg.version, msg.value, getSender()));

    String versionsSummary = state.versionReplies.entrySet().stream()
        .map(e -> nodeIdMap.get(e.getKey()) + "=" + e.getValue().version)
        .sorted()
        .reduce((a, b) -> a + ", " + b)
        .orElse("");
    logger.log("(Read) GetVersion responses: " + versionsSummary);

    if (state.versionReplies.size() >= R) {
      // Quorum reached, get the response with the highest version
      VersionResponse maxVersionResponse = state.versionReplies.values().stream()
          .max(Comparator.comparingInt(v -> v.version))
          .get();

      // Update local store if the version is newer
      DataItem current = store.get(msg.key);
      if (current == null || maxVersionResponse.version > current.version) {
        store.put(msg.key, new DataItem(maxVersionResponse.version, maxVersionResponse.value));
        logger.log("(Read) Updated local store key=" + msg.key + " v=" + maxVersionResponse.version);
      }

      // Respond to client if this is an external read
      if (mode == Mode.IDLE && state.client != getSelf()) {
        logger.log("(Read) Returning value v=" + maxVersionResponse.version + " to client");
        state.client.tell(new Client.GetResponse(msg.key, maxVersionResponse.value, maxVersionResponse.version),
            getSelf());
      }

      // Cleanup state and timeout
      if (state.timeout != null && !state.timeout.isCancelled()) {
        state.timeout.cancel();
      }
      activeReadCoordinators.remove(msg.key + " " + msg.requester.path().name());

      // Maintenance check
      if (mode == Mode.JOINING || mode == Mode.RECOVERING) {
        // If maintenance read (client == self), count completion
        if (state.client == getSelf()) {
          completeMaintenanceIfDone();
        } else {
          logger.logError("Unexpected external read during maintenance for key=" + msg.key);
        }
      }
    }
  }

  private void onUpdateVersion(UpdateVersion msg) {
    if (writeLocks.containsKey(msg.key)) {
      // Write in progress, reject
      msg.requester.tell(new UpdateVersionResponse(msg.key, -1, ""), getSelf());
      return;
    }
    writeLocks.put(msg.key, getSender());

    int version = 0;
    String value = null;
    DataItem item = store.get(msg.key);
    if (item != null) {
      version = item.version;
      value = item.value;
    }
    msg.requester.tell(new UpdateVersionResponse(msg.key, version, value), getSelf());
  }

  private void onUpdateVersionResponse(UpdateVersionResponse msg) {
    if (msg.version == -1) {
      // The sender node is busy with another write
      logger.log("Ignored UpdateVersionResponse for key=" + msg.key + " due to ongoing write");
      return;
    }

    WriteCoordinatorState state = activeWriteCoordinators.get(msg.key);
    if (state == null) {
      // No active write for this key (maybe quorum already reached)
      return;
    }

    // Register response
    state.versionReplies.put(getSender(), new VersionResponse(msg.key, msg.version, msg.value, getSender()));

    String versionsSummary = state.versionReplies.entrySet().stream()
        .map(e -> nodeIdMap.get(e.getKey()) + "=" + e.getValue().version)
        .sorted()
        .reduce((a, b) -> a + ", " + b)
        .orElse("");
    logger.log("(Write) UpdateVersion responses: " + versionsSummary);

    if (state.versionReplies.size() >= W && state.newVersion == 0) {
      // Quorum reached, compute new version
      int maxVersion = state.versionReplies.values().stream()
          .mapToInt(v -> v.version)
          .max()
          .orElse(0);
      state.newVersion = maxVersion + 1;

      logger.log("Computed new version " + state.newVersion);

      // Send UpdateValue to replicas and reply to client
      for (ActorRef node : state.responsibleNodes) {
        node.tell(new UpdateValue(state.key, state.newVersion, state.newValue), getSelf());
      }
      state.client.tell(new Client.UpdateResponse(state.key, state.newValue, state.newVersion), getSelf());

      // Cleanup state, timeout and lock
      if (state.timeout != null && !state.timeout.isCancelled()) {
        state.timeout.cancel();
      }
      writeLocks.remove(state.key);
      activeWriteCoordinators.remove(state.key);
    }
  }

  /**
   * Handles actual value update on replicas, checking version and write lock.
   */
  private void onUpdateValue(UpdateValue msg) {
    if (writeLocks.containsKey(msg.key) && writeLocks.get(msg.key) != getSender()) {
      // TODO: is a problem?
      logger.log("Ignored UpdateValue for key=" + msg.key + " due to ongoing write");
      return;
    }
    writeLocks.remove(msg.key);

    DataItem current = store.get(msg.key);
    if (current == null || msg.version >= current.version) {
      store.put(msg.key, new DataItem(msg.version, msg.value));
      logger.log(
          "Stored key=" + msg.key + " version=" + msg.version + " value=\"" + msg.value + "\"");
    } else {
      logger.log("Ignored older UpdateValue for key=" + msg.key);
    }
  }

  /**
   * Handles write timeout: if quorum W not reached, fail client and clean
   * coordinator state.
   */
  private void onUpdateTimeout(UpdateTimeout msg) {
    WriteCoordinatorState state = activeWriteCoordinators.get(msg.key);
    if (state == null) {
      return; // Already completed
    }

    writeLocks.remove(state.key);
    activeWriteCoordinators.remove(state.key);
    if (state.timeout != null && !state.timeout.isCancelled()) {
      state.timeout.cancel();
    }
    if (state.versionReplies.size() < W) {
      logger.log("UpdateTimeout for key " + state.key + ": quorum not reached");
      for (ActorRef node : state.responsibleNodes) {
        if (node != getSelf()) {
          node.tell(new OperationFailed(state.key, state.newValue, "Write quorum not reached"), getSelf());
        }
      }
      state.client.tell(new OperationFailed(state.key, state.newValue, "Write quorum not reached"), getSelf());
    }
  }

  /**
   * Handles read timeout: if quorum R not reached, fail client and clean
   * coordinator state.
   */
  private void onReadTimeout(ReadTimeout msg) {
    ReadCoordinatorState state = activeReadCoordinators.get(msg.key + " " + msg.requester.path().name());
    if (state == null) {
      return; // Already completed
    }

    activeReadCoordinators.remove(msg.key + " " + msg.requester.path().name());
    if (state.timeout != null && !state.timeout.isCancelled()) {
      state.timeout.cancel();
    }
    if (state.versionReplies.size() < R) {
      logger.log("ReadTimeout for key " + state.key + ": quorum not reached");
      state.client.tell(new OperationFailed(state.key, "Read quorum not reached"), getSelf());
    }
  }

  private void onJoinTimeout(JoinTimeout msg) {
    if (mode != Mode.JOINING) {
      logger.logError("JoinTimeout: not in JOINING mode");
      return; // Already joined or not joining
    }
    logger.logError("JoinTimeout: bootstrap did not respond in time");
    testManager.tell(new TestManager.NodeActionResponse("join", nodeId, false), getSelf());
    getContext().stop(getSelf());
  }

  private void onRecoverTimeout(RecoverTimeout msg) {
    if (mode != Mode.RECOVERING) {
      logger.logError("RecoverTimeout: not in RECOVERING mode");
      return; // Already recovered or not recovering
    }
    logger.logError("RecoverTimeout: bootstrap did not respond in time");
    testManager.tell(new TestManager.NodeActionResponse("recover", nodeId, false), getSelf());
    mode = Mode.CRASHED;
    getContext().become(crashedReceive());
  }

  // =====================
  // Membership & transfer handlers
  // =====================

  private void onNodeAction(NodeAction msg) {
    if (msg.action.equals("leave")) {
      logger.log("Received leave request");
      if (mode != Mode.IDLE) {
        logger.logError("Cannot leave while not in IDLE mode");
        return;
      }
      this.mode = Mode.CRASHED; // to block client requests
      this.serving = false;
      getContext().become(crashedReceive());
      logger.log("TransferData to successor and leaving network");

      List<ActorRef> tempNodes = new ArrayList<>(this.nodes);
      tempNodes.remove(getSelf());

      for (Map.Entry<Integer, DataItem> e : new ArrayList<>(store.entrySet())) {
        int key = e.getKey();
        DataItem di = e.getValue();
        List<ActorRef> responsible = getResponsibleNodes(key, tempNodes);

        for (ActorRef target : responsible) {
          target.tell(new TransferData(key, di.version, di.value), getSelf());
        }
      }

      // Announce leave to other nodes
      for (ActorRef p : tempNodes) {
        p.tell(new AnnounceLeave(getSelf(), nodeId), getSelf());
      }

      testManager.tell(new TestManager.NodeActionResponse("leave", nodeId, true), getSelf());
      getContext().stop(getSelf()); // stop the actor
    }

    else if (msg.action.equals("crash")) {
      if (mode != Mode.IDLE) {
        logger.logError("Cannot crash while not in IDLE mode");
        return;
      }
      this.mode = Mode.CRASHED;
      this.serving = false;
      getContext().become(crashedReceive());
      logger.log("Crashing node");
      testManager.tell(new TestManager.NodeActionResponse("crash", nodeId, true), getSelf());
    }

    else if (msg.action.equals("recover")) {
      if (mode != Mode.CRASHED) {
        logger.logError("Cannot recover while not in CRASHED mode");
        return;
      }
      logger.log("Starting RECOVERY via testManager");
      this.mode = Mode.RECOVERING;
      this.serving = false;
      getContext().become(joiningRecoveringReceive());
      msg.bootstrap.tell(new GetPeersRequest(), getSelf());

      // Schedule a timeout in case bootstrap doesn't respond
      getContext().system().scheduler().scheduleOnce(
          Duration.create(timeoutSeconds, TimeUnit.SECONDS),
          getSelf(),
          new RecoverTimeout(),
          getContext().system().dispatcher(),
          getSelf());
    }

    else {
      logger.logError("Unknown NodeAction: " + msg.action);
    }
  }

  /** Returns the current membership view to the requester (bootstrap helper). */
  private void onGetPeersRequest(GetPeersRequest msg) {
    // Answer with current nodes and their IDs
    getSender().tell(new PeerSet(new ArrayList<>(nodes), new HashMap<>(nodeIdMap)), getSelf());
  }

  /**
   * Handles a PeerSet from bootstrap: complete JOIN/RECOVERY setup or refresh
   * view.
   */
  private void onPeerSet(PeerSet msg) {
    nodes.clear();
    nodes.addAll(msg.nodes);
    nodeIdMap.putAll(msg.nodeIds);

    if (mode == Mode.JOINING) {
      ActorRef successor = RingUtils.findSuccessorOfId(nodeId, nodes, nodeIdMap);

      if (successor != null) {
        logger.log("JOIN: requesting items from " + nodeIdMap.get(successor));
        successor.tell(new ItemRequest(getSelf()), getSelf());
      } else {
        logger.logError(
            "JOIN: due specifics of the project, the network has always an active node. Here, no successor has been found => ERROR.");
        return;
      }
    }

    else if (mode == Mode.RECOVERING) {
      receivedResponseForRecovery = 0;

      // pick immediate successor and predecessor
      ActorRef successor = RingUtils.findSuccessorOfId(nodeId, nodes, nodeIdMap);
      ActorRef predecessor = RingUtils.findPredecessorOfId(nodeId, nodes, nodeIdMap);

      // send recovery requests
      logger.log("RECOVER: requesting items from successor " + nodeIdMap.get(successor));
      successor.tell(new ItemRequest(getSelf()), getSelf());

      logger.log("RECOVER: requesting items from predecessor " + nodeIdMap.get(predecessor));
      predecessor.tell(new ItemRequest(getSelf()), getSelf());
    }

    else {
      logger.log("Received PeerSet in mode " + mode + ", ignoring.");
      return;
    }
  }

  private void onItemRequest(ItemRequest msg) {
    Map<Integer, DataItem> batch = new LinkedHashMap<>();
    List<ActorRef> tempNodes = new ArrayList<>(this.nodes);
    tempNodes.add(msg.targetNode);

    for (Map.Entry<Integer, DataItem> e : store.entrySet()) {
      int key = e.getKey();
      List<ActorRef> resp = getResponsibleNodes(key, tempNodes);
      if (resp.contains(msg.targetNode)) {
        batch.put(key, e.getValue());
      }
    }

    logger.log(mode + ": sending " + batch.size() + " items to " + nodeIdMap.get(msg.targetNode));
    msg.targetNode.tell(new DataItemsBatch(batch), getSelf());
  }

  /**
   * Processes a batch of items during JOIN/RECOVERY (install, update view,
   * maintenance) or generic transfer.
   */
  private void onDataItemsBatch(DataItemsBatch msg) {
    if (mode == Mode.JOINING) {
      // 1) Store received items
      store.clear();
      for (Map.Entry<Integer, DataItem> e : msg.items.entrySet()) {
        DataItem cur = store.get(e.getKey());
        if (cur == null || e.getValue().version >= cur.version) {
          store.put(e.getKey(), e.getValue());
        }
      }

      // 2) Start maintenance read on received keys
      startMaintenanceReads(new HashSet<>(msg.items.keySet()));
      // Do not notify join completion until maintenance done

    } else if (mode == Mode.RECOVERING) {
      // 1) Store received items
      for (Map.Entry<Integer, DataItem> e : msg.items.entrySet()) {
        DataItem cur = store.get(e.getKey());
        if (cur == null || e.getValue().version >= cur.version) {
          store.put(e.getKey(), e.getValue());
        }
      }
      recoveryKeysBuffer.addAll(msg.items.keySet());
      receivedResponseForRecovery++;

      // Check for completion of both responses
      if (receivedResponseForRecovery >= 2) {
        // 2) Update view (in case of node joins/leaves during crash)
        pruneKeysNotResponsible();

        // 3) Maintenance read on recovered keys
        startMaintenanceReads(new HashSet<>(recoveryKeysBuffer));
        recoveryKeysBuffer.clear();
      } else {
        logger.log("RECOVER: waiting for more responses (" + receivedResponseForRecovery + "/2)");
      }
    } else {
      // Generic transfer
      for (Map.Entry<Integer, DataItem> e : msg.items.entrySet()) {
        DataItem cur = store.get(e.getKey());
        if (cur == null || e.getValue().version >= cur.version) {
          store.put(e.getKey(), e.getValue());
        }
      }
    }
  }

  /**
   * Adds a newly joined node to the local view, backfills keys it should now
   * replicate, then prunes.
   */
  private void onAnnounceJoin(AnnounceJoin msg) {
    if (!nodeIdMap.containsKey(msg.node)) {
      nodes.add(msg.node);
      nodeIdMap.put(msg.node, msg.nodeId);

      // Remove keys that this node is no longer responsible for
      pruneKeysNotResponsible();
      logger.log("AnnounceJoin: added " + nodeIdMap.get(msg.node) + " with backfill");
    }
  }

  /** Removes a leaving node from the view and prunes non-responsible keys. */
  private void onAnnounceLeave(AnnounceLeave msg) {
    if (nodeIdMap.containsKey(msg.node)) {
      nodes.remove(msg.node);
      nodeIdMap.remove(msg.node);

      // Remove keys that this node is no longer responsible for
      pruneKeysNotResponsible();
      logger.log("AnnounceLeave: removed " + nodeIdMap.get(msg.node));
    }
  }

  /**
   * Accepts a transferred key/value and installs it if version is newer or equal.
   */
  private void onTransferData(TransferData msg) {
    DataItem cur = store.get(msg.key);
    if (cur == null || msg.version >= cur.version) {
      store.put(msg.key, new DataItem(msg.version, msg.value));
      logger.log("Received transfer key=" + msg.key + " v=" + msg.version);
    }
  }

  private void onOperationFailed(OperationFailed msg) {
    if (msg.equals("Write quorum not reached")) {
      logger.logError("OperationFailed: write quorum not reached for key=" + msg.key);
      // Remove lock of that coordinator, if any
      writeLocks.remove(msg.key);
    } else {
      logger.logError("OperationFailed: " + msg.reason + " for key=" + msg.key);
    }
  }

  // =====================
  // Actor Lifecycle: behaviors
  // =====================

  // Behavior operativo: gestisce tutte le richieste
  private Receive activeReceive() {
    return receiveBuilder()
        .match(JoinGroupMsg.class, this::onJoinGroupMsg)

        .match(GetRequest.class, this::onGetRequest)
        .match(UpdateRequest.class, this::onUpdateRequest)

        .match(GetVersion.class, this::onGetVersion)
        .match(GetVersionResponse.class, this::onGetVersionResponse)

        .match(UpdateVersion.class, this::onUpdateVersion)
        .match(UpdateVersionResponse.class, this::onUpdateVersionResponse)

        .match(UpdateValue.class, this::onUpdateValue)

        .match(UpdateTimeout.class, this::onUpdateTimeout)
        .match(ReadTimeout.class, this::onReadTimeout)

        // Membership & transfer
        .match(NodeAction.class, this::onNodeAction)
        .match(GetPeersRequest.class, this::onGetPeersRequest)

        .match(ItemRequest.class, this::onItemRequest)
        .match(AnnounceJoin.class, this::onAnnounceJoin)
        .match(AnnounceLeave.class, this::onAnnounceLeave)
        .match(TransferData.class, this::onTransferData)
        .match(OperationFailed.class, this::onOperationFailed)
        .build();
  }

  // Behavior di JOIN/RECOVERY: nessun Put/Get; gestisce bootstrap e catch-up
  private Receive joiningRecoveringReceive() {
    return receiveBuilder()
        .match(JoinTimeout.class, this::onJoinTimeout)

        .match(PeerSet.class, this::onPeerSet)
        .match(DataItemsBatch.class, this::onDataItemsBatch)
        .match(GetVersionResponse.class, this::onGetVersionResponse)

        .match(ReadTimeout.class, this::onReadTimeout)

        .match(GetRequest.class, this::onGetRequest)
        .match(UpdateRequest.class, this::onUpdateRequest)
        .build();
  }

  // Behavior di CRASH: ignora tutto, tranne RecoverNetwork
  private Receive crashedReceive() {
    return receiveBuilder()
        .match(RecoverTimeout.class, this::onRecoverTimeout)
        .match(NodeAction.class, this::onNodeAction)
        .matchAny(m -> {
          /* drop */ })
        .build();
  }

  /** Behavior iniziale = active. */
  @Override
  public Receive createReceive() {
    return activeReceive();
  }
}