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
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Cancellable;
import akka.actor.Props;
import it.unitn.TestManager.RingUtils;
import scala.concurrent.duration.Duration;

public class Node extends AbstractActor {

  private final Logger logger;

  private final int nodeId; // TODO: need to be treated as unsigned int
  private final List<ActorRef> nodes = new ArrayList<>();
  private final Map<ActorRef, Integer> nodeIdMap = new HashMap<>(); // Map of node ActorRef to nodeId
  private boolean joining;
  private final ActorRef testManager;
  private ActorRef bootstrap; // Bootstrap node for joining
  private Mode mode; // Current mode of the node
  private boolean serving;
  // TODO: MAP KEYneed to be treated as unsigned int
  private final Map<Integer, DataItem> store = new HashMap<>();
  private final Map<Integer, ActorRef> writeLocks = new HashMap<>();

  private final int N; // Replication factor (numero di repliche)
  private final int R; // Read quorum size
  private final int W; // Write quorum size

  // Timeout duration in seconds
  private final int timeoutSeconds;
  private Cancellable joinTimeout; // TODO
  private Cancellable recoverTimeout;
  private final Map<String, String> pendingDataTransferDescriptions = new LinkedHashMap<>();

  //dealy parameters

  private static final long MIN_PROP_DELAY_MS = 10;
  private static final long MAX_PROP_DELAY_MS = 60;


  // Keys that need to be recovered during JOIN/RECOVERY
  private final Set<Integer> recoveryKeysBuffer = new HashSet<>();
  private int receivedResponseForRecovery = 0;

  // Active coordinator states for ongoing write and read operations
  // key -> state
  private final Map<Integer, WriteRequestState> writeRequestList = new HashMap<>();
  // "key + clientName" -> state
  private final Map<String, ReadRequestState> readRequestList = new HashMap<>();

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
      sendWithRandomDelay(bootstrap, new GetPeers());

      scheduleMembershipTimeout("waiting bootstrap peer response for JOIN");
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

  private class WriteRequestState {
    public final int key;
    public final String newValue;
    public final ActorRef client;
    public int newVersion = 0;

    public final List<ActorRef> responsibleNodes;
    public final Map<ActorRef, VersionResponse> versionReplies = new HashMap<>();
    public Cancellable timeout;
    public final String timeoutDetail;

    public WriteRequestState(int key, String newValue, ActorRef client, List<ActorRef> responsibleNodes) {
      this.key = key;
      this.newValue = newValue;
      this.client = client;
      this.responsibleNodes = responsibleNodes;
      this.timeoutDetail = buildWriteTimeoutDetail(key, client);

      this.timeout = context().system().scheduler().scheduleOnce(
          Duration.create(timeoutSeconds, TimeUnit.SECONDS),
          getSelf(),
          new WriteTimeout(key, timeoutDetail),
          context().dispatcher(),
          ActorRef.noSender());
    }
  }

  // Coordinator state for reads
  private class ReadRequestState {
    public final int key;
    public final ActorRef client;
    public final Map<ActorRef, VersionResponse> versionReplies = new HashMap<>();
    public final Cancellable timeout;
    public final String timeoutDetail;

    public ReadRequestState(int key, ActorRef client) {
      this.key = key;
      this.client = client;
      this.timeoutDetail = buildReadTimeoutDetail(key, client);

      this.timeout = context().system().scheduler().scheduleOnce(
          Duration.create(timeoutSeconds, TimeUnit.SECONDS),
          getSelf(),
          new ReadTimeout(key, client, timeoutDetail),
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
  private List<ActorRef> getResponsibleNodes(int key, List<ActorRef> nodes, Map<ActorRef, Integer> nodeIdMap) {
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
      List<ActorRef> resp = getResponsibleNodes(key, nodes, nodeIdMap);
      if (!resp.contains(getSelf())) {
        toRemove.add(key);
      }
    }
    for (Integer k : toRemove) {
      store.remove(k);
      logger.log("Pruned key=" + k + " (no longer responsible)");
    }
  }

  private List<ActorRef> peersExcludingSelf() {
    List<ActorRef> peers = new ArrayList<>();
    for (ActorRef node : nodes) {
      if (node != getSelf()) {
        peers.add(node);
      }
    }
    return peers;
  }

  private String describeRequester(ActorRef requester) {
    if (requester == null) {
      return "unknown";
    }
    if (requester == getSelf()) {
      return "maintenance";
    }
    return requester.path().name();
  }

  private String buildReadTimeoutDetail(int key, ActorRef requester) {
    return "Read quorum R=" + R + " not reached for key " + key + " (requester=" + describeRequester(requester) + ")";
  }

  private String buildWriteTimeoutDetail(int key, ActorRef client) {
    return "Write quorum W=" + W + " not reached for key " + key + " (client=" + describeRequester(client) + ")";
  }

  // === Helpers per gestione timeout JOIN/RECOVER ===

  private void registerPendingTransfer(String context, String description) {
    if (context != null && description != null) {
      pendingDataTransferDescriptions.put(context, description);
    }
  }

  private void clearPendingTransfers() {
    pendingDataTransferDescriptions.clear();
  }

  private String describePendingTransfers() {
    return pendingDataTransferDescriptions.values().stream()
        .reduce((a, b) -> a + ", " + b)
        .orElse("unknown peer");
  }

  private String newTransferContext(String prefix) {
    return prefix + "-" + System.nanoTime();
  }

  private void scheduleMembershipTimeout(String detail) {
    if (mode != Mode.JOINING && mode != Mode.RECOVERING) {
      return;
    }
    cancelMembershipTimeout();

    if (mode == Mode.JOINING) {
      joinTimeout = getContext().system().scheduler().scheduleOnce(
          Duration.create(timeoutSeconds, TimeUnit.SECONDS),
          getSelf(),
          new JoinTimeout(detail),
          getContext().system().dispatcher(),
          getSelf());
    } else if (mode == Mode.RECOVERING) {
      recoverTimeout = getContext().system().scheduler().scheduleOnce(
          Duration.create(timeoutSeconds, TimeUnit.SECONDS),
          getSelf(),
          new RecoverTimeout(detail),
          getContext().system().dispatcher(),
          getSelf());
    }
    if (detail != null) {
      logger.log("Timeout armed for " + mode + ": " + detail);
    }
  }

  private void cancelMembershipTimeout() {
    if (joinTimeout != null && !joinTimeout.isCancelled()) {
      joinTimeout.cancel();
    }
    joinTimeout = null;

    if (recoverTimeout != null && !recoverTimeout.isCancelled()) {
      recoverTimeout.cancel();
    }
    recoverTimeout = null;
  }

  private void failMembershipAction(String detail) {
    if (mode != Mode.JOINING && mode != Mode.RECOVERING) {
      logger.logError("failMembershipAction called while mode=" + mode + ", reason=" + detail);
      return;
    }

    String reason = (detail == null || detail.isEmpty()) ? "Unknown membership failure" : detail;
    cancelMembershipTimeout();
    clearPendingTransfers();

    if (mode == Mode.JOINING) {
      logger.logError("JOIN failed: " + reason);
      testManager.tell(new TestManager.NodeActionResponse("join", nodeId, false, reason), getSelf());
      getContext().stop(getSelf());
    } else {
      logger.logError("RECOVER failed: " + reason);
      testManager.tell(new TestManager.NodeActionResponse("recover", nodeId, false, reason), getSelf());
      mode = Mode.CRASHED;
      serving = false;
      getContext().become(crashedReceive());
    }
  }

  // === Helpers per maintenance gating ===

  private void startMaintenanceReads(Set<Integer> keys) {
    if (keys == null || keys.isEmpty()) {
      completeMaintenanceIfDone();
      return;
    }
    if (mode == Mode.JOINING || mode == Mode.RECOVERING) {
      scheduleMembershipTimeout("waiting maintenance reads quorum for " + keys.size() + " keys");
    }
    // Update pending keys set and start reads
    for (Integer key : keys) {
      ReadRequestState state = new ReadRequestState(key, getSelf());
      readRequestList.put(key + " " + getSelf().path().name(), state);

      for (ActorRef node : getResponsibleNodes(key, nodes, nodeIdMap)) {
        sendWithRandomDelay(node, new GetVersionRead(key, getSelf(), getSelf()));
      }
    }
  }

  private boolean hasActiveMaintenanceCoordinators() {
    for (ReadRequestState state : readRequestList.values()) {
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
          sendWithRandomDelay(p, new AnnounceJoin(getSelf(), nodeId));
        }
      }
      cancelMembershipTimeout();
      clearPendingTransfers();
      testManager.tell(new TestManager.NodeActionResponse("join", nodeId, true, "Maintenance completed"), getSelf());
      //sendWithRandomDelay(testManager, new TestManager.NodeActionResponse("join", nodeId, true));
      this.serving = true;
      this.mode = Mode.IDLE;
      getContext().become(activeReceive());
      logger.log("JOIN completed (maintenance done) and serving.");
    } else if (mode == Mode.RECOVERING) {
      cancelMembershipTimeout();
      clearPendingTransfers();
      testManager.tell(new TestManager.NodeActionResponse("recover", nodeId, true, "Maintenance completed"), getSelf());
      //sendWithRandomDelay(testManager, new TestManager.NodeActionResponse("recover", nodeId, true));
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

  public static class ReadRequest implements Serializable {
    public final int key;

    public ReadRequest(int key) {
      this.key = key;
    }
  }

  public static class WriteRequest implements Serializable {
    public final int key;
    public final String value;

    public WriteRequest(int key, String value) {
      this.key = key;
      this.value = value;
    }
  }

  public static class GetVersionRead implements Serializable {
    public final int key;
    public final ActorRef coordinator;
    public final ActorRef requester;

    public GetVersionRead(int key, ActorRef coordinator, ActorRef requester) {
      this.key = key;
      this.coordinator = coordinator;
      this.requester = requester;
    }
  }

  public static class VersionReadResponse implements Serializable {
    public final int key;
    public final int version;
    public final String value;
    public final ActorRef requester;

    public VersionReadResponse(int key, int version, String value, ActorRef requester) {
      this.key = key;
      this.version = version;
      this.value = value;
      this.requester = requester;
    }
  }

  public static class GetVersionWrite implements Serializable {
    public final int key;
    public final ActorRef requester;

    public GetVersionWrite(int key, ActorRef requester) {
      this.key = key;
      this.requester = requester;
    }
  }

  public static class VersionWriteResponse implements Serializable {
    public final int key;
    public final int version;
    public final String value;

    public VersionWriteResponse(int key, int version, String value) {
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
  public static class WriteTimeout implements Serializable {
    public final int key;
    public final String detail;

    public WriteTimeout(int key, String detail) {
      this.key = key;
      this.detail = detail;
    }
  }

  public static class ReadTimeout implements Serializable {
    public final int key;
    public final ActorRef requester;
    public final String detail;

    public ReadTimeout(int key, ActorRef requester, String detail) {
      this.key = key;
      this.requester = requester;
      this.detail = detail;
    }
  }

  public static class JoinTimeout implements Serializable {
    public final String detail;

    public JoinTimeout(String detail) {
      this.detail = detail;
    }
  }

  public static class RecoverTimeout implements Serializable {
    public final String detail;

    public RecoverTimeout(String detail) {
      this.detail = detail;
    }
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

  public static class PrintStore implements Serializable {
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

  public static class GetPeers implements Serializable {
  }

  public static class PeerResponse implements Serializable {
    public final List<ActorRef> nodes;
    public final Map<ActorRef, Integer> nodeIds;

    public PeerResponse(List<ActorRef> nodes, Map<ActorRef, Integer> nodeIds) {
      this.nodes = nodes;
      this.nodeIds = nodeIds;
    }
  }

  public static class ItemRequest implements Serializable {
    public final ActorRef targetNode;
    public final int targetNodeId;
    public final String context;

    public ItemRequest(ActorRef targetNode, int targetNodeId, String context) {
      this.targetNode = targetNode;
      this.targetNodeId = targetNodeId;
      this.context = context;
    }
  }

  public static class DataItemsBatch implements Serializable {
    public final Map<Integer, DataItem> items;
    public final String context;

    public DataItemsBatch(Map<Integer, DataItem> items, String context) {
      this.items = items;
      this.context = context;
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
  private void onReadRequest(ReadRequest msg) {
    if (!serving) {
      sendWithRandomDelay(getSender(), new OperationFailed(msg.key, "Node not serving yet"));
      return;
    }
    logger.log("Received ReadRequest for key=" + msg.key);

    if (writeLocks.containsKey(msg.key)) {
      sendWithRandomDelay(getSender(), new OperationFailed(msg.key, "Another write in progress for this key"));
      logger.log("Rejected ReadRequest for key=" + msg.key + " due to ongoing write");
      return;
    }

    // Start version gathering phase
    List<ActorRef> responsibleNodes = getResponsibleNodes(msg.key, nodes, nodeIdMap);
    for (ActorRef node : responsibleNodes) {
      logger.log("Notifying node " + nodeIdMap.get(node) + " for ReadRequest: key=" + msg.key + " client="
          + getSender().path().name());
      sendWithRandomDelay(node, new GetVersionRead(msg.key, getSelf(), getSender()));
    }

    // Initialize coordinator state + start timeout
    ReadRequestState state = new ReadRequestState(msg.key, getSender());
    readRequestList.put(msg.key + " " + getSender().path().name(), state);
    logger.log("Started read coordinator for key= (" + msg.key + " " + getSender().path().name() + ")");
  }

  /**
   * Handles client Write: acts as coordinator, gathers versions, computes new
   * version, writes to replicas with timeout.
   */
  private void onWriteRequest(WriteRequest msg) {
    if (!serving) {
      sendWithRandomDelay(getSender(), new OperationFailed(msg.key, msg.value, "Node not serving yet"));
      return;
    }
    logger.log("Received WriteRequest for key=" + msg.key + " value=\"" + msg.value + "\"");

    // Check if some node is already coordinating a write for this key
    if (writeLocks.containsKey(msg.key)) {
      sendWithRandomDelay(getSender(), new OperationFailed(msg.key, msg.value, "Another write in progress for this key"));
      logger.log("Rejected WriteRequest for key=" + msg.key + " due to ongoing write");
      return;
    }

    // Start version gathering phase
    List<ActorRef> responsibleNodes = getResponsibleNodes(msg.key, nodes, nodeIdMap);
    for (ActorRef node : responsibleNodes) {
      logger.log("Notifying node " + nodeIdMap.get(node) + " for WriteRequest: key=" + msg.key + " value=\"" + msg.value
          + "\"");
      sendWithRandomDelay(node, new GetVersionWrite(msg.key, getSelf()));
    }

    // Initialize coordinator state + start timeout
    WriteRequestState state = new WriteRequestState(msg.key, msg.value, getSender(), responsibleNodes);
    writeRequestList.put(msg.key, state);
  }

  /** Replies with the current version/value of a key to the requester. */
  private void onGetVersionRead(GetVersionRead msg) {
    if (writeLocks.containsKey(msg.key)) {
      // Write in progress, reject
      logger.log("Rejected GetVersionRead for key=" + msg.key + " due to ongoing write");
      sendWithRandomDelay(msg.coordinator, new VersionReadResponse(msg.key, -1, "", msg.requester));
      return;
    }

    int version = 0;
    String value = null;
    DataItem item = store.get(msg.key);
    if (item != null) {
      version = item.version;
      value = item.value;
    }
    logger.log("Replied GetVersionRead for key=" + msg.key + " v=" + version);
    sendWithRandomDelay(msg.coordinator, new VersionReadResponse(msg.key, version, value, msg.requester));
  }

  private void onVersionReadResponse(VersionReadResponse msg) {
    if (msg.version == -1) {
      // The sender node is busy with another write
      if (mode == Mode.JOINING || mode == Mode.RECOVERING) {
        logger.logError("Maintenance VersionReadResponse for key=" + msg.key
            + " rejected by node " + nodeIdMap.get(getSender()) + " due to ongoing write");
      }
      logger.log("Ignored VersionReadResponse for key=" + msg.key + " due to ongoing write");
      return;
    }

    ReadRequestState state = readRequestList.get(msg.key + " " + msg.requester.path().name());
    if (state == null) {
      // No active read for this key+client (quorum and request already completed)
      return;
    }

    // Register response
    state.versionReplies.put(getSender(), new VersionResponse(msg.key, msg.version, msg.value, getSender()));

    String versionsSummary = state.versionReplies.entrySet().stream()
        .map(e -> nodeIdMap.get(e.getKey()) + "=" + e.getValue().version)
        .sorted()
        .reduce((a, b) -> a + ", " + b)
        .orElse("");
    logger.log("(Read) GetVersionRead responses: " + versionsSummary);

    if (state.versionReplies.size() >= R) {
      // Quorum reached, get the response with the highest version
      VersionResponse maxVersionResponse = state.versionReplies.values().stream()
          .max(Comparator.comparingInt(v -> v.version))
          .get();

      // Update local store if the version is newer
      DataItem current = store.get(msg.key);
      if (current != null && maxVersionResponse.version > current.version) {
        store.put(msg.key, new DataItem(maxVersionResponse.version, maxVersionResponse.value));
        logger.log("(Read) Updated local store key=" + msg.key + " v=" + maxVersionResponse.version);
      }

      // Respond to client if this is an external read
      if (mode == Mode.IDLE && state.client != getSelf()) {
        logger.log("(Read) Returning value v=" + maxVersionResponse.version + " to client");
        sendWithRandomDelay(state.client,
            new Client.ReadResponse(msg.key, maxVersionResponse.value, maxVersionResponse.version));
      }

      // Cleanup state and timeout
      if (state.timeout != null && !state.timeout.isCancelled()) {
        state.timeout.cancel();
      }
      readRequestList.remove(msg.key + " " + msg.requester.path().name());

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

  private void onGetVersionWrite(GetVersionWrite msg) {
    if (writeLocks.containsKey(msg.key)) {
      // Write in progress, reject
      sendWithRandomDelay(msg.requester, new VersionWriteResponse(msg.key, -1, ""));
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
    sendWithRandomDelay(msg.requester, new VersionWriteResponse(msg.key, version, value));
  }

  private void onVersionWriteResponse(VersionWriteResponse msg) {
    if (msg.version == -1) {
      // The sender node is busy with another write
      logger.log("Ignored VersionWriteResponse for key=" + msg.key + " due to ongoing write");
      return;
    }

    WriteRequestState state = writeRequestList.get(msg.key);
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
    logger.log("(Write) GetVersionWrite responses: " + versionsSummary);

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
        sendWithRandomDelay(node, new UpdateValue(state.key, state.newVersion, state.newValue));
      }
      sendWithRandomDelay(state.client, new Client.WriteResponse(state.key, state.newValue, state.newVersion));

      // Cleanup state, timeout and lock
      if (state.timeout != null && !state.timeout.isCancelled()) {
        state.timeout.cancel();
      }
      writeLocks.remove(state.key);
      writeRequestList.remove(state.key);
    }
  }

  /**
   * Handles actual value update on replicas, checking version and write lock.
   */
  private void onUpdateValue(UpdateValue msg) {
    if (writeLocks.containsKey(msg.key) && writeLocks.get(msg.key) != getSender()) {
      // This node should not receive UpdateValue from a node that does not hold the lock for this key
      logger.log("Rejected UpdateValue for key=" + msg.key + " due to lock held by another node");
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
  private void onWriteTimeout(WriteTimeout msg) {
    WriteRequestState state = writeRequestList.get(msg.key);
    if (state == null) {
      return; // Already completed
    }

    writeLocks.remove(state.key);
    writeRequestList.remove(state.key);
    if (state.timeout != null && !state.timeout.isCancelled()) {
      state.timeout.cancel();
    }
    if (state.versionReplies.size() < W) {
      String detail = msg.detail != null ? msg.detail : "Write quorum not reached";
      logger.log("WriteTimeout: " + detail);
      for (ActorRef node : state.responsibleNodes) {
        if (node != getSelf()) {
          sendWithRandomDelay(node, new OperationFailed(state.key, state.newValue, detail));
        }
      }
      sendWithRandomDelay(state.client, new OperationFailed(state.key, state.newValue, detail));
    }
  }

  /**
   * Handles read timeout: if quorum R not reached, fail client and clean
   * coordinator state.
   */
  private void onReadTimeout(ReadTimeout msg) {
    ReadRequestState state = readRequestList.get(msg.key + " " + msg.requester.path().name());
    if (state == null) {
      return; // Already completed
    }

    readRequestList.remove(msg.key + " " + msg.requester.path().name());
    if (state.timeout != null && !state.timeout.isCancelled()) {
      state.timeout.cancel();
    }
    if (state.versionReplies.size() < R) {
      String detail = msg.detail != null ? msg.detail : "Read quorum not reached";
      logger.log("ReadTimeout: " + detail);
      boolean maintenanceRead = state.client == getSelf();
      if (maintenanceRead && (mode == Mode.JOINING || mode == Mode.RECOVERING)) {
        failMembershipAction(detail);
      } else {
        sendWithRandomDelay(state.client, new OperationFailed(state.key, detail));
      }
    }
  }

  private void onJoinTimeout(JoinTimeout msg) {
    if (mode != Mode.JOINING) {
      logger.logError("JoinTimeout: not in JOINING mode");
      return; // Already joined or not joining
    }
    String reason = (msg.detail == null || msg.detail.isEmpty()) ? "Join timeout" : msg.detail;
    failMembershipAction(reason);
  }

  private void onRecoverTimeout(RecoverTimeout msg) {
    if (mode != Mode.RECOVERING) {
      logger.logError("RecoverTimeout: not in RECOVERING mode");
      return; // Already recovered or not recovering
    }
    String reason = (msg.detail == null || msg.detail.isEmpty()) ? "Recover timeout" : msg.detail;
    failMembershipAction(reason);
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
      Map<ActorRef, Integer> tempNodeIdMap = new HashMap<>(this.nodeIdMap);
      tempNodes.remove(getSelf());
      tempNodeIdMap.remove(getSelf());

      for (Map.Entry<Integer, DataItem> e : new ArrayList<>(store.entrySet())) {
        int key = e.getKey();
        DataItem di = e.getValue();
        List<ActorRef> responsible = getResponsibleNodes(key, tempNodes, tempNodeIdMap);

        for (ActorRef target : responsible) {
          sendWithRandomDelay(target, new TransferData(key, di.version, di.value));
        }
      }

      // Announce leave to other nodes
      for (ActorRef p : tempNodes) {
        sendWithRandomDelay(p, new AnnounceLeave(getSelf(), nodeId));
      }
      testManager.tell(new TestManager.NodeActionResponse("leave", nodeId, true), getSelf());
      //sendWithRandomDelay(testManager, new TestManager.NodeActionResponse("leave", nodeId, true));
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
      //sendWithRandomDelay(testManager, new TestManager.NodeActionResponse("crash", nodeId, true));
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
      clearPendingTransfers();
      sendWithRandomDelay(msg.bootstrap, new GetPeers());

      scheduleMembershipTimeout("waiting bootstrap peer response for RECOVER");
    }

    else {
      logger.logError("Unknown NodeAction: " + msg.action);
    }
  }

  /** Returns the current membership view to the requester (bootstrap helper). */
  private void onGetPeers(GetPeers msg) {
    // Answer with current nodes and their IDs
    sendWithRandomDelay(getSender(), new PeerResponse(new ArrayList<>(nodes), new HashMap<>(nodeIdMap)));
  }

  /**
   * Handles a PeerResponse from bootstrap: complete JOIN/RECOVERY setup or
   * refresh
   * view.
   */
  private void onPeerResponse(PeerResponse msg) {
    nodes.clear();
    nodes.addAll(msg.nodes);
    nodeIdMap.putAll(msg.nodeIds);

    List<ActorRef> peersWithoutSelf = peersExcludingSelf();

    if (mode == Mode.JOINING) {
      if (peersWithoutSelf.isEmpty()) {
        failMembershipAction("No peers available for JOIN");
        return;
      }

      ActorRef successor = RingUtils.findSuccessorOfId(nodeId, peersWithoutSelf, nodeIdMap);

      if (successor != null) {
        logger.log("JOIN: requesting items from " + nodeIdMap.get(successor));
        clearPendingTransfers();
        String ctx = newTransferContext("join-successor-" + nodeIdMap.get(successor));
        registerPendingTransfer(ctx, "successor " + nodeIdMap.get(successor));
        sendWithRandomDelay(successor, new ItemRequest(getSelf(), nodeId, ctx));
        scheduleMembershipTimeout("waiting data transfer from " + describePendingTransfers());
      } else {
        failMembershipAction("No successor available for JOIN");
        return;
      }
    }

    else if (mode == Mode.RECOVERING) {
      receivedResponseForRecovery = 0;
      clearPendingTransfers();

      if (peersWithoutSelf.isEmpty()) {
        failMembershipAction("No peers available for RECOVER");
        return;
      }

      // pick immediate successor and predecessor
      ActorRef successor = RingUtils.findSuccessorOfId(nodeId, peersWithoutSelf, nodeIdMap);
      ActorRef predecessor = RingUtils.findPredecessorOfId(nodeId, peersWithoutSelf, nodeIdMap);

      // send recovery requests
      if (successor != null && predecessor != null) {
        logger.log("RECOVER: requesting items from successor " + nodeIdMap.get(successor));
        String successorCtx = newTransferContext("recover-successor-" + nodeIdMap.get(successor));
        registerPendingTransfer(successorCtx, "successor " + nodeIdMap.get(successor));
        sendWithRandomDelay(successor, new ItemRequest(getSelf(), nodeId, successorCtx));

        logger.log("RECOVER: requesting items from predecessor " + nodeIdMap.get(predecessor));
        String predecessorCtx = newTransferContext("recover-predecessor-" + nodeIdMap.get(predecessor));
        registerPendingTransfer(predecessorCtx, "predecessor " + nodeIdMap.get(predecessor));
        sendWithRandomDelay(predecessor, new ItemRequest(getSelf(), nodeId, predecessorCtx));
      } else {
        failMembershipAction("Missing neighbor for RECOVER (successor/predecessor)");
        return;
      }

      if (!pendingDataTransferDescriptions.isEmpty()) {
        scheduleMembershipTimeout("waiting data transfer from " + describePendingTransfers());
      }
    }

    else {
      logger.log("Received PeerResponse in mode " + mode + ", ignoring.");
      return;
    }
  }

  private void onItemRequest(ItemRequest msg) {
    Map<Integer, DataItem> batch = new LinkedHashMap<>();
    List<ActorRef> tempNodes = new ArrayList<>(this.nodes);
    Map<ActorRef, Integer> tempNodeIdMap = new HashMap<>(this.nodeIdMap);
    tempNodes.add(msg.targetNode);
    tempNodeIdMap.put(msg.targetNode, msg.targetNodeId);

    for (Map.Entry<Integer, DataItem> e : store.entrySet()) {
      int key = e.getKey();
      List<ActorRef> resp = getResponsibleNodes(key, tempNodes, tempNodeIdMap);
      if (resp.contains(msg.targetNode)) {
        batch.put(key, e.getValue());
      }
    }

    logger.log(mode + ": sending " + batch.size() + " items to " + nodeIdMap.get(msg.targetNode));
    sendWithRandomDelay(msg.targetNode, new DataItemsBatch(batch, msg.context));
  }

  /**
   * Processes a batch of items during JOIN/RECOVERY (install, update view,
   * maintenance) or generic transfer.
   */
  private void onDataItemsBatch(DataItemsBatch msg) {
    if ((mode == Mode.JOINING || mode == Mode.RECOVERING) && msg.context != null) {
      if (pendingDataTransferDescriptions.remove(msg.context) != null) {
        if (!pendingDataTransferDescriptions.isEmpty()) {
          scheduleMembershipTimeout("waiting data transfer from " + describePendingTransfers());
        } else {
          cancelMembershipTimeout();
        }
      }
    }

    // TODO: this message is used in JOIN and RECOVER operations, if the requesting
    // nodes reach this message means that bootstrap and the successor/predecessor
    // are alive and not crashed. So, the timeout can be cancelled.
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
    if ("Write quorum not reached".equals(msg.reason)) {
      if (writeLocks.remove(msg.key) != null) {
        logger.log("OperationFailed: write quorum not reached for key=" + msg.key);
      }
      logger.log("OperationFailed: no lock found for key=" + msg.key + " (other coordinating node reached quorum)");
    } else {
      logger.logError("OperationFailed: " + msg.reason + " for key=" + msg.key);
    }
  }

  private void onPrintStore(PrintStore msg) {
    sendWithRandomDelay(getSender(), new TestManager.PrintStoreResponse(nodeId, new LinkedHashMap<>(store)));
  }

  private void sendWithRandomDelay(ActorRef target, Object message) {
    long delay = ThreadLocalRandom.current()
        .nextLong(MIN_PROP_DELAY_MS, MAX_PROP_DELAY_MS + 1);
    getContext().system().scheduler().scheduleOnce(
        Duration.create(delay, TimeUnit.MILLISECONDS),
        target,
        message,
        getContext().dispatcher(),
        getSelf());
  }

  // =====================
  // Actor Lifecycle: behaviors
  // =====================

  // Behavior operativo: gestisce tutte le richieste
  private Receive activeReceive() {
    return receiveBuilder()
        .match(JoinGroupMsg.class, this::onJoinGroupMsg)

        .match(ReadRequest.class, this::onReadRequest)
        .match(WriteRequest.class, this::onWriteRequest)

        .match(GetVersionRead.class, this::onGetVersionRead)
        .match(VersionReadResponse.class, this::onVersionReadResponse)

        .match(GetVersionWrite.class, this::onGetVersionWrite)
        .match(VersionWriteResponse.class, this::onVersionWriteResponse)

        .match(UpdateValue.class, this::onUpdateValue)

        .match(WriteTimeout.class, this::onWriteTimeout)
        .match(ReadTimeout.class, this::onReadTimeout)

        // Membership & transfer
        .match(NodeAction.class, this::onNodeAction)
        .match(GetPeers.class, this::onGetPeers)

        .match(ItemRequest.class, this::onItemRequest)
        .match(AnnounceJoin.class, this::onAnnounceJoin)
        .match(AnnounceLeave.class, this::onAnnounceLeave)
        .match(TransferData.class, this::onTransferData)
        .match(OperationFailed.class, this::onOperationFailed)
        .match(PrintStore.class, this::onPrintStore)
        .build();
  }

  // Behavior di JOIN/RECOVERY: nessun Write/Get; gestisce bootstrap e catch-up
  private Receive joiningRecoveringReceive() {
    return receiveBuilder()
        .match(JoinTimeout.class, this::onJoinTimeout)
        .match(RecoverTimeout.class, this::onRecoverTimeout)

        .match(PeerResponse.class, this::onPeerResponse)
        .match(DataItemsBatch.class, this::onDataItemsBatch)
        .match(VersionReadResponse.class, this::onVersionReadResponse)

        .match(ReadTimeout.class, this::onReadTimeout)

        .match(ReadRequest.class, this::onReadRequest)
        .match(WriteRequest.class, this::onWriteRequest)
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