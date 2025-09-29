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
import akka.actor.Props;
import it.unitn.MarsSystemManager.CrashNode;
import it.unitn.MarsSystemManager.JoinNode;
import it.unitn.MarsSystemManager.LeaveNode;
import it.unitn.MarsSystemManager.RecoverNode;
import it.unitn.MarsSystemManager.RingUtils;
import scala.concurrent.duration.Duration;

public class Node extends AbstractActor {

  private final Logger logger;

  private final int nodeId;
  private List<ActorRef> nodes = new ArrayList<>();
  private final Map<ActorRef, Integer> nodeIdMap = new HashMap<>(); // Mappa ActorRef -> nodeId dei peer conosciuti
  private boolean joining;
  private final ActorRef manager;

  private final Map<Integer, DataItem> store = new HashMap<>();

  private final int N; // Replication factor (numero di repliche)
  private final int R; // Read quorum size
  private final int W; // Write quorum size

  private int receivedResponseForRecovery = 0;

  // Timeout duration in seconds
  private static final int TIMEOUT_SECONDS = 5;

  /** Constructs a node with its id and quorum parameters (N,R,W). */
  public Node(int nodeId, int N, int R, int W, boolean joining, ActorRef manager) {
    this.nodeId = nodeId;
    this.N = N;
    this.R = R;
    this.W = W;

    this.joining = joining;
    this.manager = manager;

    logger = new Logger("Node " + nodeId);
  }

  /** Akka factory method to create Props for this actor. */
  public static Props props(int nodeId, int N, int R, int W, boolean joining, ActorRef manager) {
    return Props.create(Node.class, () -> new Node(nodeId, N, R, W, joining, manager));
  }

  @Override
  public void preStart() {
    // joining nodes contact the manager
    if (joining) {
      mode = Mode.JOINING;
      serving = false;
      getContext().become(joiningRecoveringReceive());
      manager.tell(new JoinNode(nodeId), getSelf());
    }
  }

  // Stato di servizio: finché non ha completato join/recovery, non serve
  // richieste client
  private boolean serving = true; // nodi creati all’avvio servono subito; i nuovi nodi metteranno serving=false
                                  // finché pronti

  // Modalità coordinatore per join/recovery
  private enum Mode {
    IDLE, JOINING, RECOVERING, CRASHED
  }

  private Mode mode = Mode.IDLE;

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

  // =====================
  // Message Classes
  // =====================

  // All message classes: InitPeers, UpdateRequest, GetRequest,
  // GetVersionRequest,
  // GetVersionResponse, UpdateValue, UpdateAck, WriteTimeout, ReadTimeout,
  // OperationFailed

  public static class JoinGroupMsg implements Serializable {
    public final List<ActorRef> nodes; // an array of group members
    public final Map<ActorRef, Integer> nodeIdMap;

    public JoinGroupMsg(List<ActorRef> nodes, Map<ActorRef, Integer> nodeIdMap) {
      this.nodes = Collections.unmodifiableList(new ArrayList<>(nodes));
      this.nodeIdMap = Collections.unmodifiableMap(new HashMap<>(nodeIdMap));
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

  public static class GetRequest implements Serializable {
    public final int key;

    public GetRequest(int key) {
      this.key = key;
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

  public static class UpdateAck implements Serializable {
    public final int key;
    public final int version;
    public final ActorRef responder;

    public UpdateAck(int key, int version, ActorRef responder) {
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

  // ===== Membership & transfer messages (aggiunte) =====

  public static class NodeAction implements Serializable {
    public final String action; // "leave", "crash", "recover"

    public NodeAction(String action) {
      this.action = action;
    }
  }

  public static class AllowJoin implements Serializable {
    public final int nodeId;
    public final ActorRef bootstrap;

    public AllowJoin(int nodeId, ActorRef bootstrap) {
      this.nodeId = nodeId;
      this.bootstrap = bootstrap;
    }
  }

  public static class AllowCrash implements Serializable {
  }

  public static class AllowRecover implements Serializable {
    public final ActorRef bootstrap;

    public AllowRecover(ActorRef bootstrap) {
      this.bootstrap = bootstrap;
    }
  }

  public static class ViewChangeMsg implements Serializable {
    public final Integer viewId;
    public final Set<ActorRef> proposedView;

    public ViewChangeMsg(Integer viewId, Set<ActorRef> proposedView) {
      this.viewId = viewId;
      this.proposedView = Collections.unmodifiableSet(new HashSet<>(proposedView));
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

  public static class AllowLeave implements Serializable {
  }

  public static class RecoverNetwork implements Serializable {
    public final ActorRef bootstrap;

    public RecoverNetwork(ActorRef bootstrap) {
      this.bootstrap = bootstrap;
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

  public static class LeaveNetwork implements Serializable {
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
  // Coordinator State Classes
  // =====================

  private static class CoordinatorState {
    public final int key;
    public final String newValue;
    public final ActorRef client;
    public final List<ActorRef> responsibleNodes;

    public final Map<ActorRef, GetVersionResponse> versionReplies = new HashMap<>();
    public final Map<ActorRef, UpdateAck> UpdateAcks = new HashMap<>();

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
  // Message Handlers
  // =====================

  private void onJoinGroupMsg(JoinGroupMsg msg) {
    this.nodes = new ArrayList<>(msg.nodes);
    this.nodeIdMap.clear();
    this.nodeIdMap.putAll(msg.nodeIdMap);
    this.serving = true;
  }

  /**
   * Handles client Put: acts as coordinator, gathers versions, computes new
   * version, writes to replicas with timeout.
   */
  private void onUpdateRequest(UpdateRequest msg) {
    if (!serving) {
      getSender().tell(new OperationFailed(msg.key, "Node not serving yet"), getSelf());
      return;
    }
    logger.log("Received UpdateRequest for key=" + msg.key + " value=\"" + msg.value + "\"");

    List<ActorRef> responsibleNodes = getResponsibleNodes(msg.key, nodes);
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

    List<ActorRef> responsibleNodes = getResponsibleNodes(msg.key, nodes);
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

  /**
   * Handles version replies for both write and read coordinations, driving quorum
   * logic.
   */
  private void onGetVersionResponse(GetVersionResponse msg) {
    CoordinatorState writeState = activeCoordinators.get(msg.key);
    if (writeState != null) {
      writeState.versionReplies.put(msg.responder, msg);

      String versionsSummary = writeState.versionReplies.entrySet().stream()
          .map(e -> nodeIdMap.get(e.getKey()) + "=" + e.getValue().version)
          .sorted()
          .reduce((a, b) -> a + ", " + b)
          .orElse("");

      logger.log("(Write) Version responses: " + versionsSummary);
      if (writeState.versionReplies.size() >= W && writeState.newVersion == 0) {
        int maxVersion = writeState.versionReplies.values().stream()
            .mapToInt(v -> v.version)
            .max()
            .orElse(0);
        writeState.newVersion = maxVersion + 1;

        logger.log("Computed new version " + writeState.newVersion);

        for (ActorRef node : writeState.responsibleNodes) {
          node.tell(new UpdateValue(writeState.key, writeState.newVersion, writeState.newValue), getSelf());
        }
      }
      return;
    }

    ReadCoordinatorState readState = activeReadCoordinators.get(msg.key);
    if (readState != null) {
      readState.versionReplies.put(msg.responder, msg);

      String versionsSummary = readState.versionReplies.entrySet().stream()
          .map(e -> nodeIdMap.get(e.getKey()) + "=" + e.getValue().version)
          .sorted()
          .reduce((a, b) -> a + ", " + b)
          .orElse("");

      logger.log("(Read) Version responses: " + versionsSummary);

      if (readState.versionReplies.size() >= R) {
        // Get the response with the highest version
        GetVersionResponse maxVersionResponse = readState.versionReplies.values().stream()
            .max(Comparator.comparingInt(v -> v.version))
            .get(); // safe because map has at least R entries

        // Update local store if the version is newer
        DataItem current = store.get(msg.key);
        if (current == null || maxVersionResponse.version > current.version) {
          store.put(msg.key, new DataItem(maxVersionResponse.version, maxVersionResponse.value));
          logger.log("(Read) Updated local store key=" + msg.key + " v=" + maxVersionResponse.version);
        }

        // Respond to client if this is an external read
        if (readState.client != getSelf()) {
          logger.log("(Read) Returning value v=" + maxVersionResponse.version + " to client");
          readState.client.tell(maxVersionResponse, getSelf());
        }

        // Cleanup
        activeReadCoordinators.remove(msg.key);
      }
    }
  }

  /**
   * Collects UpdateAck messages and replies to client once W acks are gathered.
   */
  private void onUpdateAck(UpdateAck msg) {
    CoordinatorState state = activeCoordinators.get(msg.key);
    if (state == null)
      return;

    state.UpdateAcks.put(msg.responder, msg);
    logger.log("Received UpdateAck from " + nodeIdMap.get(msg.responder));

    if (state.UpdateAcks.size() >= W) {
      state.client.tell(new UpdateAck(state.key, state.newVersion, getSelf()), getSelf());
      logger.log("Write quorum reached for key " + state.key + ", replying success to client");

      activeCoordinators.remove(state.key);
    }
  }

  /** Replies with the current version/value of a key to the requester. */
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

  /** Applies a replica write if not older and acknowledges with UpdateAck. */
  private void onUpdateValue(UpdateValue msg) {
    DataItem current = store.get(msg.key);
    if (current == null || msg.version >= current.version) {
      store.put(msg.key, new DataItem(msg.version, msg.value));
      logger.log(
          "Stored key=" + msg.key + " version=" + msg.version + " value=\"" + msg.value + "\"");
    } else {
      logger.log("Ignored older UpdateValue for key=" + msg.key);
    }
    getSender().tell(new UpdateAck(msg.key, msg.version, getSelf()), getSelf());
  }

  /**
   * Handles write timeout: if quorum W not reached, fail client and clean
   * coordinator state.
   */
  private void onWriteTimeout(WriteTimeout msg) {
    List<Integer> toRemove = new ArrayList<>();
    for (Map.Entry<Integer, CoordinatorState> entry : activeCoordinators.entrySet()) {
      CoordinatorState state = entry.getValue();
      if (state.UpdateAcks.size() < W) {
        logger.log("WriteTimeout for key " + state.key + ": quorum not reached");
        state.client.tell(new OperationFailed(state.key, "Write quorum not reached"), getSelf());
        toRemove.add(entry.getKey());
      }
    }
    toRemove.forEach(activeCoordinators::remove);
  }

  /**
   * Handles read timeout: if quorum R not reached, fail client and clean
   * coordinator state.
   */
  private void onReadTimeout(ReadTimeout msg) {
    List<Integer> toRemove = new ArrayList<>();
    for (Map.Entry<Integer, ReadCoordinatorState> entry : activeReadCoordinators.entrySet()) {
      ReadCoordinatorState state = entry.getValue();
      if (state.versionReplies.size() < R) {
        logger.log("ReadTimeout for key " + state.key + ": quorum not reached");
        state.client.tell(new OperationFailed(state.key, "Read quorum not reached"), getSelf());
        toRemove.add(entry.getKey());
      }
    }
    toRemove.forEach(activeReadCoordinators::remove);
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
      logger.log("Leaving network via manager");
      manager.tell(new LeaveNode(nodeId), getSelf());
    }

    else if (msg.action.equals("crash")) {
      if (mode != Mode.IDLE) {
        logger.logError("Cannot crash while not in IDLE mode");
        return;
      }
      this.mode = Mode.CRASHED;
      this.serving = false;
      logger.log("Crashing node");
      manager.tell(new CrashNode(nodeId), getSelf());
    }

    else if (msg.action.equals("recover")) {
      if (mode != Mode.CRASHED) {
        logger.logError("Cannot recover while not in CRASHED mode");
        return;
      }
      this.mode = Mode.RECOVERING;
      this.serving = false;
      logger.log("Starting RECOVERY via manager");
      manager.tell(new RecoverNode(nodeId), getSelf());
    }

    else {
      logger.logError("Unknown NodeAction: " + msg.action);
    }
  }

  private void onAllowJoin(AllowJoin msg) {
    if (msg.bootstrap == getSelf()) {
      mode = Mode.IDLE;
      serving = true;
      getContext().become(activeReceive());
      logger.logError("A node cannot bootstrap to itself!");
    } else {
      msg.bootstrap.tell(new GetPeersRequest(), getSelf());
      logger.log("Asked bootstrap node " + nodeIdMap.get(msg.bootstrap) + " for nodes");
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
    if (mode == Mode.JOINING) {
      nodes = new ArrayList<>(msg.nodes);
      nodeIdMap.putAll(msg.nodeIds);
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

  private void onAllowLeave(AllowLeave msg) {
    if (mode != Mode.CRASHED) {
      mode = Mode.IDLE;
      serving = true;
      getContext().become(activeReceive());
      logger.logError("Received AllowLeave while not in CRASHED mode");
      return;
    }
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

    manager.tell(new MarsSystemManager.NodeLeft(nodeId, getSelf()), getSelf());
    getContext().stop(getSelf()); // stop the actor
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
      // 1) Salva item
      store.clear();
      for (Map.Entry<Integer, DataItem> e : msg.items.entrySet()) {
        DataItem cur = store.get(e.getKey());
        if (cur == null || e.getValue().version >= cur.version) {
          store.put(e.getKey(), e.getValue());
        }
      }

      // 2) Maintenance read per ogni key per allineare versione quorum
      for (Integer key : msg.items.keySet()) {
        ReadCoordinatorState state = new ReadCoordinatorState(key, getSelf());
        activeReadCoordinators.put(key, state);
        for (ActorRef node : getResponsibleNodes(key, nodes)) {
          node.tell(new GetVersionRequest(key, getSelf()), getSelf());
        }
      }

      // 3) Aggiorna vista
      nodes.add(getSelf());
      nodeIdMap.put(getSelf(), nodeId);

      // 4) Annuncia il join agli altri
      for (ActorRef p : nodes) {
        if (p != getSelf()) {
          p.tell(new AnnounceJoin(getSelf(), nodeId), getSelf());
        }
      }
      manager.tell(new MarsSystemManager.NodeJoined(nodeId, getSelf()), getSelf());

      this.serving = true;
      this.mode = Mode.IDLE;
      getContext().become(activeReceive());
      logger.log("JOIN completed and serving.");

    } else if (mode == Mode.RECOVERING) {
      // 1) Salva item recuperati
      for (Map.Entry<Integer, DataItem> e : msg.items.entrySet()) {
        DataItem cur = store.get(e.getKey());
        if (cur == null || e.getValue().version >= cur.version) {
          store.put(e.getKey(), e.getValue());
        }
      }
      receivedResponseForRecovery++;

      // Controlla se abbiamo ricevuto entrambe le risposte
      if (receivedResponseForRecovery >= 2) {
        // 2) Pruning rispetto alla vista corrente
        pruneKeysNotResponsible();

        // 3) Maintenance read su item recuperati
        for (Integer key : msg.items.keySet()) {
          ReadCoordinatorState state = new ReadCoordinatorState(key, getSelf());
          activeReadCoordinators.put(key, state);
          for (ActorRef node : getResponsibleNodes(key, nodes)) {
            node.tell(new GetVersionRequest(key, getSelf()), getSelf());
          }
        }

        // 4) Aggiorna stato del nodo
        this.serving = true;
        this.mode = Mode.IDLE;
        manager.tell(new MarsSystemManager.NodeRecovered(nodeId, getSelf()), getSelf());
        getContext().become(activeReceive());
        logger.log("RECOVERY completed and serving.");
      } else {
        logger.log("RECOVER: waiting for more responses (" + receivedResponseForRecovery + "/2)");
      }
    } else {
      // Batch generico: salva
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
      // 1) Update view
      nodes.add(msg.node);
      nodeIdMap.put(msg.node, msg.nodeId);

      // 2) Remove keys that this node is no longer responsible for
      pruneKeysNotResponsible();
      logger.log("AnnounceJoin: added " + nodeIdMap.get(msg.node) + " with backfill");
    }
  }

  /** Removes a leaving node from the view and prunes non-responsible keys. */
  private void onAnnounceLeave(AnnounceLeave msg) {
    if (nodeIdMap.containsKey(msg.node)) {
      nodes.remove(msg.node);
      nodeIdMap.remove(msg.node);
      // Pruning/ribilanciamento: rimuovi ciò che non è più di competenza
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

  /**
   * Simulates a temporary crash: stop serving and ignore all messages until
   * recovery.
   */
  private void onAllowCrash(AllowCrash msg) {
    if (mode != Mode.CRASHED) {
      mode = Mode.IDLE;
      serving = true;
      getContext().become(activeReceive());
      logger.logError("Received AllowCrash while not in CRASHED mode");
      return;
    }
    logger.log("Simulating CRASH (temporarily unavailable)");
    getContext().become(crashedReceive());
    manager.tell(new MarsSystemManager.NodeCrashed(nodeId, getSelf()), getSelf());
  }

  private void onAllowRecover(AllowRecover msg) {
    if (mode != Mode.RECOVERING) {
      mode = Mode.CRASHED;
      serving = false;
      getContext().become(crashedReceive());
      logger.logError("Received AllowRecover while not in RECOVERING mode");
    }

    logger.log("Allowed to RECOVER via " + nodeIdMap.get(msg.bootstrap));
    getContext().become(joiningRecoveringReceive());
    msg.bootstrap.tell(new GetPeersRequest(), getSelf());
  }

  // =====================
  // Actor Lifecycle: behaviors
  // =====================

  // Behavior operativo: gestisce tutte le richieste
  private Receive activeReceive() {
    return receiveBuilder()
        .match(JoinGroupMsg.class, this::onJoinGroupMsg)

        .match(UpdateRequest.class, this::onUpdateRequest)
        .match(UpdateValue.class, this::onUpdateValue)
        .match(UpdateAck.class, this::onUpdateAck)

        .match(GetRequest.class, this::onGetRequest)

        .match(GetVersionRequest.class, this::onGetVersionRequest)
        .match(GetVersionResponse.class, this::onGetVersionResponse)

        .match(WriteTimeout.class, this::onWriteTimeout)
        .match(ReadTimeout.class, this::onReadTimeout)
        // Membership & transfer
        .match(NodeAction.class, this::onNodeAction)

        .match(GetPeersRequest.class, this::onGetPeersRequest)

        .match(AllowLeave.class, this::onAllowLeave)
        .match(AllowCrash.class, this::onAllowCrash)

        .match(ItemRequest.class, this::onItemRequest)
        .match(AnnounceJoin.class, this::onAnnounceJoin)
        .match(AnnounceLeave.class, this::onAnnounceLeave)
        .match(TransferData.class, this::onTransferData)
        .build();
  }

  // Behavior di JOIN/RECOVERY: nessun Put/Get; gestisce bootstrap e catch-up
  private Receive joiningRecoveringReceive() {
    return receiveBuilder()
        .match(AllowJoin.class, this::onAllowJoin)
        .match(PeerSet.class, this::onPeerSet)
        .match(DataItemsBatch.class, this::onDataItemsBatch)
        .match(GetVersionRequest.class, this::onGetVersionRequest)
        .match(GetVersionResponse.class, this::onGetVersionResponse)

        .match(ReadTimeout.class, this::onReadTimeout)
        
        .match(GetRequest.class, this::onGetRequest)
        .match(UpdateRequest.class, this::onUpdateRequest)
        .build();
  }

  // Behavior di CRASH: ignora tutto, tranne RecoverNetwork
  private Receive crashedReceive() {
    return receiveBuilder()
        .match(AllowRecover.class, this::onAllowRecover)
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