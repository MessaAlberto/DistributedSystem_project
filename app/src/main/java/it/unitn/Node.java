package it.unitn;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
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

  private final int N; // Replication factor (numero di repliche)
  private final int R; // Read quorum size
  private final int W; // Write quorum size

  private final Map<ActorRef, Integer> nodeIdMap = new HashMap<>(); // Mappa ActorRef -> nodeId dei peer conosciuti

  // Timeout duration in seconds
  private static final int TIMEOUT_SECONDS = 5;

  /** Constructs a node with its id and quorum parameters (N,R,W). */
  public Node(int nodeId, int N, int R, int W) {
    this.nodeId = nodeId;
    this.N = N;
    this.R = R;
    this.W = W;
  }

  /** Akka factory method to create Props for this actor. */
  public static Props props(int nodeId, int N, int R, int W) {
    return Props.create(Node.class, () -> new Node(nodeId, N, R, W));
  }

  // Stato di servizio: finché non ha completato join/recovery, non serve richieste client
  private boolean serving = true; // nodi creati all’avvio servono subito; i nuovi nodi metteranno serving=false finché pronti

  // Modalità coordinatore per join/recovery
  private enum Mode { IDLE, JOINING, RECOVERING }
  private Mode mode = Mode.IDLE;

  private static class JoinContext {
    List<ActorRef> updatedPeers;
    Map<ActorRef, Integer> updatedMap;
    ActorRef successor;
    List<ActorRef> oldPeers;
  }
  private JoinContext joinCtx;

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

  /** Returns the N responsible replica nodes for a key (ring order, wrap-around). */
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

  /** Utility: returns peers sorted by their nodeId using the provided map. */
  private static List<ActorRef> sortPeersById(List<ActorRef> list, Map<ActorRef,Integer> map) {
    List<ActorRef> sorted = new ArrayList<>(list);
    Collections.sort(sorted, Comparator.comparingInt(map::get));
    return sorted;
  }

  /** Finds the successor peer for a given id (first peer with id >= given id, with wrap). */
  private ActorRef findSuccessorOfId(int id, List<ActorRef> list, Map<ActorRef,Integer> map) {
    List<ActorRef> sorted = sortPeersById(list, map);
    for (ActorRef p : sorted) {
      if (map.get(p) >= id) return p;
    }
    return sorted.isEmpty() ? null : sorted.get(0);
  }

  /** Finds the predecessor peer for a given id (largest id < given id, or last on wrap). */
  private ActorRef findPredecessorOfId(int id, List<ActorRef> list, Map<ActorRef,Integer> map) {
    List<ActorRef> sorted = sortPeersById(list, map);
    ActorRef pred = null;
    int predId = Integer.MIN_VALUE;
    for (ActorRef p : sorted) {
      int pid = map.get(p);
      if (pid < id && pid > predId) {
        predId = pid;
        pred = p;
      }
    }
    if (pred != null) return pred;
    return sorted.isEmpty() ? null : sorted.get(sorted.size()-1);
  }

  /** True if key lies in the ring interval (predId, selfId], considering wrap-around. */
  private boolean inRingIntervalOpenClosed(int key, int predId, int selfId) {
    // true se key ∈ (predId, selfId] con wrap-around
    if (predId < selfId) {
      return key > predId && key <= selfId;
    } else if (predId > selfId) {
      // wrap: (pred, MAX] U [MIN, self]
      return key > predId || key <= selfId;
    } else {
      // pred == self: range completo (caso limite)
      return true;
    }
  }
  
  /** Removes local keys for which this node is no longer responsible. */
  private void pruneKeysNotResponsible() {
    List<Integer> toRemove = new ArrayList<>();
    for (Map.Entry<Integer, DataItem> e : store.entrySet()) {
      int key = e.getKey();
      List<ActorRef> resp = getResponsibleNodes(key);
      if (!resp.contains(getSelf())) {
        toRemove.add(key);
      }
    }
    for (Integer k : toRemove) {
      store.remove(k);
      System.out.println("[Node " + nodeId + "] Pruned key=" + k + " (no longer responsible)");
    }
  }

  /** Returns a short, human-friendly actor name for logging. */
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

  // ===== Membership & transfer messages (aggiunte) =====
  public static class GetPeersRequest implements Serializable {}
  public static class PeerSet implements Serializable {
    public final List<ActorRef> peers;
    public final Map<ActorRef,Integer> nodeIds;
    public PeerSet(List<ActorRef> peers, Map<ActorRef,Integer> nodeIds) {
      this.peers = peers;
      this.nodeIds = nodeIds;
    }
  }
  public static class JoinNetwork implements Serializable {
    public final ActorRef bootstrap;
    public JoinNetwork(ActorRef bootstrap) { this.bootstrap = bootstrap; }
  }
  public static class RecoverNetwork implements Serializable {
    public final ActorRef bootstrap;
    public RecoverNetwork(ActorRef bootstrap) { this.bootstrap = bootstrap; }
  }
  public static class RequestItemsForJoin implements Serializable {
    public final int newNodeId;
    public final ActorRef newNode;
    public RequestItemsForJoin(int newNodeId, ActorRef newNode) {
      this.newNodeId = newNodeId; this.newNode = newNode;
    }
  }
  public static class RequestItemsForRecovery implements Serializable {
    public final int requesterId;
    public final ActorRef requester;
    public RequestItemsForRecovery(int requesterId, ActorRef requester) {
      this.requesterId = requesterId; this.requester = requester;
    }
  }
  public static class DataItemsBatch implements Serializable {
    public final Map<Integer, DataItem> items;
    public DataItemsBatch(Map<Integer, DataItem> items) { this.items = items; }
  }
  public static class AnnounceJoin implements Serializable {
    public final ActorRef node;
    public final int nodeId;
    public AnnounceJoin(ActorRef node, int nodeId) { this.node = node; this.nodeId = nodeId; }
  }
  public static class AnnounceLeave implements Serializable {
    public final ActorRef node;
    public final int nodeId;
    public AnnounceLeave(ActorRef node, int nodeId) { this.node = node; this.nodeId = nodeId; }
  }
  public static class LeaveNetwork implements Serializable {}
  public static class TransferData implements Serializable {
    public final int key;
    public final int version;
    public final String value;
    public TransferData(int key, int version, String value) {
      this.key = key; this.version = version; this.value = value;
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

  /** Akka message routing: registers all handlers for the supported messages. */
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
        // Membership & transfer
        .match(GetPeersRequest.class, this::onGetPeersRequest)
        .match(PeerSet.class, this::onPeerSet)
        .match(JoinNetwork.class, this::onJoinNetwork)
        .match(RecoverNetwork.class, this::onRecoverNetwork)
        .match(RequestItemsForJoin.class, this::onRequestItemsForJoin)
        .match(RequestItemsForRecovery.class, this::onRequestItemsForRecovery)
        .match(DataItemsBatch.class, this::onDataItemsBatch)
        .match(AnnounceJoin.class, this::onAnnounceJoin)
        .match(AnnounceLeave.class, this::onAnnounceLeave)
        .match(LeaveNetwork.class, this::onLeaveNetwork)
        .match(TransferData.class, this::onTransferData)
        .build();
  }

  // =====================
  // Message Handlers
  // =====================

  /** Initializes the local membership view and enables serving. */
  private void onInitPeers(InitPeers msg) {
    this.peers = new ArrayList<>(msg.peers);
    this.nodeIdMap.clear();
    this.nodeIdMap.putAll(msg.nodeIds);
    this.serving = true;
  }

  /** Handles client Put: acts as coordinator, gathers versions, computes new version, writes to replicas with timeout. */
  private void onPutRequest(PutRequest msg) {
    if (!serving) {
      getSender().tell(new OperationFailed(msg.key, "Node not serving yet"), getSelf());
      return;
    }
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

  /** Handles client Get: acts as coordinator, collects versions, returns the highest with timeout. */
  private void onGetRequest(GetRequest msg) {
    if (!serving) {
      getSender().tell(new OperationFailed(msg.key, "Node not serving yet"), getSelf());
      return;
    }
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

  /** Handles version replies for both write and read coordinations, driving quorum logic. */
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
          if (readState.client == getSelf()) {
            // Maintenance read: aggiorna localmente senza rispondere a client esterno
            DataItem current = store.get(msg.key);
            if (current == null || maxVersionResponse.version >= current.version) {
              store.put(msg.key, new DataItem(maxVersionResponse.version, maxVersionResponse.value));
              System.out.println("[Node " + nodeId + "] (Maintenance) Refreshed key=" + msg.key +
                                 " v=" + maxVersionResponse.version);
            }
          } else {
            System.out.println("[Node " + nodeId + "] (Read) Returning value v=" + maxVersionResponse.version + " to client");
            readState.client.tell(maxVersionResponse, getSelf());
          }
        } else {
          if (readState.client != getSelf()) {
            readState.client.tell(new GetVersionResponse(msg.key, 0, null, getSelf()), getSelf());
          }
        }

        activeReadCoordinators.remove(msg.key);
      }
    }
  }

  /** Collects PutAck messages and replies to client once W acks are gathered. */
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

  /** Stores a replica value if version is not older (compatibility path). */
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

  /** Applies a replica write if not older and acknowledges with PutAck. */
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

  /** Handles write timeout: if quorum W not reached, fail client and clean coordinator state. */
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

  /** Handles read timeout: if quorum R not reached, fail client and clean coordinator state. */
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

  // =====================
  // Membership & transfer handlers
  // =====================

  /** Returns the current membership view to the requester (bootstrap helper). */
  private void onGetPeersRequest(GetPeersRequest msg) {
    // Risponde con la vista corrente
    getSender().tell(new PeerSet(new ArrayList<>(peers), new HashMap<>(nodeIdMap)), getSelf());
  }

  /** Handles a PeerSet from bootstrap: complete JOIN/RECOVERY setup or refresh view. */
  private void onPeerSet(PeerSet msg) {
    if (mode == Mode.JOINING) {
      // Prepara contesto join
      joinCtx = new JoinContext();
      joinCtx.oldPeers = new ArrayList<>(msg.peers);
      joinCtx.updatedMap = new HashMap<>(msg.nodeIds);

      ActorRef successor = findSuccessorOfId(nodeId, joinCtx.oldPeers, joinCtx.updatedMap);
      joinCtx.successor = successor;
      this.serving = false;

      if (successor != null) {
        System.out.println("[Node " + nodeId + "] JOIN: requesting items from " + shortName(successor));
        successor.tell(new RequestItemsForJoin(nodeId, getSelf()), getSelf());
      } else {
        // Primo nodo: adotta vista vuota + sé e diventa operativo
        System.out.println("[Node " + nodeId + "] JOIN: first node, no successor. Becoming active.");
        List<ActorRef> updPeers = new ArrayList<>(joinCtx.oldPeers);
        Map<ActorRef,Integer> updMap = new HashMap<>(joinCtx.updatedMap);
        updPeers.add(getSelf());
        updMap.put(getSelf(), nodeId);
        this.peers = updPeers;
        this.nodeIdMap.clear();
        this.nodeIdMap.putAll(updMap);
        this.serving = true;
        this.mode = Mode.IDLE;
        this.joinCtx = null;
      }
    } else if (mode == Mode.RECOVERING) {
      // In recovery: adotta la vista corrente e recupera dal successore
      this.peers = new ArrayList<>(msg.peers);
      this.nodeIdMap.clear();
      this.nodeIdMap.putAll(msg.nodeIds);
      this.serving = false;

      ActorRef succ = findSuccessorOfId(nodeId, this.peers, this.nodeIdMap);
      if (succ != null) {
        System.out.println("[Node " + nodeId + "] RECOVER: requesting items from " + shortName(succ));
        succ.tell(new RequestItemsForRecovery(nodeId, getSelf()), getSelf());
      } else {
        System.out.println("[Node " + nodeId + "] RECOVER: no peers, becoming active.");
        this.serving = true;
        this.mode = Mode.IDLE;
      }
    } else {
      // Aggiorna semplice vista
      this.peers = new ArrayList<>(msg.peers);
      this.nodeIdMap.clear();
      this.nodeIdMap.putAll(msg.nodeIds);
    }
  }

  /** Starts a JOIN procedure by asking a bootstrap node for the current view. */
  private void onJoinNetwork(JoinNetwork msg) {
    // Avvio join: richiede la vista a un bootstrap
    this.mode = Mode.JOINING;
    this.serving = false;
    System.out.println("[Node " + nodeId + "] Starting JOIN via " + shortName(msg.bootstrap));
    msg.bootstrap.tell(new GetPeersRequest(), getSelf());
  }

  /** Starts a RECOVERY procedure by asking a bootstrap node for the current view. */
  private void onRecoverNetwork(RecoverNetwork msg) {
    this.mode = Mode.RECOVERING;
    this.serving = false;
    System.out.println("[Node " + nodeId + "] Starting RECOVERY via " + shortName(msg.bootstrap));
    msg.bootstrap.tell(new GetPeersRequest(), getSelf());
  }

  /** As the successor of a new node, sends items in the interval (pred(new), new] to it. */
  private void onRequestItemsForJoin(RequestItemsForJoin req) {
    // Questo nodo è il successore del nuovo nodo: invia items per (pred(new), new]
    ActorRef pred = findPredecessorOfId(req.newNodeId, this.peers, this.nodeIdMap);
    int predId = (pred == null) ? req.newNodeId : this.nodeIdMap.get(pred);

    Map<Integer, DataItem> batch = new LinkedHashMap<>();
    for (Map.Entry<Integer, DataItem> e : store.entrySet()) {
      int key = e.getKey();
      if (inRingIntervalOpenClosed(key, predId, req.newNodeId)) {
        batch.put(key, e.getValue());
      }
    }
    System.out.println("[Node " + nodeId + "] JOIN: sending " + batch.size() + " items to " + shortName(req.newNode));
    req.newNode.tell(new DataItemsBatch(batch), getSelf());
  }

  /** As the successor of a recovering node, sends items in the interval (pred(id), id] to it. */
  private void onRequestItemsForRecovery(RequestItemsForRecovery req) {
    // Il successore consegna gli item nel range (pred(req), req]
    ActorRef pred = findPredecessorOfId(req.requesterId, this.peers, this.nodeIdMap);
    int predId = (pred == null) ? req.requesterId : this.nodeIdMap.get(pred);

    Map<Integer, DataItem> batch = new LinkedHashMap<>();
    for (Map.Entry<Integer, DataItem> e : store.entrySet()) {
      int key = e.getKey();
      if (inRingIntervalOpenClosed(key, predId, req.requesterId)) {
        batch.put(key, e.getValue());
      }
    }
    System.out.println("[Node " + nodeId + "] RECOVER: sending " + batch.size() + " items to " + shortName(req.requester));
    req.requester.tell(new DataItemsBatch(batch), getSelf());
  }

  /** Processes a batch of items during JOIN/RECOVERY (install, update view, maintenance) or generic transfer. */
  private void onDataItemsBatch(DataItemsBatch msg) {
    if (mode == Mode.JOINING) {
      // 1) Salva item
      for (Map.Entry<Integer, DataItem> e : msg.items.entrySet()) {
        DataItem cur = store.get(e.getKey());
        if (cur == null || e.getValue().version >= cur.version) {
          store.put(e.getKey(), e.getValue());
        }
      }
      // 2) Applica vista aggiornata (oldPeers + self) usando la mappa ricevuta dal bootstrap
      List<ActorRef> updatedPeers = new ArrayList<>(joinCtx.oldPeers);
      Map<ActorRef,Integer> updatedMap = new HashMap<>(joinCtx.updatedMap);
      updatedPeers.add(getSelf());
      updatedMap.put(getSelf(), nodeId);

      this.peers = updatedPeers;
      this.nodeIdMap.clear();
      this.nodeIdMap.putAll(updatedMap);

      // 3) Maintenance read per ogni key per allineare versione quorum
      for (Integer key : msg.items.keySet()) {
        ReadCoordinatorState state = new ReadCoordinatorState(key, getSelf());
        activeReadCoordinators.put(key, state);
        for (ActorRef node : getResponsibleNodes(key)) {
          node.tell(new GetVersionRequest(key, getSelf()), getSelf());
        }
      }

      // 4) Annuncia il join agli altri
      for (ActorRef p : joinCtx.oldPeers) {
        p.tell(new AnnounceJoin(getSelf(), nodeId), getSelf());
      }

      this.serving = true;
      this.mode = Mode.IDLE;
      this.joinCtx = null;

      System.out.println("[Node " + nodeId + "] JOIN completed and serving.");

    } else if (mode == Mode.RECOVERING) {
      // 1) Salva item recuperati
      for (Map.Entry<Integer, DataItem> e : msg.items.entrySet()) {
        DataItem cur = store.get(e.getKey());
        if (cur == null || e.getValue().version >= cur.version) {
          store.put(e.getKey(), e.getValue());
        }
      }
      // 2) Pruning rispetto alla vista corrente
      pruneKeysNotResponsible();
      // 3) Maintenance read su item recuperati
      for (Integer key : msg.items.keySet()) {
        ReadCoordinatorState state = new ReadCoordinatorState(key, getSelf());
        activeReadCoordinators.put(key, state);
        for (ActorRef node : getResponsibleNodes(key)) {
          node.tell(new GetVersionRequest(key, getSelf()), getSelf());
        }
      }
      this.serving = true;
      this.mode = Mode.IDLE;
      System.out.println("[Node " + nodeId + "] RECOVERY completed and serving.");
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

  /** Adds a newly joined node to the local view and prunes non-responsible keys. */
  private void onAnnounceJoin(AnnounceJoin msg) {
    if (!nodeIdMap.containsKey(msg.node)) {
      peers.add(msg.node);
      nodeIdMap.put(msg.node, msg.nodeId);
      // Ordina e ripartisci (pruning)
      pruneKeysNotResponsible();
      System.out.println("[Node " + nodeId + "] AnnounceJoin: added " + shortName(msg.node));
    }
  }

  /** Removes a leaving node from the view and prunes non-responsible keys. */
  private void onAnnounceLeave(AnnounceLeave msg) {
    if (nodeIdMap.containsKey(msg.node)) {
      peers.remove(msg.node);
      nodeIdMap.remove(msg.node);
      // Pruning/ribilanciamento: rimuovi ciò che non è più di competenza
      pruneKeysNotResponsible();
      System.out.println("[Node " + nodeId + "] AnnounceLeave: removed " + shortName(msg.node));
    }
  }

  /** Gracefully leaves: transfers data to new responsible nodes, announces leave, and stops self. */
  private void onLeaveNetwork(LeaveNetwork msg) {
    System.out.println("[Node " + nodeId + "] Leaving network...");
    // Costruisce vista aggiornata senza di sé
    List<ActorRef> updatedPeers = new ArrayList<>(peers);
    updatedPeers.remove(getSelf());
    Map<ActorRef,Integer> updatedMap = new HashMap<>(nodeIdMap);
    updatedMap.remove(getSelf());

    // Trasferisce i dati ai nuovi responsabili
    for (Map.Entry<Integer, DataItem> e : new ArrayList<>(store.entrySet())) {
      int key = e.getKey();
      DataItem di = e.getValue();
      // Calcola responsabili nella vista senza di sé
      List<ActorRef> sorted = sortPeersById(updatedPeers, updatedMap);
      if (!sorted.isEmpty()) {
        // Primo responsabile: startIndex come in getResponsibleNodes ma con vista aggiornata
        long unsignedKey = key & 0xFFFFFFFFL;
        int startIndex = 0; boolean found = false;
        for (int i = 0; i < sorted.size(); i++) {
          long nodeKey = updatedMap.get(sorted.get(i)) & 0xFFFFFFFFL;
          if (nodeKey >= unsignedKey) { startIndex = i; found = true; break; }
        }
        if (!found) startIndex = 0;
        int rf = Math.min(N, sorted.size());
        for (int i = 0; i < rf; i++) {
          ActorRef target = sorted.get((startIndex + i) % sorted.size());
          target.tell(new TransferData(key, di.version, di.value), getSelf());
        }
      }
    }

    // Annuncia la leave
    for (ActorRef p : peers) {
      if (p != getSelf()) {
        p.tell(new AnnounceLeave(getSelf(), nodeId), getSelf());
      }
    }

    // Ferma l'attore
    context().stop(getSelf());
  }

  /** Accepts a transferred key/value and installs it if version is newer or equal. */
  private void onTransferData(TransferData msg) {
    DataItem cur = store.get(msg.key);
    if (cur == null || msg.version >= cur.version) {
      store.put(msg.key, new DataItem(msg.version, msg.value));
      System.out.println("[Node " + nodeId + "] Received transfer key=" + msg.key + " v=" + msg.version);
    }
  }
}