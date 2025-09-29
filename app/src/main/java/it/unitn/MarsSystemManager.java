package it.unitn;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import it.unitn.Node.JoinGroupMsg;

/**
 * Central manager for the Mars distributed storage system.
 * Handles node lifecycle (join/leave/crash/recovery), and orchestrates test
 * scenarios according to project specifications.
 */
public class MarsSystemManager extends AbstractActor {

  private static final Logger logger = new Logger("MarsSystemManager");
  private final Random random = new Random();

  /** System state **/
  private final List<ActorRef> activeNodes;
  private final List<ActorRef> crashedNodes;
  private final Map<ActorRef, Integer> nodeIdMap;
  private int viewId;
  private boolean isViewChangedStable;

  /** Constructs the manager with replication parameters. */
  public MarsSystemManager() {
    this.activeNodes = new ArrayList<>();
    this.crashedNodes = new ArrayList<>();
    this.nodeIdMap = new HashMap<>();
    this.viewId = 0;
    this.isViewChangedStable = true;
  }

  /** Akka factory method to create Props for this actor. */
  public static Props props() {
    return Props.create(MarsSystemManager.class, MarsSystemManager::new);
  }

  // =====================
  // Message Classes
  // =====================

  public static class JoinNode implements Serializable {
    public final int nodeId;

    public JoinNode(int nodeId) {
      this.nodeId = nodeId;
    }
  }

  public static class NodeJoined implements Serializable {
    public final int nodeId;
    public final ActorRef nodeRef;

    public NodeJoined(int nodeId, ActorRef nodeRef) {
      this.nodeId = nodeId;
      this.nodeRef = nodeRef;
    }
  }

  public static class LeaveNode implements Serializable {
    public final int nodeId;

    public LeaveNode(int nodeId) {
      this.nodeId = nodeId;
    }
  }

  public static class NodeLeft implements Serializable {
    public final int nodeId;
    public final ActorRef nodeRef;

    public NodeLeft(int nodeId, ActorRef nodeRef) {
      this.nodeId = nodeId;
      this.nodeRef = nodeRef;
    }
  }

  /** Crash a node temporarily. */
  public static class CrashNode implements Serializable {
    public final int nodeId;

    public CrashNode(int nodeId) {
      this.nodeId = nodeId;
    }
  }

  public static class NodeCrashed implements Serializable {
    public final int nodeId;
    public final ActorRef nodeRef;

    public NodeCrashed(int nodeId, ActorRef nodeRef) {
      this.nodeId = nodeId;
      this.nodeRef = nodeRef;
    }
  }

  /** Recover a crashed node. */
  public static class RecoverNode implements Serializable {
    public final int nodeId;

    public RecoverNode(int nodeId) {
      this.nodeId = nodeId;
    }
  }

  public static class NodeRecovered implements Serializable {
    public final int nodeId;
    public final ActorRef nodeRef;

    public NodeRecovered(int nodeId, ActorRef nodeRef) {
      this.nodeId = nodeId;
      this.nodeRef = nodeRef;
    }
  }

  /** Get current system status. */
  public static class GetSystemStatus implements Serializable {
  }

  /** Response with current system status. */
  public static class SystemStatus implements Serializable {
    public final int activeNodeCount;
    public final int crashedNodeCount;
    public final String nodeIds;

    public SystemStatus(int activeNodeCount, int crashedNodeCount, String nodeIds) {
      // public SystemStatus(int activeNodeCount, int crashedNodeCount, int
      // String nodeIds) {
      this.activeNodeCount = activeNodeCount;
      this.crashedNodeCount = crashedNodeCount;
      this.nodeIds = nodeIds;
    }
  }

  // =====================
  // Message Handlers
  // =====================

  private void onJoinGroupMsg(JoinGroupMsg msg) {
    activeNodes.addAll(msg.nodes);
    for (ActorRef node : msg.nodes) {
      nodeIdMap.put(node, msg.nodeIdMap.get(node));
    }
  }

  private void onJoinNode(JoinNode msg) {
    if (!isViewChangedStable) {
      logger.logError("View change in progress, cannot join new node now.");
      return;
    }

    // Choose a random existing node as bootstrap
    ActorRef newNode = getSender();
    ActorRef bootstrap = activeNodes.get(random.nextInt(activeNodes.size()));
    ActorRef succ = RingUtils.findSuccessorOfId(msg.nodeId, activeNodes, nodeIdMap);
    if (succ == null) {
      logger.logError("Cannot join: no suitable bootstrap node found");
      return;
    }

    isViewChangedStable = false;
    newNode.tell(new Node.AllowJoin(msg.nodeId, bootstrap), self());
  }

  private void onNodeJoined(NodeJoined msg) {
    ActorRef newNode = msg.nodeRef;
    int nodeId = msg.nodeId;

    if (nodeIdMap.containsKey(newNode)) {
      logger.logError("Node " + nodeId + " already in the system");
      return;
    }

    isViewChangedStable = true;
    nodeIdMap.put(newNode, nodeId);
    activeNodes.add(newNode);
    viewId++;
    logger.log("JOIN completed: ViewId=" + viewId + ", Node " + nodeId + " joined the system");
  }

  private void onLeaveNode(LeaveNode msg) {
    if (!isViewChangedStable) {
      logger.logError("View change in progress, cannot leave node now.");
      return;
    }

    if (activeNodes.size() <= 1) {
      logger.logError("Refusing to leave the last active node");
      return;
    }

    ActorRef nodeToLeave = findNodeById(activeNodes, msg.nodeId);
    if (nodeToLeave == null) {
      logger.log("Node " + msg.nodeId + " not found in active nodes");
      return;
    }

    logger.log("Allow leaving for node " + msg.nodeId);
    isViewChangedStable = false;
    nodeToLeave.tell(new Node.AllowLeave(), self());
  }

  private void onNodeLeft(NodeLeft msg) {
    ActorRef nodeLeft = msg.nodeRef;
    int nodeId = msg.nodeId;

    if (!nodeIdMap.containsKey(nodeLeft)) {
      logger.logError("Node " + nodeId + " not found in system during NodeLeft");
      return;
    }

    activeNodes.remove(nodeLeft);
    nodeIdMap.remove(nodeLeft);
    isViewChangedStable = true;
    viewId++;
    logger.log("LEAVE completed: ViewId=" + viewId + ", Node " + nodeId + " left the system");
  }

  /** Crashes a node temporarily (simulates failure). */
  private void onCrashNode(CrashNode msg) {
    if (!isViewChangedStable) {
      logger.logError("View change in progress, cannot crash node now.");
      return;
    }

    if (activeNodes.size() <= 1) {
      logger.logError("Refusing to crash the last active node");
      return;
    }

    ActorRef nodeToCrash = findNodeById(activeNodes, msg.nodeId);
    if (nodeToCrash == null) {
      logger.log("Node " + msg.nodeId + " not found in active nodes");
      return;
    }

    isViewChangedStable = false;
    logger.log("Crashing node " + msg.nodeId);
    nodeToCrash.tell(new Node.AllowCrash(), self());
  }

  private void onNodeCrashed(NodeCrashed msg) {
    ActorRef nodeCrashed = msg.nodeRef;
    int nodeId = msg.nodeId;

    if (!nodeIdMap.containsKey(nodeCrashed)) {
      logger.logError("Node " + nodeId + " not found in system during NodeCrashed");
      return;
    }

    isViewChangedStable = true;
    activeNodes.remove(nodeCrashed);
    crashedNodes.add(nodeCrashed);
    logger.log("CRASH completed: ViewId=" + viewId + ", Node " + nodeId + " crashed");
  }

  /** Recovers a crashed node. */
  private void onRecoverNode(RecoverNode msg) {
    if (!isViewChangedStable) {
      logger.logError("View change in progress, cannot recover node now.");
      return;
    }

    if (crashedNodes.isEmpty()) {
      logger.logError("No crashed nodes to recover");
      return;
    }

    if (activeNodes.isEmpty()) {
      logger.logError("Cannot recover: no active nodes for bootstrap");
      return;
    }

    ActorRef nodeToRecover = findNodeById(crashedNodes, msg.nodeId);
    if (nodeToRecover == null) {
      logger.log("Node " + msg.nodeId + " not found in crashed nodes, searching...");
      return;
    }

    ActorRef succ = RingUtils.findSuccessorOfId(msg.nodeId, activeNodes, nodeIdMap);
    ActorRef pred = RingUtils.findPredecessorOfId(msg.nodeId, activeNodes, nodeIdMap);

    if (succ == null || pred == null) {
      logger.logError("Cannot recover: no suitable bootstrap node found");
      return;
    }

    ActorRef bootstrap = activeNodes.get(random.nextInt(activeNodes.size()));
    logger.log("Recovering node " + nodeIdMap.getOrDefault(nodeToRecover, -1) + " via bootstrap "
        + nodeIdMap.getOrDefault(bootstrap, -1));
    isViewChangedStable = false;
    nodeToRecover.tell(new Node.AllowRecover(bootstrap), self());
  }

  private void onNodeRecovered(NodeRecovered msg) {
    ActorRef nodeRecovered = msg.nodeRef;
    int nodeId = msg.nodeId;

    if (!nodeIdMap.containsKey(nodeRecovered)) {
      logger.logError("Node " + nodeId + " not found in system during NodeRecovered");
      return;
    }

    isViewChangedStable = true;
    crashedNodes.remove(nodeRecovered);
    activeNodes.add(nodeRecovered);
    logger.log("RECOVER completed: ViewId=" + viewId + ", Node " + nodeId + " recovered");
  }

  /** Returns current system status. */
  private void onGetSystemStatus(GetSystemStatus msg) {
    String nodeIds = activeNodes.stream()
        .map(node -> String.valueOf(nodeIdMap.get(node)))
        .reduce((a, b) -> a + "," + b)
        .orElse("none");

    SystemStatus status = new SystemStatus(
        activeNodes.size(),
        crashedNodes.size(),
        nodeIds);

    ActorRef replyTo = getSender();
    // Avoid dead letters: do not reply if the sender is deadLetters
    if (!replyTo.equals(getContext().getSystem().deadLetters())) {
      replyTo.tell(status, self());
    }
    logSystemStatus();
  }

  // =====================
  // Helper Methods
  // =====================

  public class RingUtils {
    /**
     * Finds the successor peer for a given id (first peer with id >= given id, with
     * wrap).
     */
    public static ActorRef findSuccessorOfId(int id, List<ActorRef> list, Map<ActorRef, Integer> map) {
      List<ActorRef> sorted = new ArrayList<>(list);
      sorted.sort(Comparator.comparingInt(map::get));

      for (ActorRef p : sorted) {
        if (map.get(p) >= id)
          return p;
      }
      return sorted.isEmpty() ? null : sorted.get(0);
    }

    /**
     * Finds the predecessor peer for a given id (largest id < given id, or last on
     * wrap).
     */
    public static ActorRef findPredecessorOfId(int id, List<ActorRef> list, Map<ActorRef, Integer> map) {
      List<ActorRef> sorted = new ArrayList<>(list);
      sorted.sort(Comparator.comparingInt(map::get));

      ActorRef pred = null;
      int predId = Integer.MIN_VALUE;
      for (ActorRef p : sorted) {
        int pid = map.get(p);
        if (pid < id && pid > predId) {
          predId = pid;
          pred = p;
        }
      }
      if (pred != null)
        return pred;
      return sorted.isEmpty() ? null : sorted.get(sorted.size() - 1);
    }
  }

  private ActorRef findNodeById(List<ActorRef> nodes, int nodeId) {
    return nodes.stream().filter(n -> nodeIdMap.get(n) == nodeId).findFirst().orElse(null);
  }

  /** Logs current system status. */
  private void logSystemStatus() {
    System.out.println("[MarsSystemManager] === SYSTEM STATUS ===");
    System.out.println("[MarsSystemManager] Active nodes: " + activeNodes.size());
    System.out.println("[MarsSystemManager] Crashed nodes: " + crashedNodes.size());

    if (!activeNodes.isEmpty()) {
      String activeIds = activeNodes.stream()
          .map(node -> String.valueOf(nodeIdMap.get(node)))
          .reduce((a, b) -> a + ", " + b)
          .orElse("");
      System.out.println("[MarsSystemManager] Active node IDs: [" + activeIds + "]");
    }

    if (!crashedNodes.isEmpty()) {
      String crashedIds = crashedNodes.stream()
          .map(node -> String.valueOf(nodeIdMap.get(node)))
          .reduce((a, b) -> a + ", " + b)
          .orElse("");
      System.out.println("[MarsSystemManager] Crashed node IDs: [" + crashedIds + "]");
    }

    System.out.println("[MarsSystemManager] =====================");
  }

  // =====================
  // Actor Lifecycle
  // =====================

  /** Message routing for the manager. */
  @Override
  public Receive createReceive() {
    return receiveBuilder()
        .match(JoinGroupMsg.class, this::onJoinGroupMsg)

        .match(JoinNode.class, this::onJoinNode)
        .match(NodeJoined.class, this::onNodeJoined)

        .match(LeaveNode.class, this::onLeaveNode)
        .match(NodeLeft.class, this::onNodeLeft)

        .match(CrashNode.class, this::onCrashNode)
        .match(NodeCrashed.class, this::onNodeCrashed)

        .match(RecoverNode.class, this::onRecoverNode)
        .match(NodeRecovered.class, this::onNodeRecovered)

        .match(GetSystemStatus.class, this::onGetSystemStatus)
        .build();
  }
}