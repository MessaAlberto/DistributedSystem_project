package it.unitn;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import akka.actor.*;
import akka.japi.pf.ReceiveBuilder;

public class MarsStorageSystem {

    // Message types
    public static class PutRequest implements Serializable {
        public final String key;
        public final String value;
        public PutRequest(String key, String value) {
            this.key = key;
            this.value = value;
        }
    }

    public static class GetRequest implements Serializable {
        public final String key;
        public GetRequest(String key) {
            this.key = key;
        }
    }

    public static class GetResponse implements Serializable {
        public final String key;
        public final String value;
        public GetResponse(String key, String value) {
            this.key = key;
            this.value = value;
        }
    }

    public static class Gossip implements Serializable {
        public final Map<String, VersionedValue> data;
        public Gossip(Map<String, VersionedValue> data) {
            this.data = data;
        }
    }

    public static class VersionedValue implements Serializable {
        public final String value;
        public final int version;
        public VersionedValue(String value, int version) {
            this.value = value;
            this.version = version;
        }
    }

    // StorageNode Actor
    public static class StorageNode extends AbstractActor {

        private final String id;
        private final Map<String, VersionedValue> store = new ConcurrentHashMap<>();
        private final List<ActorRef> peers = new ArrayList<>();
        private final Path filePath;

        public StorageNode(String id) {
            this.id = id;
            this.filePath = Paths.get("data_" + id + ".txt");
            loadFromDisk();
        }

        @Override
        public Receive createReceive() {
            return ReceiveBuilder.create()
                .match(PutRequest.class, this::onPut)
                .match(GetRequest.class, this::onGet)
                .match(Gossip.class, this::onGossip)
                .matchEquals("gossip", m -> gossip())
                .match(ActorRef.class, this::onJoin)
                .build();
        }

        private void onPut(PutRequest req) {
            VersionedValue current = store.getOrDefault(req.key, new VersionedValue("", 0));
            VersionedValue updated = new VersionedValue(req.value, current.version + 1);
            store.put(req.key, updated);
            saveToDisk();

            // Broadcast to peers
            for (ActorRef peer : peers) {
                peer.tell(new Gossip(Collections.singletonMap(req.key, updated)), self());
            }
        }

        private void onGet(GetRequest req) {
            VersionedValue val = store.get(req.key);
            String response = val != null ? val.value : null;
            sender().tell(new GetResponse(req.key, response), self());
        }

        private void onGossip(Gossip gossip) {
            for (Map.Entry<String, VersionedValue> entry : gossip.data.entrySet()) {
                String key = entry.getKey();
                VersionedValue incoming = entry.getValue();
                VersionedValue local = store.get(key);

                if (local == null || incoming.version > local.version) {
                    store.put(key, incoming);
                }
            }
            saveToDisk();
        }

        private void gossip() {
            for (ActorRef peer : peers) {
                peer.tell(new Gossip(new HashMap<>(store)), self());
            }
        }

        private void onJoin(ActorRef peer) {
            if (!peers.contains(peer) && !peer.equals(self())) {
                peers.add(peer);
            }
        }

        private void loadFromDisk() {
            try {
                if (Files.exists(filePath)) {
                    List<String> lines = Files.readAllLines(filePath);
                    for (String line : lines) {
                        String[] parts = line.split(",");
                        if (parts.length == 3) {
                            String key = parts[0];
                            String value = parts[1];
                            int version = Integer.parseInt(parts[2]);
                            store.put(key, new VersionedValue(value, version));
                        }
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        private void saveToDisk() {
            try (BufferedWriter writer = Files.newBufferedWriter(filePath)) {
                for (Map.Entry<String, VersionedValue> entry : store.entrySet()) {
                    writer.write(entry.getKey() + "," + entry.getValue().value + "," + entry.getValue().version);
                    writer.newLine();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) {
        ActorSystem system = ActorSystem.create("MarsSystem");

        ActorRef nodeA = system.actorOf(Props.create(StorageNode.class, "A"), "nodeA");
        ActorRef nodeB = system.actorOf(Props.create(StorageNode.class, "B"), "nodeB");

        // Make nodes aware of each other
        nodeA.tell(nodeB, ActorRef.noSender());
        nodeB.tell(nodeA, ActorRef.noSender());

        // Send test put/get
        nodeA.tell(new PutRequest("x", "42"), ActorRef.noSender());
        nodeB.tell(new GetRequest("x"), ActorRef.noSender());

        // Periodic gossip
        system.scheduler().scheduleAtFixedRate(
            Duration.ofSeconds(5),
            Duration.ofSeconds(10),
            nodeA,
            "gossip",
            system.dispatcher(),
            ActorRef.noSender()
        );
    }
}
