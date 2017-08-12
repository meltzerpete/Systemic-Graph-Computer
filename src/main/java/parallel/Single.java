//package parallel;
//
//import graphEngine.Components;
//import nodeParser.NodeMatch;
//import nodeParser.Parser;
//import org.apache.commons.lang3.time.StopWatch;
//import org.neo4j.graphdb.*;
//
//import java.util.HashSet;
//import java.util.LinkedList;
//import java.util.concurrent.*;
//import java.util.function.BiConsumer;
//import java.util.stream.Stream;
//
//import static graphEngine.Components.CONTAINS;
//import static parallel.Manager.*;
//import static parallel.Manager.tripletQueue;
//
///**
// * Created by Pete Meltzer on 05/08/17.
// */
//public class Single {
//
//    static final int MAX_INTERACTIONS = 10000;
//    static final int QUEUE_SIZE = 20;
//    static final int NO_OF_CONSUMERS = 18;
//
//    static GraphDatabaseService db;
//
//    static Semaphore[] nodeLocks;
//
//    static ContextEntry[] contextArray;
//    static BlockingQueue<Triplet> tripletQueue = new ArrayBlockingQueue<>(QUEUE_SIZE);
//    static ConcurrentHashMap<Long,Long[]> nodesContainedInScope;
//
//    static boolean run = true;
//    static CountDownLatch count = new CountDownLatch(MAX_INTERACTIONS);
//
//    static ThreadPoolExecutor executor;
//    static Thread p1 = new Thread(new ProduceTripletTask());
//
//    /**
//     * Get all parent scopes for the given {@link Node}.
//     * @param node {@link Node} for which to find containing scopes
//     * @return {@link Stream} of scope {@link Node}s
//     */
//    static Stream<Node> getParentScopes(Node node) {
//        ResourceIterator<Relationship> relationships =
//                (ResourceIterator<Relationship>) node.getRelationships(CONTAINS, Direction.INCOMING).iterator();
//
//        return relationships.stream().map(Relationship::getStartNode);
//    }
//
//    static void go(GraphDatabaseService db) {
//        Single.db = db;
//
//        try (Transaction tx = db.beginTx()) {
//
//            /* SETUP */
//
//            StopWatch setupTimer = new StopWatch();
//            setupTimer.start();
//
//            // create a consumerMutex for every node
//            nodeLocks = new Semaphore[(int) db.getAllNodes().stream().count()];
//            for (int i = 0; i < nodeLocks.length; i++)
//                nodeLocks[i] = new Semaphore(1);
//
//            // for every contextEntry - create a contextEntry entry. if contextEntry appears in multiple scope -> add entry for each
//            LinkedList<ContextEntry> contextEntries = new LinkedList<>();
//            db.findNodes(Components.CONTEXT).stream().forEach(context -> {
//                // check function defined
//                if (!context.hasProperty(Components.function))
//                    return;
//
//                BiConsumer<Node, Node> function = Functions.getFunction((String) context.getProperty(Components.function));
//
//                // compile queries
//                if (!(context.hasProperty(Components.s1Query) && context.hasProperty(Components.s2Query)))
//                    return;
//
////                Parser parser = new Parser();
//
//                NodeMatch s1 = (new Parser()).parse((String) context.getProperty(Components.s1Query));
//                NodeMatch s2 = (new Parser()).parse((String) context.getProperty(Components.s2Query));
//
//                getParentScopes(context).forEach(scope -> {
//                    contextEntries.add(new ContextEntry(context.getId(), function, s1, s2, scope.getId()));
//                });
//            });
//
//            contextArray = new ContextEntry[contextEntries.size()];
//            contextEntries.toArray(contextArray);
////            contexts = new AtomicReferenceArray<>(contextArray);
//
//            // for every scope create a vector for the systems it contains
//            int nScopes = (int) db.findNodes(Components.SCOPE).stream().count();
//            nodesContainedInScope = new ConcurrentHashMap<>((nScopes * 3) / 2);
//
//            db.findNodes(Components.SCOPE).forEachRemaining(scope -> {
//
//                LinkedList<Long> contained = new LinkedList<>();
//                ((ResourceIterator<Relationship>)
//                        scope.getRelationships(Components.CONTAINS, Direction.OUTGOING).iterator()).stream()
//                        .map(Relationship::getEndNode)
//                        .forEach(node -> contained.add(node.getId()));
//
//                Long[] containedArray = new Long[contained.size()];
//                contained.toArray(containedArray);
//                nodesContainedInScope.put(scope.getId(), containedArray);
//            });
//        tx.success();
//        setupTimer.stop();
//            System.out.println(String.format("setup: %,d x 10e-9 s", setupTimer.getNanoTime()));
//        }
//
//
//        /* LAUNCH PRODUCER AND CONSUMER THREADS */
//        StopWatch timer = new StopWatch();
//        timer.start();
//
//        int count = 0;
//
//        Triplet triplet = null;
//
//        while (count < MAX_INTERACTIONS) {
//            /* PRODUCER */
//
//            ContextEntry contextEntry = contextArray[(int) (Math.random() * (contextArray.length))];
//            Long[] containedIDs = nodesContainedInScope.get(contextEntry.scope).clone();
//
//            long s1Match = -1;
//            long s2Match = -1;
//
//            HashSet<Long> visited = new HashSet<>();
//            int visitedCount = 0;
//
//            while (containedIDs.length > visitedCount) {
//
//                // get random targetID
//                long targetID = containedIDs[(int) (Math.random() * containedIDs.length)];
//                if (!visited.add(targetID)) continue;
//                visitedCount++;
//
//                // if target is the context try next target
//                if (targetID == contextEntry.context) {
//                    continue;
//                }
//
//                try (Transaction tx = db.beginTx()) {
//                    // S1
//                    // check node matches properties, labels etc.
//                    if (s1Match < 0 && matchNode(contextEntry.s1, db.getNodeById(targetID))) {
//                        // match
//                        s1Match = targetID;
//                        tx.success();
//                        continue;   // prevent s1 and s2 getting same target
//                    }
//
//                    // S2
//                    // check node matches properties, labels etc.
//                    if (s2Match < 0 && matchNode(contextEntry.s2, db.getNodeById(targetID))) {
//                        // match
//                        s2Match = targetID;
//                    }
//                    tx.success();
//                } catch (DatabaseShutdownException e) {
//                    if (run) e.printStackTrace();
//                    // otherwise ignore exception
//                }
//
//                if (s1Match >= 0 && s2Match >= 0) break;
//            }
//
//            if (s1Match >= 0 && s2Match >= 0) {
//                // match found - add to tripletQueue
//                triplet = new Triplet(contextEntry, s1Match, s2Match);
////                tripletQueue.offer(triplet);
//            }
//
//            if (triplet == null) continue;
//
//            /* CONSUMER */
////            Triplet triplet;
//
////                triplet = tripletQueue.poll();
//
//            try (Transaction tx = db.beginTx()) {
//                Node s1 = db.getNodeById(triplet.s1);
//                Node s2 = db.getNodeById(triplet.s2);
//
//                // acquire locks - nodes could be in any order so must be
//                // wrapped with consumerMutex to avoid deadlock
//                if ((matchNode(triplet.contextEntry.s1, s1) && matchNode(triplet.contextEntry.s2, s2))) {
//                    triplet.contextEntry.function.accept(s1, s2);
//                    if (count % 100 == 0) {
////                            System.out.println(c);
//                        System.out.println((count) + " queueSize: " + tripletQueue.size());
////                            System.out.println(String.format("Current: %s", triplet));
////                            System.out.println(Arrays.toString(db.getAllLabels().stream().toArray()));
//                    }
//                }
//                // commit transaction and release locks
//                count++;
//                tx.success();
//
//            } catch (DatabaseShutdownException e) {
//                if (run) e.printStackTrace();
//                // otherwise ignore exception
//            }
//
//        }
//
//
//        timer.stop();
//        System.out.println("TERMINATING");
//        System.out.println(String.format("Time: %,d x 10e-3 s", timer.getTime()));
//
//
//
//    }
//
//    static void printQueue() {
//        int pos = 0;
//        tripletQueue.forEach(triplet -> {
//            System.out.println(String.format("%d: %s", pos, triplet));
//        });
//    }
//
//    static boolean matchNode(NodeMatch queryNode, Node targetNode) {
//
//        // check labels & properties
//        if (!queryNode.getLabels().parallelStream().allMatch(targetNode::hasLabel))
//            return false;
//
//        return queryNode.getProperties().parallelStream()
//                .allMatch(objectPropertyPair ->
//                        targetNode.getProperty(objectPropertyPair.getKey(), objectPropertyPair.getValue()) != null
//                                && targetNode.getProperty(objectPropertyPair.getKey()).equals(objectPropertyPair.getValue()));
//    }
//}
