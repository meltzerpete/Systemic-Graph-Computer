package parallel;

import graphEngine.Components;
import nodeParser.NodeMatch;
import nodeParser.Parser;
import org.apache.commons.lang3.time.StopWatch;
import org.neo4j.graphdb.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.WeakHashMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.stream.Stream;

import static graphEngine.Components.CONTAINS;

/**
 * Created by Pete Meltzer on 05/08/17.
 */
public class Manager {

    static final int MAX_INTERACTIONS = 10000;
    static final int QUEUE_SIZE = 20;
    static final int NO_OF_CONSUMERS = 100;
    static final int NO_OF_PRODUCERS = 2;

    static GraphDatabaseService db;

    static WeakHashMap<Long,ReentrantLock> nodeLocks;

    static ContextEntry[] contextArray;
    static BlockingQueue<Triplet> tripletQueue = new ArrayBlockingQueue<>(QUEUE_SIZE);
    static ConcurrentHashMap<Long,Long[]> scopeContainedIDs;

    static boolean run = true;
    static CountDownLatch count = new CountDownLatch(MAX_INTERACTIONS);

    /**
     * Get all parent scopes for the given {@link Node}.
     * @param node {@link Node} for which to find containing scopes
     * @return {@link Stream} of scope {@link Node}s
     */
    static Stream<Node> getParentScopes(Node node) {
        ResourceIterator<Relationship> relationships =
                (ResourceIterator<Relationship>) node.getRelationships(CONTAINS, Direction.INCOMING).iterator();

        return relationships.stream().map(Relationship::getStartNode);
    }

    //TODO swap single permit semaphores to ReentrantLocks

    public static void go(GraphDatabaseService db) {
        Manager.db = db;

        try (Transaction tx = db.beginTx()) {

            /* SETUP */

            StopWatch setupTimer = new StopWatch();
            setupTimer.start();

            // create a mutex for every node
            nodeLocks = new WeakHashMap<>();
            db.getAllNodes().stream()
                    .forEach(node -> nodeLocks.put(node.getId(), new ReentrantLock()));


            // create node change cache
//            cache = new NodeCache(nodeLocks.length);

            // for every contextEntry - create a contextEntry entry. if contextEntry appears in multiple scope -> add entry for each
            ArrayList<ContextEntry> contextEntries = new ArrayList<>();
            db.findNodes(Components.CONTEXT).stream().forEach(context -> {
                // check function defined
                if (!context.hasProperty(Components.function))
                    return;

                BiConsumer<Node, Node> function = Functions.getFunction((String) context.getProperty(Components.function));

                // compile queries
                if (!(context.hasProperty(Components.s1Query) && context.hasProperty(Components.s2Query)))
                    return;

//                Parser parser = new Parser();

                NodeMatch s1 = (new Parser()).parse((String) context.getProperty(Components.s1Query));
                NodeMatch s2 = (new Parser()).parse((String) context.getProperty(Components.s2Query));

                getParentScopes(context).forEach(scope -> {
                    contextEntries.add(new ContextEntry(context.getId(), function, s1, s2, scope.getId()));
                });
            });

            contextArray = new ContextEntry[contextEntries.size()];
            contextEntries.toArray(contextArray);

            // for every scope create a vector for the systems it contains
            int nScopes = (int) db.findNodes(Components.SCOPE).stream().count();
            scopeContainedIDs = new ConcurrentHashMap<>((nScopes * 3) / 2);

            db.findNodes(Components.SCOPE).forEachRemaining(scope -> {

                ArrayList<Long> contained = new ArrayList<>(nScopes);
                ((ResourceIterator<Relationship>)
                        scope.getRelationships(Components.CONTAINS, Direction.OUTGOING).iterator()).stream()
                        .map(Relationship::getEndNode)
                        .forEach(node -> contained.add(node.getId()));

                Long[] containedArray = new Long[contained.size()];
                contained.toArray(containedArray);
                scopeContainedIDs.put(scope.getId(), containedArray);
            });
        tx.success();
        setupTimer.stop();
        System.out.println(String.format("setup: %,d x 10e-9 s", setupTimer.getNanoTime()));
        }


        /* LAUNCH PRODUCER AND CONSUMER THREADS */

        StopWatch timer = new StopWatch();
        timer.start();

        Thread[] producerThreads = new Thread[NO_OF_PRODUCERS];
        for (Thread p : producerThreads) {
            p = new Thread(new ProduceTripletTask(),"Producer");
            p.start();
        }

        Thread[] consumerThreads = new Thread[NO_OF_CONSUMERS];
        for (Thread c : consumerThreads) {
            c = new Thread(new ConsumeTripletTask(), "Consumer");
            c.start();
        }

        try {
            count.await();
            timer.stop();
            run = false;

            for (Thread p : producerThreads) {
                if (p != null && p.isAlive()) p.getThreadGroup().destroy();
            }            
            
            for (Thread c : consumerThreads) {
                if (c != null && c.isAlive()) c.getThreadGroup().destroy();
            }

        } catch (NullPointerException | IllegalThreadStateException | InterruptedException e) {
            //ignore exception
        } finally {
            for (Thread c : consumerThreads) {
                while (c != null && c.isAlive());
            }
            System.out.println("TERMINATING");
            System.out.println(String.format("Time: %,d x 10e-3 s", timer.getTime()));
        }


    }

    static void printQueue() {
        int pos = 0;
        tripletQueue.forEach(triplet -> {
            System.out.println(String.format("%d: %s", pos, triplet));
        });
    }

    static boolean matchNode(NodeMatch queryNode, Node targetNode) {

        // check labels & properties
        if (!queryNode.getLabels().parallelStream().allMatch(targetNode::hasLabel))
            return false;

        return queryNode.getProperties().parallelStream()
                .allMatch(objectPropertyPair ->
                        targetNode.getProperty(objectPropertyPair.getKey(), objectPropertyPair.getValue()) != null
                                && targetNode.getProperty(objectPropertyPair.getKey()).equals(objectPropertyPair.getValue()));
    }
}
