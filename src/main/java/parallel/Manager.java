package parallel;

import graphEngine.Components;
import nodeParser.NodeMatch;
import nodeParser.Parser;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.neo4j.graphdb.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.stream.Stream;

import static graphEngine.Components.CONTAINS;

/**
 * Created by Pete Meltzer on 05/08/17.
 */
public class Manager {

    final int MAX_INTERACTIONS;
    final int QUEUE_SIZE;
    final int NO_OF_CONSUMERS;

    final ReentrantLock consumerMutex;
    HashMap<Long, ReentrantLock> producerLocks;
    final AtomicInteger upCounter;
    CountDownLatch count;

    int NO_OF_PRODUCERS = 2;

    GraphDatabaseService db;

    HashMap<Long,ReentrantLock> nodeLocks;

    ContextEntry[] contextArray;

    BlockingQueue<Triplet> tripletQueue;
    ConcurrentHashMap<Long,Long[]> scopeContainedIDs;
    AtomicBoolean run;


    public Manager(int MAX_INTERACTIONS, int QUEUE_SIZE, int NO_OF_CONSUMERS, int NO_OF_PRODUCERS, GraphDatabaseService db) {
        this.MAX_INTERACTIONS = MAX_INTERACTIONS;
        this.QUEUE_SIZE = QUEUE_SIZE;
        this.NO_OF_CONSUMERS = NO_OF_CONSUMERS;
        this.NO_OF_PRODUCERS = NO_OF_PRODUCERS;
        this.db = db;
        tripletQueue = new ArrayBlockingQueue<>(this.QUEUE_SIZE);
        count = new CountDownLatch(this.MAX_INTERACTIONS);
        run = new AtomicBoolean(true);
        consumerMutex = new ReentrantLock();
        upCounter = new AtomicInteger(0);
    }

    public Manager(int MAX_INTERACTIONS, GraphDatabaseService db) {
        this(MAX_INTERACTIONS, 20, 20, 4, db);
    }

    /**
     * Get all parent scopes for the given {@link Node}.
     * @param node {@link Node} for which to find containing scopes
     * @return {@link Stream} of scope {@link Node}s
     */
    Stream<Node> getParentScopes(Node node) {
        ResourceIterator<Relationship> relationships =
                (ResourceIterator<Relationship>) node.getRelationships(CONTAINS, Direction.INCOMING).iterator();

        return relationships.stream().map(Relationship::getStartNode);
    }

    public void go() {

        try (Transaction tx = db.beginTx()) {

            /* SETUP */

            StopWatch setupTimer = new StopWatch();
            setupTimer.start();

            // create a consumerMutex for every node
            nodeLocks = new HashMap<>();
            producerLocks = new HashMap<>();
            db.getAllNodes().stream()
                    .forEach(node -> {
                        nodeLocks.put(node.getId(), new ReentrantLock());
                        producerLocks.put(node.getId(), new ReentrantLock());
                    });

            // for every contextEntry - create a contextEntry entry. if contextEntry appears in multiple scope -> add entry for each
            ArrayList<ContextEntry> contextEntries = new ArrayList<>();
            db.findNodes(Components.CONTEXT).stream().forEach(context -> {
                // check function defined
                if (!context.hasProperty(Components.function))
                    return;

                Functions functions = new Functions(this);
                BiConsumer<Node, Node> function = functions.getFunction((String) context.getProperty(Components.function));

                // compile queries
                if (!(context.hasProperty(Components.s1Query) && context.hasProperty(Components.s2Query)))
                    return;

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

        Thread[] consumerThreads = new Thread[NO_OF_CONSUMERS];
        for (Thread c : consumerThreads) {
            c = new Thread(new ConsumeTripletTask(this));
            c.setName("Consumer[" + c.getId() + "]");
            c.start();
        }

        Thread[] producerThreads = new Thread[NO_OF_PRODUCERS];
        for (Thread p : producerThreads) {
            p = new Thread(new ProduceTripletTask2(this),"Producer");
            p.start();
        }

        try {
            count.await();
            timer.stop();
            run.set(false);

            // poison the queue
            tripletQueue.put(new Triplet(null, 0, 0));

            for (Thread thread : ArrayUtils.addAll(producerThreads, consumerThreads)) {
                if (thread != null) thread.interrupt();
                if (thread != null) thread.join();
            }

        } catch (NullPointerException | IllegalThreadStateException | InterruptedException e) {
            //ignore exception
            e.printStackTrace();
        } finally {
            System.out.println("TERMINATING");
            System.out.println(String.format("Time: %,d x 10e-3 s", timer.getTime()));
        }


    }

    void printQueue() {
        int pos = 0;
        tripletQueue.forEach(triplet -> {
            System.out.println(String.format("%d: %s", pos, triplet));
        });
    }

    boolean matchNode(NodeMatch queryNode, Node targetNode) {

        // check labels & properties
        if (!queryNode.getLabels().parallelStream().allMatch(targetNode::hasLabel))
            return false;

        return queryNode.getProperties().parallelStream()
                .allMatch(objectPropertyPair ->
                        targetNode.getProperty(objectPropertyPair.getKey(), objectPropertyPair.getValue()) != null
                                && targetNode.getProperty(objectPropertyPair.getKey()).equals(objectPropertyPair.getValue()));
    }

    boolean match(NodeMatch queryNode, NodeMatch targetNode) {

        // labels
        if (!queryNode.getLabels().parallelStream()
                .allMatch(label -> targetNode.getLabels().contains(label))) {
            return false;
        }

        return queryNode.getProperties().parallelStream()
                .allMatch(objectPropertyPair ->
                        targetNode.getProperties().contains(objectPropertyPair));

    }
}
