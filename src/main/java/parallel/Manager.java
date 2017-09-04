package parallel;

import common.Components;
import nodeParser.NodeMatch;
import nodeParser.Parser;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.neo4j.graphdb.*;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
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

/**
 * Created by Pete Meltzer on 05/08/17.
 */
public class Manager {

    private final int NO_OF_CONSUMERS;
    private final int NO_OF_PRODUCERS;

    final ReentrantLock consumerMutex;
    final AtomicInteger upCounter;
    CountDownLatch count;

    AtomicBoolean run;

    GraphDatabaseService db;

    BlockingQueue<Triplet> tripletQueue;
    HashMap<Long,ReentrantLock> nodeLocks;

    // ONLY USED IN PRODUCER TO GET RANDOM CONTEXT
    ContextEntry[] contextArray;

    ConcurrentHashMap<Long,Long[]> nodesContainedInScope;

    // USED FOR TESTING
    volatile StringBuilder timingLog = new StringBuilder();


    public Manager(int MAX_INTERACTIONS, int QUEUE_SIZE, int NO_OF_CONSUMERS, int NO_OF_PRODUCERS, GraphDatabaseService db) {
        this.NO_OF_CONSUMERS = NO_OF_CONSUMERS;
        this.NO_OF_PRODUCERS = NO_OF_PRODUCERS;
        this.db = db;
        tripletQueue = new ArrayBlockingQueue<>(QUEUE_SIZE);
        count = new CountDownLatch(MAX_INTERACTIONS);
        run = new AtomicBoolean(true);
        consumerMutex = new ReentrantLock();
        upCounter = new AtomicInteger(0);
    }

    public Manager(int MAX_INTERACTIONS, GraphDatabaseService db) {

        this(MAX_INTERACTIONS,
                20,
                200,
                2,
                db);
    }

    /**
     * Get all parent scopes for the given {@link Node}.
     * @param node {@link Node} for which to find containing scopes
     * @return {@link Stream} of scope {@link Node}s
     */
    private Stream<Node> getParentScopes(Node node) {
        ResourceIterator<Relationship> relationships =
                (ResourceIterator<Relationship>) node.getRelationships(Components.CONTAINS, Direction.INCOMING).iterator();

        return relationships.stream().map(Relationship::getStartNode);
    }

    public void go() {

        try (Transaction tx = db.beginTx()) {

            /* SETUP */

            StopWatch setupTimer = new StopWatch();
            setupTimer.start();

            // create a consumerMutex for every node
            nodeLocks = new HashMap<>();
            db.getAllNodes().stream()
                    .forEach(node -> {
                        nodeLocks.put(node.getId(), new ReentrantLock());
                    });

            // for every contextEntry - create a contextEntry entry. if contextEntry appears in multiple scope -> add entry for each
            ArrayList<ContextEntry> contextEntriesArrayList = new ArrayList<>();
            db.findNodes(Components.CONTEXT).stream()
                    // check has function defined
                    .filter(context -> context.hasProperty(Components.function))
                    // check has Query properties defined
                    .filter(context -> context.hasProperty(Components.s1Query))
                    .filter(context -> context.hasProperty(Components.s2Query))
                    .forEach(context -> {

                        // get function
                        Functions functions = new Functions(this);
                        BiConsumer<Node, Node> function = functions.getFunction((String) context.getProperty(Components.function));

                        // compile queries
                        NodeMatch s1 = (new Parser()).parse((String) context.getProperty(Components.s1Query));
                        NodeMatch s2 = (new Parser()).parse((String) context.getProperty(Components.s2Query));

                        getParentScopes(context).forEach(scope -> {
                            ContextEntry entry = new ContextEntry(context.getId(), function, s1, s2, scope.getId());
                            contextEntriesArrayList.add(entry);
                        });
            });

            // convert to array
            contextArray = new ContextEntry[contextEntriesArrayList.size()];
            contextEntriesArrayList.toArray(contextArray);

            // for every scope create an array for the systems it contains
            int nScopes = (int) db.findNodes(Components.SCOPE).stream().count();
            nodesContainedInScope = new ConcurrentHashMap<>((nScopes * 3) / 2);

            db.findNodes(Components.SCOPE).forEachRemaining(scope -> {

                ArrayList<Long> containedNodesArrayList = new ArrayList<>();
                ((ResourceIterator<Relationship>)
                        scope.getRelationships(Components.CONTAINS, Direction.OUTGOING).iterator()).stream()
                        .map(Relationship::getEndNode)
                        .forEach(node -> containedNodesArrayList.add(node.getId()));

                Long[] containedNodesArray = new Long[containedNodesArrayList.size()];
                containedNodesArrayList.toArray(containedNodesArray);
                nodesContainedInScope.put(scope.getId(), containedNodesArray);
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
            p = new Thread(new ProduceTripletTask(this),"Producer");
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

            // write the timing log to file
            File file = new File("sc2-knapsack-100interactions.csv");
            FileWriter writer = new FileWriter(file, true);
            writer.append("sc2 Knapsack\n");
            writer.append(timingLog);
            writer.close();

        } catch (NullPointerException | IllegalThreadStateException | InterruptedException | IOException e) {
            e.printStackTrace();
        } finally {
            System.out.println("TERMINATING");
            System.out.println(String.format("Time: %,d x 10e-3 s", timer.getTime()));
        }
    }

    // for debugging
    void printQueue() {
        int pos = 0;
        tripletQueue.forEach(triplet -> {
            System.out.println(String.format("%d: %s", pos, triplet));
        });
    }

    boolean matchNode(NodeMatch queryNode, Node targetNode) {

        // check labels
        if (!queryNode.getLabels().parallelStream().allMatch(targetNode::hasLabel))
            return false;

        // check properties
        return queryNode.getProperties().parallelStream()
                .allMatch(objectPropertyPair ->
                        targetNode.getProperty(objectPropertyPair.getKey(), objectPropertyPair.getValue()) != null
                                && targetNode.getProperty(objectPropertyPair.getKey()).equals(objectPropertyPair.getValue()));
    }
}
