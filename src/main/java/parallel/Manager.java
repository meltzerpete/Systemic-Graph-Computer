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

    int totalNoOfNodes;

    final ReentrantLock consumerMutex;
    final AtomicInteger upCounter;
    CountDownLatch count;

    final int NO_OF_PRODUCERS;

    GraphDatabaseService db;

    HashMap<Long,ReentrantLock> nodeLocks;

    // ONLY USED IN PRODUCER TO GET RANDOM CONTEXT
    ContextEntry[] contextArray;

    Long[] allNodeIDs;

    BlockingQueue<Triplet> tripletQueue;
    ConcurrentHashMap<Long,Long[]> nodesContainedInScope;
    AtomicBoolean run;

    ConcurrentHashMap<Long, Long[]> parentScopes;


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
        this(MAX_INTERACTIONS, 100, 200, 10, db);
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

            // for every system create an array for the scopes it is in
            totalNoOfNodes = (int) db.getAllNodes().stream().count();
            parentScopes = new ConcurrentHashMap<>((totalNoOfNodes * 3) / 2);

            db.getAllNodes().forEach(node -> {

                ArrayList<Long> parents = new ArrayList<>();
                ((ResourceIterator<Relationship>)
                        node.getRelationships(Components.CONTAINS, Direction.INCOMING).iterator()).stream()
                        .map(Relationship::getStartNode)
                        .forEach(scope -> parents.add(scope.getId()));

                Long[] parentsArray = new Long[parents.size()];
                parents.toArray(parentsArray);
                parentScopes.put(node.getId(), parentsArray);
            });

            // create an array of all node IDs
            ArrayList<Long> allNodes = new ArrayList<>();
            db.getAllNodes().stream()
                    .map(Node::getId)
                    .forEach(allNodes::add);

            allNodeIDs = new Long[allNodes.size()];
            allNodes.toArray(allNodeIDs);

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
