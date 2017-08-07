package parallel;

import graphEngine.Components;
import nodeParser.NodeMatch;
import nodeParser.Parser;
import org.apache.commons.lang3.time.StopWatch;
import org.neo4j.graphdb.*;

import java.util.LinkedList;
import java.util.Vector;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.BiConsumer;
import java.util.stream.Stream;

import static graphEngine.Components.CONTAINS;

/**
 * Created by Pete Meltzer on 05/08/17.
 */
public class Manager {

    static final int MAX_INTERACTIONS = 1000;
    static final int QUEUE_SIZE = 100;

    static GraphDatabaseService db;

    static AtomicReferenceArray<ContextEntry> contexts;
    static BlockingQueue<Triplet> tripletQueue = new ArrayBlockingQueue<>(QUEUE_SIZE);
//    static Set<Long> inQueueSet = Collections.newSetFromMap(new ConcurrentHashMap<Long, Boolean>(QUEUE_SIZE * 3));
    static ConcurrentHashMap<Long,Vector<Long>> scopeVectors;

    static CountDownLatch count = new CountDownLatch(MAX_INTERACTIONS);

    static ConcurrentLinkedQueue<Long> consumerTimes = new ConcurrentLinkedQueue<>();
    static ConcurrentLinkedQueue<Long> producerTimes = new ConcurrentLinkedQueue<>();

    static ThreadPoolExecutor executor;
    static Future<?> extraProducer;
    static Future<?> extraConsumer;

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

    static void go(GraphDatabaseService db) {
        Manager.db = db;

        try (Transaction tx = db.beginTx()) {

            /* SETUP */

            // for every context - create a context entry. if context appears in multiple scope -> add entry for each
            LinkedList<ContextEntry> contextEntries = new LinkedList<>();
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

            ContextEntry[] contextArray = new ContextEntry[contextEntries.size()];
            contextEntries.toArray(contextArray);
            contexts = new AtomicReferenceArray<>(contextArray);

            // for every scope create a vector for the systems it contains
            int nScopes = (int) db.findNodes(Components.SCOPE).stream().count();
            scopeVectors = new ConcurrentHashMap<>((nScopes * 3) / 2);

            db.findNodes(Components.SCOPE).forEachRemaining(scope -> {

                Vector<Long> contained = new Vector<>();
                ((ResourceIterator<Relationship>)
                        scope.getRelationships(Components.CONTAINS, Direction.OUTGOING).iterator()).stream()
                        .map(Relationship::getEndNode)
                        .forEach(node -> contained.add(node.getId()));

                scopeVectors.put(scope.getId(), contained);
            });
        }

        /* LAUNCH PRODUCER AND CONSUMER THREADS */

        int numCores = Runtime.getRuntime().availableProcessors();

        executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(numCores);

        StopWatch timer = new StopWatch();
        timer.start();
        Future<?> p1 = executor.submit(new ProduceTripletTask());
        extraProducer = executor.submit(new ProduceTripletTask());
//        Future<?> p3 = executor.submit(new ProduceTripletTask());
        Future<?> c1 = executor.submit(new ConsumeTripletTask());
//        Future<?> c2 = executor.submit(new ConsumeTripletTask());
//        Future<?> c3 = executor.submit(new ConsumeTripletTask());



        try {
            count.await();
            executor.shutdownNow();
            System.out.println("TERMINATING");
            System.out.println(String.format("Time: %,d x 10e-3 s", timer.getTime()));

            long totalConsumerTime = consumerTimes.stream().reduce(0L, (aLong, aLong2) -> aLong + aLong2);
            long averageConsumerTime = totalConsumerTime / MAX_INTERACTIONS;
            long totalProducerTime = producerTimes.stream().reduce(0L, (aLong, aLong2) -> aLong + aLong2);
            long averageProducerTime = totalProducerTime / MAX_INTERACTIONS;

            System.out.println(String.format("Ave. consumer time: %,d x 10e-9 s", averageConsumerTime));
            System.out.println(String.format("Ave. producer time: %,d x 10e-9 s", averageProducerTime));
            db.shutdown();
        } catch (InterruptedException e) {
        }


    }

    static void printQueue() {
        int pos = 0;
        tripletQueue.forEach(triplet -> {
            System.out.println(String.format("%d: %s", pos, triplet));
        });
    }
}
