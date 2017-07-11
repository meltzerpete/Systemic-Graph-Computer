package graphEngine;

import org.apache.commons.lang3.time.StopWatch;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Description;
import org.neo4j.procedure.Mode;
import org.neo4j.procedure.Procedure;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by pete on 05/07/17.
 */
public class Random {

    @Context
    public GraphDatabaseService db;

    @Context
    public Log log;

    @Procedure(value = "graphEngine.randomTest", mode= Mode.WRITE)
    @Description("")
    public void test() {

        // TODO instead of this lovely functional method/while loop.. perhaps use a counter++/-- every time READY++/--
        // can save this for selecting random S1/S2 etc.
        // TODO check when while loop in separate function

        StopWatch t1 = new StopWatch();
        StopWatch t2 = new StopWatch();
        StopWatch t3 = new StopWatch();

        t1.start();
        t1.suspend();
        t2.start();
        t2.suspend();
        t3.start();
        t3.suspend();


        for (int i = 0; i < 1000; i++)
            db.createNode();

        List<Integer> list1 = new LinkedList<>();
        List<Integer> list2 = new LinkedList<>();

        for (int i = 0; i < 10; i++) {
            ResourceIterable<Node> res = db.getAllNodes();

            t2.reset();
            t2.start();
            AtomicInteger count2 = new AtomicInteger(2);
            Node rNode2 = res.stream()
                    .reduce((node, node2) -> Math.random() > 1.0 / count2.getAndIncrement() ? node : node2).get();
            t2.stop();

            t1.reset();
            t1.start();
            final int[] count1 = {2};
            Node rNode1 = res.stream()
                    .reduce((node, node2) -> Math.random() > 1.0 / count1[0]++ ? node : node2).get();
            t1.stop();

            t3.reset();
            t3.start();
            ResourceIterator<Node> it = res.iterator();
            Node acc = it.next();
            int count3 = 2;
            while(it.hasNext()) {
                Node next = it.next();
                acc = Math.random() > 1.0 / count3++ ? acc : next;
            }
            t3.stop();

            list1.add((int) rNode1.getId());
            list2.add((int) rNode2.getId());
        }

        int[] count1 = new int[1000];
        int[] count2 = new int[1000];

        list1.forEach(integer -> count1[integer]++);
        list2.forEach(integer -> count2[integer]++);

        System.out.println("t1: " + t1.getNanoTime() + " ns");
        System.out.println("t2: " + t2.getNanoTime() + " ns");
        System.out.println("t3: " + t3.getNanoTime() + " ns");

    }

}
