package parallel;

import org.neo4j.graphdb.DatabaseShutdownException;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

import java.util.Arrays;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

import static parallel.Manager.*;

/**
 * Created by Pete Meltzer on 05/08/17.
 */
public class ConsumeTripletTask implements Runnable {

    private static Semaphore mutex = new Semaphore(1);
    private static AtomicInteger counter = new AtomicInteger(0);

    @Override
    public void run() {
        while (run) {
            Triplet triplet;
            try {

                triplet = tripletQueue.take();

                try (Transaction tx = db.beginTx()) {
                    Node s1 = db.getNodeById(triplet.s1);
                    Node s2 = db.getNodeById(triplet.s2);

                    // acquire locks - nodes could be in any order so must be
                    // wrapped with mutex to avoid deadlock
                    mutex.acquire();
                    nodeLocks[(int) triplet.s1].acquire();
                    nodeLocks[(int) triplet.s2].acquire();
                    mutex.release();
                    if ((matchNode(triplet.contextEntry.s1, s1) && matchNode(triplet.contextEntry.s2, s2))) {
                        triplet.contextEntry.function.accept(s1, s2);
                        if (counter.getAndIncrement() % 100 == 0) {
//                            System.out.println(c);
                            System.out.println((counter.get() - 1) + " queueSize: " + tripletQueue.size());
//                            System.out.println(String.format("Current: %s", triplet));
//                            System.out.println(Arrays.toString(db.getAllLabels().stream().toArray()));
                        }
                    }
                    // commit transaction and release locks
                    tx.success();
                    nodeLocks[(int) triplet.s1].release();
                    nodeLocks[(int) triplet.s2].release();
                    count.countDown();


                } catch (DatabaseShutdownException e) {
                    if (run) e.printStackTrace();
                    // otherwise ignore exception
                }

            } catch (InterruptedException e) {
//                e.printStackTrace();
            }
        }
    }
}
