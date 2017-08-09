package parallel;

import org.neo4j.graphdb.DatabaseShutdownException;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import static parallel.Manager.*;

/**
 * Created by Pete Meltzer on 05/08/17.
 */
public class ConsumeTripletTask implements Runnable {

    private static ReentrantLock mutex = new ReentrantLock();
    private static AtomicInteger counter = new AtomicInteger(0);

    @Override
    public void run() {
        while (run) {
            Triplet triplet;
            try {

                //TODO time this part
                triplet = tripletQueue.take();

                try (Transaction tx = db.beginTx()) {

                    Node s1 = db.getNodeById(triplet.s1);
                    Node s2 = db.getNodeById(triplet.s2);

                    long s1ID = triplet.s1;
                    long s2ID = triplet.s2;

                    // acquire locks - nodes could be in any order so must be
                    // wrapped with mutex to avoid deadlock
                    mutex.lock();
                    try {
                        if (!nodeLocks.get(s1ID).tryLock()) {
                            // if s1 is locked - get the next triplet
                            continue;
                        }
                        if (!nodeLocks.get(s2ID).tryLock()) {
                            // if s2 is locked - get the next triplet
                            nodeLocks.get(s1ID).unlock();
                            continue;
                        }
                    } finally {
                        mutex.unlock();
                    }

                    if ((matchNode(triplet.contextEntry.s1, s1)
                            && matchNode(triplet.contextEntry.s2, s2))) {
                        tx.acquireWriteLock(s1);
                        tx.acquireWriteLock(s2);
                        triplet.contextEntry.function.accept(s1, s2);
                        tx.success();
                        count.countDown();
                        if (counter.getAndIncrement() % 100 == 0) {
                            System.out.println(counter.get() - 1);
//                            System.out.println((counter.get() - 1) + " queueSize: " + tripletQueue.size());
                        }
                    }
                    // commit transaction and release locks
                    nodeLocks.get(s1ID).unlock();
                    nodeLocks.get(s2ID).unlock();


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
