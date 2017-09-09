package parallel;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

/**
 * Created by Pete Meltzer on 05/08/17.
 * <p>The Consumer implementation for parallel execution
 *  - takes triplets from the queue, checks they still match
 *  and performs the transformations.</p>
 */
public class ConsumeTripletTask implements Runnable {

    private final Manager manager;

    /**
     * Instantiates a new ConsumeTripletTask.
     * @param manager An instance of the Manager.
     */
    public ConsumeTripletTask(Manager manager) {
        this.manager = manager;
    }

    /**
     * Main execution cycle.
     */
    @Override
    public void run() {
        long s1ID;
        long s2ID;

        while (manager.run.get()) {
            Triplet triplet;

            try {
                triplet = manager.tripletQueue.take();
                // check for poison
                if (triplet.contextEntry == null) {
                    // put the poison back for another thread
                    manager.tripletQueue.put(triplet);
                    // terminate loop
                    break;
                }
                s1ID = triplet.s1;
                s2ID = triplet.s2;
            } catch (InterruptedException e) {
                // terminate loop
                break;
            }

            try (Transaction tx = manager.db.beginTx()) {

                Node s1 = manager.db.getNodeById(triplet.s1);
                Node s2 = manager.db.getNodeById(triplet.s2);

                // acquire locks - nodes could be in any order so must be
                // wrapped with consumerMutex to avoid deadlock
                manager.consumerMutex.lockInterruptibly();
                try {
                    if (!manager.nodeLocks.get(s1ID).tryLock()) {
                        // if s1 is locked - get the next triplet
                        continue;
                    }
                    if (!manager.nodeLocks.get(s2ID).tryLock()) {
                        // if s2 is locked - unlock s1 and get the next triplet
                        manager.nodeLocks.get(s1ID).unlock();
                        continue;
                    }
                } finally {
                    manager.consumerMutex.unlock();
                }

                if ((manager.matchNode(triplet.contextEntry.s1, s1)
                        && manager.matchNode(triplet.contextEntry.s2, s2))) {
                    tx.acquireWriteLock(s1);
                    tx.acquireWriteLock(s2);
                    triplet.contextEntry.function.accept(s1, s2);
                    tx.success();
                    manager.count.countDown();
                    if (manager.upCounter.getAndIncrement() % 100 == 0) {
                            System.out.println((manager.upCounter.get() - 1) + " queueSize: " + manager.tripletQueue.size());
                            manager.timingLog.append(System.currentTimeMillis());
                            manager.timingLog.append("\n");
                    }
                }
                // commit transaction and release locks
                manager.nodeLocks.get(s1ID).unlock();
                manager.nodeLocks.get(s2ID).unlock();


            } catch (InterruptedException e) {
                // terminate loop
                break;
            } finally {
                if (manager.nodeLocks.get(s1ID).isHeldByCurrentThread()) {
                    manager.nodeLocks.get(s1ID).unlock();
                }
                if (manager.nodeLocks.get(s2ID).isHeldByCurrentThread()) {
                    manager.nodeLocks.get(s2ID).unlock();
                }
            }
        }
    }
}
