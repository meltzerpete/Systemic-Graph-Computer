package parallel;

import org.apache.commons.lang3.time.StopWatch;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

import static parallel.Manager.*;

/**
 * Created by Pete Meltzer on 05/08/17.
 */
public class ConsumeTripletTask implements Runnable {
    @Override
    public void run() {
        while (!Thread.currentThread().isInterrupted()) {
            Triplet triplet;
            try {
                StopWatch timer = new StopWatch();
                timer.start();

                triplet = tripletQueue.take();

                try (Transaction tx = db.beginTx()) {
                    Node s1 = db.getNodeById(triplet.s1);
                    Node s2 = db.getNodeById(triplet.s2);
                    triplet.function.accept(s1, s2);
//                    inQueueSet.remove(triplet.s1);
//                    inQueueSet.remove(triplet.s2);
                    int c = (int) count.getCount();
                    count.countDown();
                    if (c % 100 == 0) {
                        System.out.println(c);
                        if (tripletQueue.size() > (QUEUE_SIZE * 0.9) && !extraProducer.isCancelled()) {
                            System.out.println("Cancel extra producer");
                            extraProducer.cancel(true);
//                            extraConsumer = executor.submit(new ConsumeTripletTask());
                        } else if (tripletQueue.size() < (QUEUE_SIZE * 0.1) && extraProducer.isCancelled()) {
                            System.out.println("Enable extra producer");
//                            extraConsumer.cancel(true);
                            extraProducer = executor.submit(new ProduceTripletTask());
                        }
//                        System.out.println("Queue size: " + tripletQueue.size());
                    }
                    tx.success();
                    timer.stop();
                    consumerTimes.add(timer.getNanoTime());
                }
            } catch (InterruptedException e) {
//                e.printStackTrace();
            }
        }
    }
}
