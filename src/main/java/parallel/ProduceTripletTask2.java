//package parallel;
//
//import org.neo4j.graphdb.Node;
//import org.neo4j.graphdb.Transaction;
//
//import java.util.concurrent.ThreadLocalRandom;
//
///**
// * Created by Pete Meltzer on 10/08/17.
// */
//@Deprecated
//public class ProduceTripletTask2 implements Runnable {
//
//    public ProduceTripletTask2(Manager manager) {
//        this.manager = manager;
//    }
//
//    private final Manager manager;
//
//    @Override
//    @SuppressWarnings("unchecked cast")
//    public void run() {
//        try (Transaction tx = manager.db.beginTx()) {
//            while(manager.run.get()) {
//
//                // choose a nodeID at random, and get its scope - better distribution of selection
//                long s1ID = manager.allNodeIDs[ThreadLocalRandom.current().nextInt(manager.totalNoOfNodes)];
//
//                // get a random parent scopeID
//                // if it doesn't have one - try next
//                Long[] scopesIDs = manager.parentScopes.get(s1ID);
//                if (scopesIDs.length == 0) {
//                    // node has no parent scopes - must be main
//                    continue;
//                }
//                long scopeID = scopesIDs[ThreadLocalRandom.current().nextInt(scopesIDs.length)];
//
//                // choose a random partner nodeID from the same scope
//                Long[] others = manager.nodesContainedInScope.get(scopeID);
//                // if only one node in scope - try next
//                if (others.length < 2) continue;
//
//                long s2ID = others[ThreadLocalRandom.current().nextInt(others.length)];
//                // ensure it is not the same as S1
//                while (s2ID == s1ID)
//                    s2ID = others[ThreadLocalRandom.current().nextInt(others.length)];
//
//                int noOfContexts = manager.contextArray.length;
//                int initialPosition = ThreadLocalRandom.current().nextInt(noOfContexts);
//
//                ContextEntry randomContextEntry = manager.contextArray[initialPosition];
//
//                int count = 0;
//                while (randomContextEntry.scope != scopeID && count < noOfContexts)
//                    randomContextEntry = manager.contextArray[(initialPosition + ++count) % manager.contextArray.length];
//
//                // if still can't find a matching context - try next pair of nodes
//                if (randomContextEntry.scope != scopeID) continue;
//
//                Node s1 = manager.db.getNodeById(s1ID);
//                Node s2 = manager.db.getNodeById(s2ID);
//
//                if (manager.matchNode(randomContextEntry.s1, s1)
//                        && manager.matchNode(randomContextEntry.s2, s2)) {
//                    try {
//                        // add to queue
//                        manager.tripletQueue.put(new Triplet(randomContextEntry, s1ID, s2ID));
//                    } catch (InterruptedException e) {
//                        e.printStackTrace();
//                    }
//                }
//            }
//        }
//    }
//}
