package probability;

import graphEngine.Components;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.neo4j.graphdb.*;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Created by Pete Meltzer on 14/08/17.
 */
public class Manager {

    private int MAX_INTERACTIONS;
    GraphDatabaseService db;
    long[] arrayOfScopeIDs;
    ConcurrentHashMap<Long,Long[]> nodesContainedInScope;

    public Manager(int MAX_INTERACTIONS, GraphDatabaseService db) {
        this.MAX_INTERACTIONS = MAX_INTERACTIONS;
        this.db = db;
    }


    public void go() {

        StopWatch timer = new StopWatch();
        timer.start();

//        try (Transaction tx = db.beginTx()) {

            // for every scope create an array for the systems it contains
            int nScopes = (int) db.findNodes(Components.SCOPE).stream().count();
            nodesContainedInScope = new ConcurrentHashMap<>();

            ArrayList<Long> arrayListOfScopes = new ArrayList<>(nScopes);

            db.findNodes(Components.SCOPE).forEachRemaining(scope -> {

                ArrayList<Long> containedNodesArrayList = new ArrayList<>();
                ((ResourceIterator<Relationship>)
                        scope.getRelationships(Components.CONTAINS, Direction.OUTGOING).iterator()).stream()
                        .map(Relationship::getEndNode)
                        .forEach(node -> {
                            if (node.hasProperty(Components.probability)) {
                                int p = (int) node.getProperty(Components.probability);
                                for (int i = 0; i < p; i++) {
                                    containedNodesArrayList.add(node.getId());
                                }
                            } else {
                                containedNodesArrayList.add(node.getId());
                            }
                        });

                Long[] containedNodesArray = new Long[containedNodesArrayList.size()];
                containedNodesArrayList.toArray(containedNodesArray);
                nodesContainedInScope.put(scope.getId(), containedNodesArray);

                arrayListOfScopes.add(scope.getId());
            });

            Long[] objectLongArrayOfScopes = new Long[nScopes];
            arrayListOfScopes.toArray(objectLongArrayOfScopes);
            arrayOfScopeIDs = ArrayUtils.toPrimitive(objectLongArrayOfScopes);

        Functions functions = new Functions(this);

        int count = 0;

        // EXECUTION

        while (count < MAX_INTERACTIONS) {
            int index = ThreadLocalRandom.current().nextInt(arrayOfScopeIDs.length);

            long scopeID = arrayOfScopeIDs[index];

            Long[] containedIDs = nodesContainedInScope.get(scopeID);

            if (containedIDs.length < 2) continue;

            int s1IDIndex = ThreadLocalRandom.current().nextInt(containedIDs.length);
            int s2IDIndex = ThreadLocalRandom.current().nextInt(containedIDs.length);

            long s1ID = containedIDs[s1IDIndex];
            long s2ID = containedIDs[s2IDIndex];

            while (s1ID == s2ID)
                s2ID = containedIDs[(++s2IDIndex) % containedIDs.length];

            if (functions.interact(s1ID, s2ID)) {
                count++;
//                if ((count % 100) == 0) System.out.println(count);
            }

        }
        timer.stop();
        System.out.println(String.format("Time: %,d x 10e-3 s", timer.getTime()));
    }
}
