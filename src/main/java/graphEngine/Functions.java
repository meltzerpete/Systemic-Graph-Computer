package graphEngine;


import org.neo4j.graphdb.Node;

/**
 * Created by Pete Meltzer on 11/07/17.
 */
enum Functions {

    NOP {
        @Override
        void perform(Node context, Node S1, Node S2) {
            //TODO NOP()
        }
    },

    ADD {
        @Override
        void perform(Node context, Node S1, Node S2) {
            //TODO ADD()
        }
    };
    //TODO add remaining functions

    abstract void perform(Node context, Node S1, Node S2);
}