package graphEngine;


import org.neo4j.graphdb.Node;

/**
 * Created by Pete Meltzer on 11/07/17.
 */
enum Functions {

    NOP {
        @Override
        void perform(Node context, Node s1, Node s2) {
            //TODO NOP()
        }
    },

    ADD {
        @Override
        void perform(Node context, Node s1, Node s2) {

            long result = ((long) s1.getProperty("data")) + ((long) s2.getProperty("data"));
            s1.setProperty("data", result);
            s2.getProperty("data", ((long) 0));
        }
    },
    ESCAPE {
        @Override
        void perform(Node context, Node s1, Node s2) {
            //TODO ESCAPE()
        }
    },
    SUBTRACT {
        @Override
        void perform(Node context, Node s1, Node s2) {

            long result = ((long) s1.getProperty("data")) - ((long) s2.getProperty("data"));
            s1.setProperty("data", result);
            s2.setProperty("data", ((long) 0));
        }
    },
    SUBTRACTe {
        @Override
        void perform(Node context, Node s1, Node s2) {
            //TODO SUBTRACTe
        }
    };
    //TODO add remaining functions

    abstract void perform(Node context, Node s1, Node s2);
}