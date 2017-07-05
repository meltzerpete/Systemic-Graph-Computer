package GraphComponents;

/**
 * Created by pete on 04/07/17.
 */
public enum Function {
    NOP {
        @Override
        public void compute(System s1, System s2) {

        }
    },
    ADD {
        @Override
        public void compute(System s1, System s2) {

        }
    },
    ADDe {
        @Override
        public void compute(System s1, System s2) {

        }
    },
    SUBTRACT {
        @Override
        public void compute(System s1, System s2) {

        }
    },
    SUBTRACTe {
        @Override
        public void compute(System s1, System s2) {

        }
    },
    MULT {
        @Override
        public void compute(System s1, System s2) {

        }
    },
    MULTe {
        @Override
        public void compute(System s1, System s2) {

        }
    },
    DIV {
        @Override
        public void compute(System s1, System s2) {

        }
    },
    DIVe {
        @Override
        public void compute(System s1, System s2) {

        }
    },
    AND {
        @Override
        public void compute(System s1, System s2) {

        }
    },
    OR {
        @Override
        public void compute(System s1, System s2) {

        }
    },
    EOR {
        @Override
        public void compute(System s1, System s2) {

        }
    },
    ZERO {
        @Override
        public void compute(System s1, System s2) {

        }
    },
    ESCAPE {
        @Override
        public void compute(System s1, System s2) {

        }
    },
    CAPTURE {
        @Override
        public void compute(System s1, System s2) {

        }
    },
    PRINT {
        @Override
        public void compute(System s1, System s2) {

        }
    },
    COPY {
        @Override
        public void compute(System s1, System s2) {

        }
    },
    ISZERO {
        @Override
        public void compute(System s1, System s2) {

        }
    };

    public abstract void compute(System s1, System s2);
}
