package GraphComponents;

/**
 * Created by pete on 04/07/17.
 */
public enum Function {
    NOP {
        @Override
        public void compute(Triplet triplet) {
            System.out.println("I'm a NOP system");
        }
    },
    ADD {
        @Override
        public void compute(Triplet triplet) {
            System.out.println("I'm an ADD system");
        }
    },
    SUBTRACT {
        @Override
        public void compute(Triplet triplet) {
            System.out.println("Performing subtraction on " + triplet);
            long a = (long) triplet.s1.getProperty("data");
            long b = (long) triplet.s2.getProperty("data");
            long c = a - b;
            triplet.s1.setProperty("data", c);
            triplet.s2.setProperty("data", (long) 0);
            System.out.println("State is now " + triplet);
        }
    },
    SUBTRACTe {
        @Override
        public void compute(Triplet triplet) {
            System.out.println("Performing subtract escape on " + triplet);
            SUBTRACT.compute(triplet);
            ESCAPE.compute(triplet);
            System.out.println("State is now " + triplet);
        }
    },
    ESCAPE {
        @Override
        public void compute(Triplet triplet) {
            System.out.println("I'm an ESCAPE system");
        }
    },
    PRINT {
        @Override
        public void compute(Triplet triplet) {
            System.out.println("I'm a PRINT system");
        }
    };

    public abstract void compute(Triplet triplet);
}
