package GraphComponents;

/**
 * Created by pete on 04/07/17.
 */
public enum Function {
    NOP {
        @Override
        public void compute(SCSystem s1, SCSystem s2) {
            System.out.println("I'm a NOP system");
        }
    },
    ADD {
        @Override
        public void compute(SCSystem s1, SCSystem s2) {
            System.out.println("I'm an ADD system");
        }
    },
    SUBTRACT {
        @Override
        public void compute(SCSystem s1, SCSystem s2) {
            System.out.println("I'm a SUBTRACT system");
        }
    },
    ESCAPE {
        @Override
        public void compute(SCSystem s1, SCSystem s2) {
            System.out.println("I'm an ESCAPE system");
        }
    },
    PRINT {
        @Override
        public void compute(SCSystem s1, SCSystem s2) {
            System.out.println("I'm a PRINT system");
        }
    };

    public abstract void compute(SCSystem s1, SCSystem s2);
}
