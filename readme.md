# Systemic Graph Computer

This project comprises three implementations of a Systemic Graph Computer.

The Systemic Graph Computer demonstrates a model of nature inspired computation in the environment of a graph database.

- Version I - a proof of concept
- Version II - a multithreaded implementation
- Version III - an adaptation to the model of Systemic Computation

The packages are as follows:

- `common` - classes common to all three implementations
- `nodeParser` - classes for parsing query strings - used by Version I and II
- `graphEngine` - classes relevant to Version I
- `parallel` - classes relevant to Version II
- `probability` - classes relevant to Version III

The `Execute` classes in `graphEngine`, `parallel`, and `probability` packages, each contain all procedures available for calling directly from the cypher-shell or Neo4j browser interface.

---

This project was cloned from the Neo4j procedure template. The original template may be found [here](https://github.com/neo4j-examples/neo4j-procedure-template).

This project requires a Neo4j 3.0.0 snapshot or milestone dependency.

## Building

This project uses maven, to build a jar-file with the procedure in this
project, simply package the project with maven:

    mvn clean package

This will produce a jar-file, `target/systemicGraphComputer.jar`,
that can be deployed in the `plugin` directory of your Neo4j instance.

## License

Apache License V2, see LICENSE
