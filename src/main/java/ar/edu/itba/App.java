package ar.edu.itba;

import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.core.util.JanusGraphCleanup;

import java.io.IOException;
import java.util.Iterator;

public class App {

    public static void main(String[] args) throws IOException {
        String path = args[0];

        final JanusGraph graph = JanusGraphFactory.build()
                .set("storage.backend", "cassandra")
                .set("storage.hostname", "10.16.6.21,10.16.6.22,10.16.6.23,10.16.6.24")
                .set("storage.cassandra.replication-factor", 2)
                .set("storage.cassandra.keyspace", "tcolloca")
                .set("schema.default", "none")
                .set("storage.username", "tcolloca")
                .set("storage.password", "tcolloca")
                .open();

        System.out.println("Closed and cleared graph");

        buildSchema(graph);

        System.out.println("Schema built");

        DataReader reader = new DataReader(graph, path);
        reader.buildGraph();

        graph.traversal().V().toStream().forEach(vertex -> {
            for (Iterator<VertexProperty<Object>> it = vertex.properties(); it.hasNext(); ) {
                VertexProperty p = it.next();
                System.out.println(p.label());
                System.out.println(p.value());
            }

            System.out.println(vertex.label());
        });

        graph.close();
    }

    private static void buildSchema(JanusGraph graph) {
        JanusGraphManagement mgmt = graph.openManagement();

        addVertexLabel(mgmt, "phone");
        addVertexLabel(mgmt, "user");
        addVertexLabel(mgmt, "city");
        addVertexLabel(mgmt, "country");
        addVertexLabel(mgmt, "allLocations");

        addVertexLabel(mgmt, "operator");
        addVertexLabel(mgmt, "allOperators");

        addVertexLabel(mgmt, "timestamp");
        addVertexLabel(mgmt, "day");
        addVertexLabel(mgmt, "month");
        addVertexLabel(mgmt, "year");
        addVertexLabel(mgmt, "allTimes");

        addVertexLabel(mgmt, "call");

        addEdgeLabel(mgmt, "calledBy");
        addEdgeLabel(mgmt, "integratedBy");
        addEdgeLabel(mgmt, "atTime");

        addEdgeLabel(mgmt, "extendsFrom");

        addPropertyKey(mgmt, "value", String.class, Cardinality.SINGLE);
        addPropertyKey(mgmt, "duration", Integer.class, Cardinality.LIST);

        mgmt.commit();
    }

    private static void addVertexLabel(JanusGraphManagement mgmt, String label) {
        if (mgmt.getVertexLabel(label) != null)
            return;

        mgmt.makeVertexLabel(label).make();
    }

    private static void addEdgeLabel(JanusGraphManagement mgmt, String label) {
        if (mgmt.getEdgeLabel(label) != null)
            return;

        mgmt.makeEdgeLabel(label).make();
    }

    private static void addPropertyKey(JanusGraphManagement mgmt, String label, Class dataType, Cardinality cardinality) {
        if (mgmt.getPropertyKey(label) != null)
            return;

        mgmt.makePropertyKey(label).dataType(dataType).cardinality(cardinality).make();
    }
}
