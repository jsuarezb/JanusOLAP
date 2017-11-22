package ar.edu.itba;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphTransaction;

import java.util.Iterator;

public class Operations {

    JanusGraph graph;
    JanusGraphTransaction tx;

    public Operations(JanusGraph graph) {
        this.graph = graph;
        this.tx = graph.newTransaction();
    }

    public void climb(String bottom, String top) {
        GraphTraversal<Vertex, Vertex> bottomTraversal = graph.traversal().V().hasLabel(bottom);
        GraphTraversal<Vertex, Vertex> topTraversal = bottomTraversal.asAdmin().clone();
        GraphTraversal<Vertex, Vertex> auxTraversal;

        int i = 0;
        while (true) {
            System.out.println(i++);
            topTraversal = topTraversal.out("extendsFrom");
            auxTraversal = topTraversal.asAdmin().clone();

//            auxTraversal.asAdmin().clone().V().toStream().forEach(vertex -> {
//                System.out.println(String.format("Label: %s, Property: %s", vertex.label(), vertex.property("value")));
//            });

            if (auxTraversal.hasLabel(top).hasNext())
                break;
        }

//        System.out.println(topTraversal.V().valueMap());

        topTraversal.hasLabel(top).toStream().forEach(vertex -> {
            System.out.println(String.format("Label: %s, Property: %s", vertex.label(), vertex.property("value")));
        });
    }
}
