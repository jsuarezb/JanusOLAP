package ar.edu.itba;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.in;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.stream.StreamSupport;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;

public class GraphUtils {

  private static final Map<String, Set<Object>> userPhoneIdsCache = new HashMap<>();

  public static GraphTraversal<Vertex, Vertex> getNeighborPhones(JanusGraph graph, Vertex phone) {
    return toTraversal(graph, phone).in("integratedBy") // Calls of Phone (1)
        .out("integratedBy"); // Phones (2) that Phone (1) had calls with
  }

  public static GraphTraversal<Object, Vertex> neighborPhones() {
    return in("integratedBy") // Calls of Phone (1)
        .out("integratedBy"); // Phones (2) that Phone (1) had calls with
  }
  
  public static GraphTraversal<Vertex, Vertex> phonesOf(JanusGraph graph, String user) {
    return toTraversal(graph, phoneIdsOf(graph, user));
  }

  public static Set<Object> phoneIdsOf(JanusGraph graph, String user) {
    return userPhoneIdsCache.computeIfAbsent(user, x -> graph.traversal().V()
        .has("type", "user").has("value", user) // User vertex
        .in("extendsFrom") // Phones of user
        // Graph ids of phones of user
        .id().toSet());
  }

  public static Vertex getOperator(Vertex phone) {
    return getParent(phone, "operator");
  }

  public static Vertex getUser(Vertex phone) {
    return getParent(phone, "user");
  }

  static Vertex getCity(Vertex user) {
    return getParent(user, "city");
  }

  public static String getUserName(Vertex user) {
    return user.value("value");
  }

  private static Vertex getParent(Vertex phone, String type) {
    return StreamSupport.stream(
        ((Iterable<Edge>) () -> phone.edges(Direction.OUT, "extendsFrom")).spliterator(), true)
        // Filter only nodes of the given type.
        .filter(edge -> edge.inVertex().value("type").equals(type))
        .findFirst().get() // All phones should have an operator.
        .inVertex();
  }

  // private static Vertex getPhone(Traverser<Vertex> trv, int index) {
  // return getVertex(trv, (int) Math.floor(index / 2.0),
  // (index % 2 == (index >= 0 ? 0 : -1)) ? 1 : 3);
  // }
  //
  // private static Vertex getUser(Traverser<Vertex> trv, int index) {
  // return getVertex(trv, index, 0);
  // }

  static Vertex getPathVertex(Traverser<Vertex> trv, int index) {
    Path path = trv.path();
    int newIndex = 4 * index;
    if (index < 0) {
      newIndex = path.size() - 1 + 4 * (index + 1);
    }
    if (newIndex < 0 || newIndex >= path.size()) {
      throw new NoSuchElementException(String.format("Path size is %d, trying to access %d.",
          path.size(), newIndex));
    }
    return path.get(newIndex);
  }

  static GraphTraversal<Vertex, Vertex> toTraversal(JanusGraph graph, Object... ids) {
    return graph.traversal().V(ids);
  }
}
