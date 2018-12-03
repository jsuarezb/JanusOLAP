package ar.edu.itba;

import static ar.edu.itba.GraphUtils.getUser;
import static ar.edu.itba.GraphUtils.getUserName;
import static ar.edu.itba.GraphUtils.neighborPhones;
import static ar.edu.itba.GraphUtils.phonesOf;
import static ar.edu.itba.GraphUtils.toTraversal;
import static org.apache.tinkerpop.gremlin.process.traversal.P.without;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;

public class FinalQueriesNotOlap {

  public static boolean printTuples;

  public static void query2_a(JanusGraph graph) {
    Map<String, Map<String, Integer>> distancesMap =
        shortestDistanceBetweenUsers(graph, null, null, -1);
    PrintUtils.printDistances(distancesMap, printTuples);
  }

  public static void query2_b(JanusGraph graph) {
    Map<String, Map<String, Integer>> distancesMap =
        shortestDistanceBetweenUsers(graph, QueryFilters.query2_b_filter, null, -1);
    PrintUtils.printDistances(distancesMap, printTuples);
  }

  public static void query2_c(JanusGraph graph) {
    Map<String, Map<String, Integer>> distancesMap =
        shortestDistanceBetweenUsers(graph, QueryFilters.query2_c_filter, null, -1);
    PrintUtils.printDistances(distancesMap, printTuples);
  }

  public static void query2_d(JanusGraph graph, String user, int threshold) {
    Map<String, Map<String, Integer>> distancesMap =
        shortestDistanceBetweenUsers(graph, null, user, threshold);
    PrintUtils.printDistances(distancesMap, printTuples);
  }

  /**
   * Prints the shortest distance between users applying the corresponding filter, user as source
   * and threshold. If filter or user are null, then the corresponding one has no effect. If
   * threshold is negative, then it is considered as Integer.MAX_VALUE.
   * 
   * @param graph Graph to apply the algorithm.
   * @param filter Filter to be applied, that receives two phone vertices. (i.e: User phones not
   *        from the same city, or calls between phones with different operator). If null, a dummy
   *        always-true filter is applied.
   * @param user Source user to apply the algorithm to. If null the algorithm is applied for every
   *        pair of users.
   * @param threshold Distance between users will be lower than threshold. If is negative, then it
   *        is considered as Integer.MAX_VALUE.
   */
  public static Map<String, Map<String, Integer>> shortestDistanceBetweenUsers(JanusGraph graph,
      BiFunction<Vertex, Vertex, Boolean> filter, String user, int threshold) {
    GraphTraversal<Vertex, Vertex> phones;
    if (user != null) {
      // Base case for threshold 0 and 1.
      if (threshold == 0) {
        return new HashMap<>();
      } else if (threshold == 0) {
        Map<String, Map<String, Integer>> map = new HashMap<>();
        map.computeIfAbsent(user, x -> new HashMap<>()).put(user, 0);
        return map;
      }
      phones = phonesOf(graph, user);
    } else {
      phones = graph.traversal().V().has("type", "phone");
    }

    List<GraphTraversal<Vertex, Path>> shortestPaths = phones.toStream()
        .map(phone -> toTraversal(graph, phone.id())
            .store("boundary") // Used to store the vertices reached by phone.
            .until(trv -> trv.loops() + 1 >= (threshold < 0 ? Integer.MAX_VALUE : threshold))
            // Consider only those that haven't been reached yet, and add new reached to boundary.
            .repeat(neighborPhones().where(without("boundary")).aggregate("boundary"))
            .emit() // Emit the results.
            .filter(filter == null ? x -> true : trv -> {
              Vertex src = GraphUtils.getPathVertex(trv, 0);
              Vertex dst = GraphUtils.getPathVertex(trv, -1);
              return filter.apply(src, dst);
            }) // Filter required ones.
            .dedup() // Remove paths of same length between two vertices.
            .path())
        .collect(Collectors.toList());

    return toDistancesMap(graph, shortestPaths, user);
  }

  private static Map<String, Map<String, Integer>> toDistancesMap(
      JanusGraph graph, List<GraphTraversal<Vertex, Path>> paths, String user) {
    Map<String, Map<String, Integer>> distancesMap = new TreeMap<>();

    paths.stream().forEach(l -> l.toStream().forEach(p -> {
      String userName1 = getUserName(getUser((Vertex) p.get(0)));
      String userName2 = getUserName(getUser((Vertex) p.get(p.size() - 1)));
      int distance = (p.size() - 1) / 2; // Between 2 phones there's a call node.

      // Update distance between user1 and user2.
      distancesMap.computeIfAbsent(userName1, x -> new TreeMap<>())
          .compute(userName2,
              (x, currDist) -> currDist == null ? distance : (int) Math.min(currDist, distance));
    }));

    // Add username - username with distance 0 for every user o just `user` if provided.
    if (user == null) {
      graph.traversal().V().has("type", "user").toSet()
          .stream()
          .forEach(userV -> {
            String userName = getUserName(userV);
            distancesMap.computeIfAbsent(userName, x -> new TreeMap<>()).put(userName, 0);
          });
    } else {
      distancesMap.computeIfAbsent(user, x -> new TreeMap<>()).put(user, 0);
    }

    return distancesMap;
  }
}
