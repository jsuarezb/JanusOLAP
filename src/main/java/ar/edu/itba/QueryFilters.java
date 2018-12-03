package ar.edu.itba;

import static ar.edu.itba.GraphUtils.getCity;
import static ar.edu.itba.GraphUtils.getOperator;
import static ar.edu.itba.GraphUtils.getUser;
import java.util.function.BiFunction;
import org.apache.tinkerpop.gremlin.structure.Vertex;

public class QueryFilters {

  static BiFunction<Vertex, Vertex, Boolean> query2_b_filter =
      (phone1, phone2) -> !getOperator(phone1)
          .equals(getOperator(phone2));

  static BiFunction<Vertex, Vertex, Boolean> query2_c_filter = 
      (phone1, phone2) -> !getCity(getUser(phone1))
          .equals(getCity(getUser(phone2)));
  
//  static Predicate<Traverser<Vertex>> query2_b_filter =
//      trv -> !getOperator(getPhone(trv, 0)) // Operator of phone of User (1)
//          .equals(getOperator(getPhone(trv, -1))); // Operator of phone of User (2)
//
//  static Predicate<Traverser<Vertex>> query2_c_filter =
//      trv -> !getUser(trv, 0) // Source of path User (1)
//          .edges(Direction.OUT, "extendsFrom").next().inVertex() // City of User (1)
//          .equals(getUser(trv, -1) // Dest of path User (2)
//              .edges(Direction.OUT, "extendsFrom").next().inVertex()); // City of User (2)

}
