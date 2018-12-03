package ar.edu.itba;

import java.util.Map;

public class PrintUtils {

  public static <S extends Comparable<? super S>> void printDistances(
      Map<S, Map<S, Integer>> distancesMap,
      boolean printTuples) {
    int rowsCount = 0;
    StringBuilder builder = new StringBuilder();

    for (Map.Entry<S, Map<S, Integer>> e1 : distancesMap.entrySet()) {
      S member1 = e1.getKey();
      for (Map.Entry<S, Integer> e2 : e1.getValue().entrySet()) {
        S member2 = e2.getKey();
        if (member1.compareTo(member2) > 0) {
          continue;
        }
        int distance = e2.getValue();
        if (printTuples) {
          builder.append(member1);
          builder.append("  ");
          builder.append(member2);
          builder.append("  ");
          builder.append(distance);
          builder.append("\n");
        }
        rowsCount++;
      }
    }

    System.out.print(builder.toString());
    System.out.println(rowsCount + " rows.");
  }
}
