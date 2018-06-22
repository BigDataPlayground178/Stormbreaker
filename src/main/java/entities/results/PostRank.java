package entities.results;

import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Stream;

public class PostRank {

    private Map<Long, Long> rank = new TreeMap<>();

    public void addNewValue(Long post_id) {
        if (rank.get(post_id) == null) {
            rank.put(post_id, (long) 1);
        } else {
            rank.put(post_id, rank.get(post_id) + 1);
        }
    }

    public Stream getTopRank() {
        return rank.entrySet().stream()
                .sorted(Map.Entry.<Long, Long>comparingByValue().reversed())
                .limit(10);
    }

    public void printRank() {
        rank.entrySet().stream()
                .sorted(Map.Entry.<Long, Long>comparingByValue().reversed())
                .limit(10)
                .forEach(System.out::println); // or any other terminal method
    }
}
