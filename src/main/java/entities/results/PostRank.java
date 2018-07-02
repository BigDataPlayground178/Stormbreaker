package entities.results;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class PostRank {

    private Map<String, Long> rank = new TreeMap<>();

    public void addNewValue(Long post_id) {
        String id = String.valueOf(post_id);
        if (rank.get(id) == null) {
            rank.put(id, (long) 1);
        } else {
            rank.put(id, rank.get(id) + 1);
        }
    }

    public Map<String, Long> getTopRank() {
        return rank.entrySet().stream()
                .sorted(Map.Entry.<String, Long>comparingByValue().reversed())
                .limit(10)
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));


    }

    @Override
    public String toString() {
        Map<String,Long> toprank = getTopRank();
        StringBuilder result = new StringBuilder(System.currentTimeMillis() + " , ");
        for (Map.Entry<String, Long> entry : toprank.entrySet()) {
            result.append(entry.getKey()).append(" , ").append(entry.getValue()).append(" , ");
        }
        result.delete(result.length() - 3, result.length());
        return result.toString();
    }
}
