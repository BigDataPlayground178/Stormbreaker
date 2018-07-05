package entities.results;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * Class for Post ranking by comments
 */
public class PostRank {

    private Long ts = Long.MAX_VALUE;
    private Map<String, Long> rank = new TreeMap<>();


    /**
     * Increment the number of comments that a post received
     * @param post_id id of the commented post
     */
    public void addNewValue(Long post_id) {
        String id = String.valueOf(post_id);
        Long value = 1L;
        if (rank.get(id) != null) {
            value = rank.get(id) + 1;
        }
        rank.put(id, value);
    }

    /**
     * Return the top 10 of the most commented posts
     * @return Map<String,Long> of PostID / Number of Comments it received
     */
    public Map<String, Long> getTopRank() {
        return rank.entrySet().stream()
                .sorted(Map.Entry.<String, Long>comparingByValue().reversed())
                .limit(10)
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));
    }

    @Override
    public String toString() {
        Map<String,Long> toprank = getTopRank();
        StringBuilder result = new StringBuilder(ts + " , ");
        for (Map.Entry<String, Long> entry : toprank.entrySet()) {
            result.append(entry.getKey()).append(" , ").append(entry.getValue()).append(" , ");
        }
        result.delete(result.length() - 3, result.length());
        return result.toString();
    }

    public void setTs(Long ts) {
        this.ts = ts;
    }

    public Long getTS() {
        return this.ts;
    }
}
