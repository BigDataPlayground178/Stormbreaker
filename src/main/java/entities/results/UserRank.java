package entities.results;

import org.apache.flink.api.java.tuple.Tuple3;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static utils.StormbreakerConstants.USERS_RANKING_MAX;

/**
 * Class for user ranking by a given score
 */
public class UserRank {

    public Long ts = Long.MAX_VALUE;
    private Map<Long, Integer> rank = new TreeMap<>();

    /**
     * This method can be used to add a user to the growing ranking
     * @param user user tuple <user_id, score, timestamp>
     */
    public void addUser(Tuple3<Long, Integer, Long> user) {
        Long id = user.f0;
        Integer value = user.f1;
        if (rank.get(id) != null) {
            value = rank.get(id) + 1;
        }
        rank.put(id, value);
    }

    /**
     * This method leverages upon lambda functions to retrieve the configurable
     * top N of users
     * @return Map<Long, Integer> of user id and its score
     */
    public Map<Long, Integer> getTopRank() {
        return rank.entrySet().stream()
                .sorted(Map.Entry.<Long, Integer>comparingByValue().reversed())
                .limit(USERS_RANKING_MAX)
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));
    }

    public void setTs(Long ts) {
        this.ts = ts;
    }

    public Long getTs() {
        return this.ts;
    }


    public String toString() {
        Map<Long,Integer> toprank = getTopRank();
        StringBuilder result = new StringBuilder(ts + " , ");
        for (Map.Entry<Long, Integer> user : toprank.entrySet()) {
            result.append(user.getKey()).append(" , ").append(user.getValue()).append(" , ");
        }
        result.delete(result.length() - 3, result.length());
        return result.toString();
    }

}
