package entities.results;

import java.time.ZonedDateTime;
import java.util.HashMap;

/**
 * This class can be used to keep trace of the number of friendships
 * that are recorded in the stream
 */
public class FriendshipCount {

    private ZonedDateTime ts;
    private HashMap<String, Long> counters = new HashMap<String, Long>();

    /**
     * This method can be used to handle the counters increase of the friendships
     * @param counterID String, the identifier of the counter hour (e.g. 00, 23...)
     */
    public void incrementCounter(String counterID) {
        Long value = 1L;
        if (counters.containsKey(counterID))
            value = counters.get(counterID) + 1;
        counters.put(counterID, value);
    }

    /**
     * This method can be used to handle the counters increase of the friendships
     * @param counterID String, the identifier of the counter hour (e.g. 00, 23...)
     * @param newValue Long, to handle the counter increase of a given amount
     */
    public void incrementCounter(String counterID, Long newValue) {
        Long value = 1L;
        if (counters.containsKey(counterID))
            value = counters.get(counterID) + newValue;
        counters.put(counterID, value);
    }

    public ZonedDateTime getTs() {
        return ts;
    }

    public void setTs(ZonedDateTime ts) {
        this.ts = ts;
    }

    public HashMap<String, Long> getCounters() {
        return counters;
    }

    public void setCounters(HashMap<String, Long> counters) {
        this.counters = counters;
    }

    public String toString() {
        StringBuilder stamp = new StringBuilder(String.valueOf(getTs().toInstant().toEpochMilli()) + " , ");
        for (HashMap.Entry<String, Long> entry : counters.entrySet()) {
            stamp.append(entry.getKey()).append(" , ").append(entry.getValue()).append(" , ");
        }
        stamp.delete(stamp.length() - 3, stamp.length());
        return stamp.toString();
    }
}
