package entities.results;

import java.time.ZonedDateTime;
import java.util.HashMap;

public class FriendshipCount {

    private ZonedDateTime ts;
    private HashMap<String, Long> counters = new HashMap<String, Long>();

    public void incrementCounter(String counterID) {
        Long value = 1L;
        if (counters.containsKey(counterID))
            value = counters.get(counterID) + 1;
        counters.put(counterID, value);
        //counters.put(counterID, counters.getOrDefault(counterID, 0L) + 1);
    }

    public void incrementCounter(String counterID, Long newValue) {
        Long value = 1L;
        if (counters.containsKey(counterID))
            value = counters.get(counterID) + newValue;
        counters.put(counterID, value);
        //counters.put(counterID, counters.getOrDefault(counterID, 0L) + value);
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
