package entities.results;

import java.util.Date;
import java.util.HashMap;

public class FriendshipCount {

    private Date ts;
    private HashMap<String, Long> counters = new HashMap<String, Long>();

    public void incrementCounter(String counterID) {
        counters.put(counterID, counters.getOrDefault(counterID, 0L) + 1);
    }

    public void incrementCounter(String counterID, Long value) {
        counters.put(counterID, counters.getOrDefault(counterID, 0L) + value);
    }

    public Date getTs() {
        return ts;
    }

    public void setTs(Date ts) {
        this.ts = ts;
    }

    public HashMap<String, Long> getCounters() {
        return counters;
    }

    public void setCounters(HashMap<String, Long> counters) {
        this.counters = counters;
    }
}
