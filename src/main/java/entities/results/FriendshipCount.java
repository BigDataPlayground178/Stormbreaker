package entities.results;

import java.util.Date;
import java.util.HashMap;

public class FriendshipCount {

    private Date ts;
    private HashMap<String, Long> counters = new HashMap<String, Long>();

    public void incrementCounter(String counterID) {
        counters.put(counterID, counters.get(counterID) + 1);
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
