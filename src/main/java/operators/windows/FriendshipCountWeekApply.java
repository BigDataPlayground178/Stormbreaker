package operators.windows;

import entities.results.FriendshipCount;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

public class FriendshipCountWeekApply implements AllWindowFunction<FriendshipCount, FriendshipCount, TimeWindow> {

    @Override
    public void apply(TimeWindow timeWindow, Iterable<FriendshipCount> friendshipCounts, Collector<FriendshipCount> out) throws Exception {
        // instantiating a new FriendshipCount object
        FriendshipCount fc = new FriendshipCount();
        // iterating over friendship records
        for (FriendshipCount counter : friendshipCounts) {
            // setting timestamp
            if (fc.getTs() == null) fc.setTs(counter.getTs());

            // updating counter
            HashMap<String, Long> c = counter.getCounters();
            for (Map.Entry<String, Long> entry : c.entrySet()) {
                fc.incrementCounter(entry.getKey(), entry.getValue());
            }
        }

        // collecting new friendship counter
        out.collect(fc);
    }
}
