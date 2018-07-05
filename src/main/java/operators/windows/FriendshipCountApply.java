package operators.windows;

import entities.records.FriendshipRecord;
import entities.results.FriendshipCount;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import static utils.StormbreakerConstants.HOUR_COUNTER_PREFIX;

/**
 * This class is an extension of an AllWindowFunction to apply the sum of the friendship
 * interactions in a given window
 */
public class FriendshipCountApply implements AllWindowFunction<FriendshipRecord, FriendshipCount, TimeWindow> {

    @Override
    public void apply(TimeWindow timeWindow, Iterable<FriendshipRecord> friendshipRecords, Collector<FriendshipCount> out) throws Exception {
        // instantiating a new FriendshipCount object
        FriendshipCount fc = new FriendshipCount();
        // iterating over friendship records
        for (FriendshipRecord fr : friendshipRecords) {
            if (fc.getTs() == null) fc.setTs(fr.getFriendshipDate());
            // retrieving counter identifier
            String counterID = HOUR_COUNTER_PREFIX + fr.getRawHour();
            // updating counter
            fc.incrementCounter(counterID);
        }

        // collecting new friendship counter
        out.collect(fc);
    }
}
