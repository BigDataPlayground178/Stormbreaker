package operators.windows;

import entities.records.FriendshipRecord;
import entities.results.FriendshipCount;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class FriendshipCountDayApply implements WindowFunction<FriendshipRecord, FriendshipCount, Integer, TimeWindow> {
    public void apply(Integer integer, TimeWindow timeWindow, Iterable<FriendshipRecord> iterable, Collector<FriendshipCount> collector) throws Exception {
        // TODO
    }
}
