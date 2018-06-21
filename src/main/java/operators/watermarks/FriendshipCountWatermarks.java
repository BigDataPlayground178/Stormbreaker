package operators.watermarks;

import entities.results.FriendshipCount;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

public class FriendshipCountWatermarks implements AssignerWithPeriodicWatermarks<FriendshipCount> {

    private long currentMaxTimestamp = 0;

    @Nullable
    public Watermark getCurrentWatermark() {
        return new Watermark(currentMaxTimestamp);
    }

    public long extractTimestamp(FriendshipCount friendshipCount, long l) {
        long timestamp = friendshipCount.getTs().getTime();        // milliseconds
        currentMaxTimestamp = Math.max(currentMaxTimestamp, timestamp);
        return timestamp;
    }
}
