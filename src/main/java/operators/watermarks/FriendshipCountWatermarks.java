package operators.watermarks;

import entities.results.FriendshipCount;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

import static utils.StormbreakerConstants.DATASET_START_TIMESTAMP;

/**
 * This class can be used to extract watermarks from friendship records (in query 1)
 */
public class FriendshipCountWatermarks implements AssignerWithPeriodicWatermarks<FriendshipCount> {

    private long currentMaxTimestamp = DATASET_START_TIMESTAMP;

    @Nullable
    public Watermark getCurrentWatermark() {
        return new Watermark(currentMaxTimestamp);
    }

    public long extractTimestamp(FriendshipCount friendshipCount, long l) {
        long timestamp;
        if (friendshipCount.getTs() != null)
            timestamp = friendshipCount.getTs().toInstant().toEpochMilli();             // milliseconds
        else
            timestamp = currentMaxTimestamp;
        currentMaxTimestamp = Math.max(currentMaxTimestamp, timestamp);
        return timestamp;
    }
}
