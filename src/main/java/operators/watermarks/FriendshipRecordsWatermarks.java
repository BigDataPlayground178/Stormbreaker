package operators.watermarks;

import entities.records.FriendshipRecord;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

import static utils.StormbreakerConstants.DATASET_START_TIMESTAMP;

public class FriendshipRecordsWatermarks implements AssignerWithPeriodicWatermarks<FriendshipRecord> {

    private long currentMaxTimestamp = DATASET_START_TIMESTAMP;

    @Nullable
    public Watermark getCurrentWatermark() {
        return new Watermark(currentMaxTimestamp);
    }

    public long extractTimestamp(FriendshipRecord friendshipRecord, long l) {
        long timestamp = friendshipRecord.getFriendshipDate().toInstant().toEpochMilli();        // milliseconds
        currentMaxTimestamp = Math.max(currentMaxTimestamp, timestamp);
        return timestamp;
    }
}
