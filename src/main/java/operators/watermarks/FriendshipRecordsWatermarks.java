package operators.watermarks;

import entities.records.FriendshipRecord;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

/**
 * This class can be used to extract watermarks from friendship records
 */
public class FriendshipRecordsWatermarks implements AssignerWithPeriodicWatermarks<FriendshipRecord> {

    private long currentMaxTimestamp;

    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        return new Watermark(currentMaxTimestamp);
    }

    @Override
    public long extractTimestamp(FriendshipRecord friendshipRecord, long l) {
        long timestamp = friendshipRecord.getFriendshipDate().toInstant().toEpochMilli();        // milliseconds
        currentMaxTimestamp = Math.max(currentMaxTimestamp, timestamp);
        return timestamp;
    }
}
