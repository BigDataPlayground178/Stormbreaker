package operators.watermarks;

import entities.records.PostRecord;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

public class PostRecordsWatermarks implements AssignerWithPeriodicWatermarks<PostRecord> {

    private long currentMaxTimestamp;


    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        return new Watermark(currentMaxTimestamp);
    }

    @Override
    public long extractTimestamp(PostRecord postRecord, long l) {
        long timestamp = postRecord.getTimestamp().toInstant().toEpochMilli();        // milliseconds
        currentMaxTimestamp = Math.max(currentMaxTimestamp, timestamp);
        return timestamp;
    }
}
