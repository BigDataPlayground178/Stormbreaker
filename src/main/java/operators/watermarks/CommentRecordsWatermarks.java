package operators.watermarks;

import entities.records.CommentRecord;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

/**
 * This class can be used to extract watermarks from comment records
 */
public class CommentRecordsWatermarks implements AssignerWithPeriodicWatermarks<CommentRecord> {

    private long currentMaxTimestamp;

    @Override
    public Watermark getCurrentWatermark() {
        return new Watermark(currentMaxTimestamp);
    }

    @Override
    public long extractTimestamp(CommentRecord commentRecord, long l) {
        Long timestamp = commentRecord.getTimestamp().toInstant().toEpochMilli();
        currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
        return timestamp;
    }
}

