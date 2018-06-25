package operators.watermarks;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import static utils.StormbreakerConstants.DATASET_START_TIMESTAMP;

public class UserRankWatermarks implements AssignerWithPeriodicWatermarks<Tuple3<Long, Integer, Long>> {

    private long currentMaxTimestamp = DATASET_START_TIMESTAMP;

    @Override
    public Watermark getCurrentWatermark() {
        return new Watermark(currentMaxTimestamp);
    }

    @Override
    public long extractTimestamp(Tuple3<Long, Integer, Long> record, long l) {
        Long timestamp = record.f2;
        currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
        return timestamp;
    }
}
