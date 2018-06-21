import entities.records.FriendshipRecord;
import entities.results.FriendshipCount;
import operators.watermarks.FriendshipCountWatermarks;
import operators.watermarks.FriendshipRecordsWatermarks;
import operators.windows.FriendshipCountDayApply;
import operators.windows.FriendshipCountWeekApply;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import utils.FriendshipReader;

import static utils.StormbreakerConstants.FRIENDSHIP_DAT_PATH;
import static utils.StormbreakerConstants.STORMBREAKER_ENV;


public class StormbreakerMain {

    public static void main(String[] args) throws Exception {

        // retrieving streaming environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // --------------------- START QUERY 1 ---------------------
        // [SAMPLE] retrieving input friendship records from file
        String friendshipSamplePath = StormbreakerMain.class.getResource(FRIENDSHIP_DAT_PATH).getPath();
        DataStream<FriendshipRecord> inputFriendshipStream = env.readFile(
                new TextInputFormat(new Path(friendshipSamplePath)),
                friendshipSamplePath
        ).map(new FriendshipReader());

        // -> assigning watermarks - timestamp of friendship (milliseconds)
        inputFriendshipStream.assignTimestampsAndWatermarks(new FriendshipRecordsWatermarks());
        // -> setting parallelism
        ((SingleOutputStreamOperator<FriendshipRecord>) inputFriendshipStream).setParallelism(1);

        // TODO pre-filter friendship duplicates (if A->B, not considering B->A)

        // [24h] -> windowing over a 24h timespan
        DataStream<FriendshipCount> friendshipDayDataStream = inputFriendshipStream
                        .windowAll(TumblingEventTimeWindows.of(Time.hours(24)))
                        .apply(new FriendshipCountDayApply());

        // [7d] -> windowing over a 7 X 24h timespan
        DataStream<FriendshipCount> friendshipWeekDataStream = friendshipDayDataStream
                        .assignTimestampsAndWatermarks(new FriendshipCountWatermarks())
                        .windowAll(TumblingEventTimeWindows.of(Time.days(7)))
                        .apply(new FriendshipCountWeekApply());

        // TODO [ENTIRE DATASET]

        // ---------------------- END QUERY 1 ----------------------

        // running streaming environment
        env.execute(STORMBREAKER_ENV);
    }
}
