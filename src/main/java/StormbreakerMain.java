import entities.records.FriendshipRecord;
import operators.selectors.UserSelector;
import operators.watermarks.FriendshipRecordsWatermarks;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import utils.FriendshipReader;


public class StormbreakerMain {

    public static void main(String[] args) throws Exception {

        // retrieving streaming environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // --------------------- START QUERY 1 ---------------------
        // [SAMPLE] retrieving input friendship records from file
        String friendshipSamplePath = StormbreakerMain.class.getResource(StormbreakerConstants.FRIENDSHIP_DAT_PATH).getPath();
        DataStream<FriendshipRecord> inputFriendshipStream = env.readFile(
                new TextInputFormat(new Path(friendshipSamplePath)),
                friendshipSamplePath
        ).map(new FriendshipReader());
        // assigning watermarks - timestamp of friendship (milliseconds)
        inputFriendshipStream.assignTimestampsAndWatermarks(new FriendshipRecordsWatermarks());

        // -> windowing over a 24h timespan
        inputFriendshipStream
                .keyBy(new UserSelector())
                .timeWindow(Time.hours(24));

        // ---------------------- END QUERY 1 ----------------------

        // running streaming environment
        env.execute(StormbreakerConstants.STORMBREAKER_ENV);
    }
}
