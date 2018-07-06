import entities.records.CommentRecord;
import entities.records.FriendshipRecord;
import entities.records.PostRecord;
import entities.results.FriendshipCount;
import entities.results.PostRank;
import entities.results.UserRank;
import operators.maps.RelationDuplicateFilter;
import operators.maps.UserInteractionsFlatMap;
import operators.selectors.UserKeySelector;
import operators.sinks.InfluxDBFriendshipCountSink;
import operators.sinks.InfluxDBPostRankSink;
import operators.sinks.InfluxDBUserRankSink;
import operators.watermarks.*;
import operators.windows.*;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import utils.CommentReader;
import utils.FriendshipReader;
import utils.PostReader;

import java.util.Properties;

import static utils.StormbreakerConstants.*;


public class StormbreakerMain {

    public static void main(String[] args) throws Exception {

        // retrieving streaming environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setLatencyTrackingInterval(50);
        //env.setParallelism(3);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // -------------------- SOURCE STREAMS ---------------------
        // preparing Kafka properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", KAFKA_BOOTSTRAP_SERVER);
        properties.setProperty("zookeeper.connect", KAFKA_ZOOKEEPER_SERVER);


        // -> friendships records
        DataStream<FriendshipRecord> friendshipStream = env
                .addSource(new FlinkKafkaConsumer011<>(KAFKA_FRIENDSHIP_TOPIC, new SimpleStringSchema(), properties))
                .map(new FriendshipReader()).name("friendshipStreamFromKafka");
        // -> comments records
        DataStream<CommentRecord> commentsStream = env
                .addSource(new FlinkKafkaConsumer011<>(KAFKA_COMMENTS_TOPIC, new SimpleStringSchema(), properties))
                .map(new CommentReader()).name("commentsStreamFromKafka");
        // -> posts records
        DataStream<PostRecord> postsStream = env
                .addSource(new FlinkKafkaConsumer011<>(KAFKA_POSTS_TOPIC, new SimpleStringSchema(), properties))
                .map(new PostReader()).name("postsStreamFromKafka");



        // --------------------- START QUERY 1 ---------------------
        // [SAMPLE] retrieving input friendship records from file
        /*
        String friendshipSamplePath = StormbreakerMain.class.getResource(FRIENDSHIP_DAT_PATH).getPath();
        DataStream<FriendshipRecord> friendshipStream = env.readFile(
                new TextInputFormat(new Path(friendshipSamplePath)),
                friendshipSamplePath
        ).setParallelism(1).map(new FriendshipReader());
        */

        // -> setting parallelism
        ((SingleOutputStreamOperator<FriendshipRecord>) friendshipStream).setParallelism(1);

        // -> removing relation duplicates (using built-in Flink storage)
        friendshipStream = friendshipStream.flatMap(new RelationDuplicateFilter()).uid("rimozioneDuplicati");

        // [24h] -> windowing over a 24h timespan
        DataStream<FriendshipCount> friendshipDayDataStream = friendshipStream
                .assignTimestampsAndWatermarks(new FriendshipRecordsWatermarks()).uid("assegnaTimestamp")
                .windowAll(TumblingEventTimeWindows.of(Time.hours(24)))
                .apply(new FriendshipCountApply()).uid("faApply").name("FriendshipCount24hours");

        //friendshipDayDataStream.map(new FriendshipCountTp("tpQuery1Hour"));

        // [24 h] -> sink to InfluxDB
        friendshipDayDataStream.addSink(new InfluxDBFriendshipCountSink("friendships_day"));

        // [7d] -> windowing over a 7 X 24h timespan
        DataStream<FriendshipCount> friendshipWeekDataStream = friendshipDayDataStream
                .assignTimestampsAndWatermarks(new FriendshipCountWatermarks())
                .windowAll(TumblingEventTimeWindows.of(Time.days(7)))
                .apply(new FriendshipCountWeekApply()).name("FriendshipCount7Days");

        //friendshipWeekDataStream.map(new FriendshipCountTp("tpQuery1Week"));

        // [7d] -> sink to InfluxDB
        friendshipWeekDataStream.addSink(new InfluxDBFriendshipCountSink("friendships_week"));

        // [ENTIRE DATASET] -> windowing over a configurable timespan (in minutes)
        DataStream<FriendshipCount> friendshipCountDataStream = friendshipStream
                .assignTimestampsAndWatermarks(new FriendshipRecordsWatermarks())
                .windowAll(TumblingEventTimeWindows.of(Time.minutes(DATASET_STATS_MINUTES)))
                .apply(new FriendshipCountApply()).name("FriendshipCount60Minutes");

        //friendshipCountDataStream.map(new FriendshipCountTp("tpQuery1All"));
        // [ENTIRE DATASET] -> sink to InfluxDB
        friendshipCountDataStream.addSink(new InfluxDBFriendshipCountSink("friendships_all"));


        // ---------------------- END QUERY 1 ----------------------


        // --------------------- START QUERY 2 ---------------------
        // [SAMPLE] retrieving input comments records from file
        /*
        String commentsSamplePath = StormbreakerMain.class.getResource(COMMENTS_DAT_PATH).getPath();
        String postsSamplePath = StormbreakerMain.class.getResource(POSTS_DAT_PATH).getPath();
        DataStream<CommentRecord> commentsStream = env.readFile(
                new TextInputFormat(new Path(commentsSamplePath)),
                commentsSamplePath
        ).setParallelism(1).map(new CommentReader());
        */

        // -> Assign timestamps and Watermarks to stream of Comments
        commentsStream = commentsStream.assignTimestampsAndWatermarks(new CommentRecordsWatermarks());

        // -> Take only comments to Posts
        DataStream<CommentRecord> commentsToPost = commentsStream.filter(new FilterFunction<CommentRecord>() {
            @Override
            public boolean filter(CommentRecord comment) throws Exception {
                return comment.isCommentToPost();
            }
        });


        // -> group comments for time window (hour, day, week)
        AllWindowedStream<CommentRecord, TimeWindow> commentsToPostHour = commentsToPost.windowAll(TumblingEventTimeWindows.of(Time.hours(1)));
        AllWindowedStream<CommentRecord, TimeWindow> commentsToPostDay = commentsToPost.windowAll(TumblingEventTimeWindows.of(Time.hours(24)));
        AllWindowedStream<CommentRecord, TimeWindow> commentsToPostWeek = commentsToPost.windowAll(TumblingEventTimeWindows.of(Time.days(7)));


        // -> compute rank for each window
        DataStream<PostRank> postRankHour = commentsToPostHour.apply(new PostRankApply());
        DataStream<PostRank> postRankDay = commentsToPostDay.apply(new PostRankApply());
        DataStream<PostRank> postRankWeek = commentsToPostWeek.apply(new PostRankApply());


        // -> sink ranks to InfluxDB
        postRankHour.addSink(new InfluxDBPostRankSink("postrank_hour"));
        postRankDay.addSink(new InfluxDBPostRankSink("postrank_day"));
        postRankWeek.addSink(new InfluxDBPostRankSink("postrank_week"));

        // ---------------------- END QUERY 2 ----------------------

        // [SAMPLE] retrieving posts stream
        // DataStream<PostRecord> postsStream = env.readFile(
        //        new TextInputFormat(new Path(postsSamplePath)),
        //                              postsSamplePath
        //).setParallelism(1).map(new PostReader());

        // -> assigning timestamp and watermarks to stream of Posts
        DataStream<PostRecord> postRecordDataStream = postsStream.assignTimestampsAndWatermarks(new PostRecordsWatermarks());


        // ---------------------- BEGIN QUERY 3 --------------------

        // [1h] -> retrieving number of relations created by user (<user, relations, timestamp>)
        DataStream<Tuple3<Long, Integer, Long>> aStreamHour = friendshipStream
                    .flatMap(new UserInteractionsFlatMap())
                    .assignTimestampsAndWatermarks(new UserRankWatermarks())
                    .keyBy(0)
                    .window(TumblingEventTimeWindows.of(Time.hours(USERS_RANKING_HOUR)))
                    .sum(1);

        // -> join Comments To Posts and Posts by UserID
        //    group them by time window
        //    count number of comments and number of posts and return a Tuple2<user, numpost + numcomments, timestamp>
        DataStream<Tuple3<Long, Integer, Long>> bcStreamHour = commentsStream.coGroup(postRecordDataStream)
                .where(commentRecord -> commentRecord.getUser_id())
                .equalTo(postRecord -> postRecord.getUser_id())
                .window(TumblingEventTimeWindows.of(Time.hours(USERS_RANKING_HOUR)))
                .apply(new bcRankCoGroup());


        // -> finally merging the two streams to compute user stats for ranking
        DataStream<Tuple3<Long, Integer, Long>> userRankStreamHour = aStreamHour.coGroup(bcStreamHour)
                .where(new UserKeySelector()).equalTo(new UserKeySelector())
                .window(TumblingEventTimeWindows.of(Time.hours(USERS_RANKING_HOUR)))
                .apply(new UserRankJoin());

        // [1h] -> retrieving first N users to build the ranking
        DataStream<UserRank> userRankHour = userRankStreamHour
                .windowAll(TumblingEventTimeWindows.of(Time.hours(USERS_RANKING_HOUR)))
                .apply(new UserRanking());

        // [1h] -> sink ranks to InfluxDB
        userRankHour.addSink(new InfluxDBUserRankSink("userrank_hour"));


        // [24h] -> retrieving first N users to build the ranking
        DataStream<UserRank> userRankDay = userRankStreamHour
                .windowAll(TumblingEventTimeWindows.of(Time.hours(USERS_RANKING_DAY_HOUR)))
                .apply(new UserRanking());

        // [24h] -> sink rank to InfluxDB
        userRankDay.addSink(new InfluxDBUserRankSink("userrank_day"));


        // [7d] -> sink ranks to InfluxDB
        DataStream<UserRank> userRankWeek = userRankStreamHour
                .windowAll(TumblingEventTimeWindows.of(Time.days(USERS_RANKING_WEEK_DAY)))
                .apply(new UserRanking());

        // [7d] -> retrieving first N users to build the ranking
        userRankWeek.addSink(new InfluxDBUserRankSink("userrank_week"));



        // ---------------------- END QUERY 3 ----------------------

        // DEBUG: printing execution plan
        System.out.println(env.getExecutionPlan());
        // running streaming environment
        env.execute(STORMBREAKER_ENV);
    }
}
