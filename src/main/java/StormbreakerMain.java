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
import operators.watermarks.*;
import operators.windows.*;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
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
import org.apache.flink.util.Collector;
import utils.CommentReader;
import utils.FriendshipReader;
import utils.PostReader;

import java.util.Properties;

import static utils.StormbreakerConstants.*;


public class StormbreakerMain {

    public static void main(String[] args) throws Exception {

        // retrieving streaming environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //env.getConfig().setAutoWatermarkInterval(50);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // -------------------- SOURCE STREAMS ---------------------
        // preparing Kafka properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", KAFKA_BOOTSTRAP_SERVER);
        properties.setProperty("zookeeper.connect", KAFKA_ZOOKEEPER_SERVER);

        // -> friendships records
        DataStream<FriendshipRecord> friendshipStream = env
                .addSource(new FlinkKafkaConsumer011<>(KAFKA_FRIENDSHIP_TOPIC, new SimpleStringSchema(), properties))
                .map(new FriendshipReader());
        // -> comments records
        DataStream<CommentRecord> commentsStream = env
                .addSource(new FlinkKafkaConsumer011<>(KAFKA_COMMENTS_TOPIC, new SimpleStringSchema(), properties))
                .map(new CommentReader());
        // -> posts records
        DataStream<PostRecord> postsStream = env
                .addSource(new FlinkKafkaConsumer011<>(KAFKA_POSTS_TOPIC, new SimpleStringSchema(), properties))
                .map(new PostReader());

        // --------------------- START QUERY 1 ---------------------
        // [SAMPLE] retrieving input friendship records from file
        //String friendshipSamplePath = StormbreakerMain.class.getResource(FRIENDSHIP_DAT_PATH).getPath();
        //DataStream<FriendshipRecord> inputFriendshipStream = env.readFile(
        //        new TextInputFormat(new Path(friendshipSamplePath)),
        //        friendshipSamplePath
        //).setParallelism(1).map(new FriendshipReader());

        // -> setting parallelism
        ((SingleOutputStreamOperator<FriendshipRecord>) friendshipStream).setParallelism(1);

        // -> removing relation duplicates (using built-in Flink storage)
        friendshipStream = friendshipStream.flatMap(new RelationDuplicateFilter());

        // [24h] -> windowing over a 24h timespan
        DataStream<FriendshipCount> friendshipDayDataStream = friendshipStream
                        .assignTimestampsAndWatermarks(new FriendshipRecordsWatermarks())
                        .windowAll(TumblingEventTimeWindows.of(Time.hours(24)))
                        .apply(new FriendshipCountApply());

        friendshipDayDataStream.print();

        friendshipDayDataStream.addSink(new InfluxDBFriendshipCountSink("friendships_hour"));

        // [7d] -> windowing over a 7 X 24h timespan
        DataStream<FriendshipCount> friendshipWeekDataStream = friendshipDayDataStream
                        .assignTimestampsAndWatermarks(new FriendshipCountWatermarks())
                        .windowAll(TumblingEventTimeWindows.of(Time.days(7)))
                        .apply(new FriendshipCountWeekApply());

        // [ENTIRE DATASET] -> windowing over a configurable timespan (in minutes)
        DataStream<FriendshipCount> friendshipCountDataStream = friendshipStream
                        .assignTimestampsAndWatermarks(new FriendshipRecordsWatermarks())
                        .windowAll(TumblingEventTimeWindows.of(Time.minutes(DATASET_STATS_MINUTES)))
                        .apply(new FriendshipCountApply());


        // ---------------------- END QUERY 1 ----------------------



        // --------------------- START QUERY 2 ---------------------
        // [SAMPLE] retrieving input comments records from file
        //String commentsSamplePath = StormbreakerMain.class.getResource(COMMENTS_DAT_PATH).getPath();
        //String postsSamplePath = StormbreakerMain.class.getResource(POSTS_DAT_PATH).getPath();
        //DataStream<CommentRecord> commentsStream = env.readFile(
        //        new TextInputFormat(new Path(commentsSamplePath)),
        //        commentsSamplePath
        //).setParallelism(1).map(new CommentReader());

        // Take only comments of Posts
        DataStream<CommentRecord> commentsToPost = commentsStream.filter(new FilterFunction<CommentRecord>() {
            @Override
            public boolean filter(CommentRecord comment) throws Exception {
                return comment.isCommentToPost();
            }
        }).assignTimestampsAndWatermarks(new CommentRecordsWatermarks());


        // group comments for time window (hour, day, week)
        AllWindowedStream<CommentRecord, TimeWindow> commentsToPostHour = commentsToPost.windowAll(TumblingEventTimeWindows.of(Time.hours(1)));
        AllWindowedStream<CommentRecord, TimeWindow> commentsToPostDay = commentsToPost.windowAll(TumblingEventTimeWindows.of(Time.hours(24)));
        AllWindowedStream<CommentRecord, TimeWindow> commentsToPostWeek = commentsToPost.windowAll(TumblingEventTimeWindows.of(Time.days(7)));


        // compute rank for each window
        DataStream<PostRank> postRankHour = commentsToPostHour.apply(new PostRankApply());
        DataStream<PostRank> postRankDay = commentsToPostDay.apply(new PostRankApply());
        DataStream<PostRank> postRankWeek = commentsToPostWeek.apply(new PostRankApply());

        postRankHour.addSink(new InfluxDBPostRankSink("postrank_hour"));
        postRankDay.addSink(new InfluxDBPostRankSink("postrank_day"));
        postRankWeek.addSink(new InfluxDBPostRankSink("postrank_week"));

        // ---------------------- END QUERY 2 ----------------------


        // ---------------------- BEGIN QUERY 3 --------------------

        // -> retrieving number of relations created by user (<user, relations, timestamp>)
        DataStream<Tuple3<Long, Integer, Long>> aStream = friendshipStream
                    .flatMap(new UserInteractionsFlatMap())
                    .assignTimestampsAndWatermarks(new UserRankWatermarks())
                    .keyBy(0)
                    .window(TumblingEventTimeWindows.of(Time.minutes(USERS_RANKING_MINUTES)))
                    .sum(1);

        // [SAMPLE] retrieving posts stream
        // DataStream<PostRecord> postsStream = env.readFile(
        //        new TextInputFormat(new Path(postsSamplePath)),
        //                              postsSamplePath
        //).setParallelism(1).map(new PostReader());

        // -> assigning timestamp and watermarks to stream of Posts
        DataStream<PostRecord> postRecordDataStream = postsStream.assignTimestampsAndWatermarks(new PostRecordsWatermarks());

        // -> join Comments To Posts and Posts by UserID
        //    group them by time window
        //    count number of comments and number of posts and return a Tuple2<user, numpost + numcomments, timestamp>
        DataStream<Tuple3<Long, Integer, Long>> bcStream = commentsToPost.coGroup(postRecordDataStream)
                .where(new KeySelector<CommentRecord, Long>() {
                    @Override
                    public Long getKey(CommentRecord commentRecord) throws Exception {
                        return commentRecord.getUser_id();
                    }
                }).equalTo(new KeySelector<PostRecord, Long>() {
                    @Override
                    public Long getKey(PostRecord postRecord) throws Exception {
                        return postRecord.getUser_id();
                    }
                }).window(TumblingEventTimeWindows.of(Time.minutes(USERS_RANKING_MINUTES)))
                .apply(new CoGroupFunction<CommentRecord, PostRecord, Tuple3<Long, Integer, Long>>() {
                    @Override
                    public void coGroup(Iterable<CommentRecord> iterable, Iterable<PostRecord> iterable1, Collector<Tuple3<Long, Integer, Long>> collector) throws Exception {
                        int numCom = 0;
                        int numPost = 0;
                        long timestamp = 0;
                        Long userID = null;

                        if (iterable != null) {
                            for (CommentRecord cr : iterable) {
                                userID = cr.getUser_id();
                                numCom++;
                                if (cr.getTimestamp().toInstant().toEpochMilli() >= timestamp)
                                    timestamp = cr.getTimestamp().toInstant().toEpochMilli();
                            }
                        }

                        if (iterable1 != null) {
                            for (PostRecord pr : iterable1) {
                                userID = pr.getUser_id();
                                numPost++;
                                if (pr.getTimestamp().toInstant().toEpochMilli() >= timestamp)
                                    timestamp = pr.getTimestamp().toInstant().toEpochMilli();
                            }
                        }

                        if (userID != null) {
                            collector.collect(new Tuple3<>(userID, numCom + numPost, timestamp));
                        }
                    }
                });
        // adding watermark
        bcStream = bcStream.assignTimestampsAndWatermarks(new UserRankWatermarks());

        // -> finally merging the two streams to compute user stats for ranking
        DataStream<Tuple3<Long, Integer, Long>> userRankStream = aStream.join(bcStream)
                .where(new UserKeySelector()).equalTo(new UserKeySelector())
                .window(TumblingEventTimeWindows.of(Time.minutes(USERS_RANKING_MINUTES)))
                .apply(new UserRankJoin());
        userRankStream = userRankStream.assignTimestampsAndWatermarks(new UserRankWatermarks());

        // -> retrieving first N users to build the ranking
        DataStream<UserRank> userRank = userRankStream
                .windowAll(TumblingEventTimeWindows.of(Time.minutes(USERS_RANKING_MINUTES)))
                .apply(new UserRanking());

        // ---------------------- END QUERY 3 ----------------------

        // DEBUG: printing execution plan
        // System.out.println(env.getExecutionPlan());
        // running streaming environment
        env.execute(STORMBREAKER_ENV);
    }
}
