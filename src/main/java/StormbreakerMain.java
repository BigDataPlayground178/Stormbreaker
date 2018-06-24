import entities.records.CommentRecord;
import entities.records.FriendshipRecord;
import entities.records.PostRecord;
import entities.results.FriendshipCount;
import operators.maps.RelationDuplicateFilter;
import operators.watermarks.CommentRecordsWatermarks;
import operators.watermarks.FriendshipCountWatermarks;
import operators.watermarks.FriendshipRecordsWatermarks;
import operators.watermarks.PostRecordsWatermarks;
import operators.windows.FriendshipCountApply;
import operators.windows.FriendshipCountWeekApply;
import operators.windows.PostRankApply;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import utils.CommentReader;
import utils.FriendshipReader;
import utils.PostReader;

import static utils.StormbreakerConstants.*;


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
        // -> removing relation duplicates (using built-in Flink storage)
        inputFriendshipStream = inputFriendshipStream.flatMap(new RelationDuplicateFilter());

        // -> setting parallelism
        ((SingleOutputStreamOperator<FriendshipRecord>) inputFriendshipStream).setParallelism(1);

        // [24h] -> windowing over a 24h timespan
        DataStream<FriendshipCount> friendshipDayDataStream = inputFriendshipStream
                        .windowAll(TumblingEventTimeWindows.of(Time.hours(24)))
                        .apply(new FriendshipCountApply());

        // [7d] -> windowing over a 7 X 24h timespan
        DataStream<FriendshipCount> friendshipWeekDataStream = friendshipDayDataStream
                        .assignTimestampsAndWatermarks(new FriendshipCountWatermarks())
                        .windowAll(TumblingEventTimeWindows.of(Time.days(7)))
                        .apply(new FriendshipCountWeekApply());

        // [ENTIRE DATASET] -> windowing over a configurable timespan (in minutes)
        DataStream<FriendshipCount> friendshipCountDataStream = inputFriendshipStream
                        .windowAll(TumblingEventTimeWindows.of(Time.minutes(DATASET_STATS_MINUTES)))
                        .apply(new FriendshipCountApply());

        // ---------------------- END QUERY 1 ----------------------



        // --------------------- START QUERY 2 ---------------------
        // [SAMPLE] retrieving input comments records from file
        String commentsSamplePath = StormbreakerMain.class.getResource(COMMENTS_DAT_PATH).getPath();
        String postsSamplePath = StormbreakerMain.class.getResource(POSTS_DAT_PATH).getPath();


        DataStream<CommentRecord> commentsStream = env.readFile(
                new TextInputFormat(new Path(commentsSamplePath)),
                commentsSamplePath
        ).map(new CommentReader());

        // Take only comments of Posts
        DataStream<CommentRecord> commentsToPost = commentsStream.filter(new FilterFunction<CommentRecord>() {
            @Override
            public boolean filter(CommentRecord comment) throws Exception {
                return comment.isCommentToPost();
            }
        }).assignTimestampsAndWatermarks(new CommentRecordsWatermarks());


        // group comments for time window (hour, day, week)
        AllWindowedStream<CommentRecord, TimeWindow> commentsHour = commentsToPost.windowAll(TumblingEventTimeWindows.of(Time.hours(1)));
        AllWindowedStream<CommentRecord, TimeWindow> commentsDay = commentsToPost.windowAll(TumblingEventTimeWindows.of(Time.hours(24)));
        AllWindowedStream<CommentRecord, TimeWindow> commentsWeek = commentsToPost.windowAll(TumblingEventTimeWindows.of(Time.days(7)));


        // compute rank for each window
        commentsHour.apply(new PostRankApply());
        commentsDay.apply(new PostRankApply());
        commentsWeek.apply(new PostRankApply());

        // ---------------------- END QUERY 2 ----------------------


        // ---------------------- BEGIN QUERY 3 --------------------

        DataStream<PostRecord> postsStream = env.readFile(
                new TextInputFormat(new Path(postsSamplePath)),
                postsSamplePath
        ).map(new PostReader());

        // assing timestam and watermarks to stream of Posts
        DataStream<PostRecord> postRecordDataStream = postsStream.assignTimestampsAndWatermarks(new PostRecordsWatermarks());


        // join Comments To Posts and Posts by UserID
        // group them by time window
        // count number of comments and number of posts and return a Tuple2<UserID, numpost + numcomments>
        DataStream<Tuple2<Long, Integer>> bcStream = commentsToPost.coGroup(postRecordDataStream)
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
                }).window(TumblingEventTimeWindows.of(Time.days(7)))
                .apply(new CoGroupFunction<CommentRecord, PostRecord, Tuple2<Long, Integer>>() {
                    @Override
                    public void coGroup(Iterable<CommentRecord> iterable, Iterable<PostRecord> iterable1, Collector<Tuple2<Long, Integer>> collector) throws Exception {
                        int numCom = 0;
                        int numPost = 0;
                        Long userID = null;

                        if (iterable != null) {
                            for (CommentRecord cr : iterable) {
                                userID = cr.getUser_id();
                                numCom++;
                            }
                        }

                        if (iterable1 != null) {
                            for (PostRecord pr : iterable1) {
                                userID = pr.getUser_id();
                                numPost++;
                            }
                        }

                        if (userID != null) {
                            collector.collect(new Tuple2<>(userID, numCom + numPost));
                        }
                    }
                });

        bcStream.print();








        // ---------------------- END QUERY 3 ----------------------

        // running streaming environment
        env.execute(STORMBREAKER_ENV);
    }
}
