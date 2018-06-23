import entities.records.CommentRecord;
import entities.records.FriendshipRecord;
import entities.results.FriendshipCount;
import entities.results.PostRank;
import operators.maps.RelationDuplicateFilter;
import operators.watermarks.CommentRecordsWatermarks;
import operators.watermarks.FriendshipCountWatermarks;
import operators.watermarks.FriendshipRecordsWatermarks;
import operators.windows.FriendshipCountDayApply;
import operators.windows.FriendshipCountWeekApply;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import utils.CommentReader;
import utils.FriendshipReader;

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
                        .apply(new FriendshipCountDayApply());

        // [7d] -> windowing over a 7 X 24h timespan
        DataStream<FriendshipCount> friendshipWeekDataStream = friendshipDayDataStream
                        .assignTimestampsAndWatermarks(new FriendshipCountWatermarks())
                        .windowAll(TumblingEventTimeWindows.of(Time.days(7)))
                        .apply(new FriendshipCountWeekApply());

        // TODO [ENTIRE DATASET]

        // ---------------------- END QUERY 1 ----------------------

        // --------------------- START QUERY 2 ---------------------
        // [SAMPLE] retrieving input comments records from file
        String commentsSamplePath = StormbreakerMain.class.getResource(COMMENTS_DAT_PATH).getPath();
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


        commentsToPost.windowAll(TumblingEventTimeWindows.of(Time.hours(1)))
                .apply(new AllWindowFunction<CommentRecord, PostRank, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow timeWindow, Iterable<CommentRecord> iterable, Collector<PostRank> collector) throws Exception {

                        PostRank pr = new PostRank();

                        for (CommentRecord cr : iterable) {
                            pr.addNewValue(cr.getPost_commented());
                        }

//                        Stream<Map.Entry<Long, Long>> topRank = pr.getTopRank();
//                        System.out.print(System.currentTimeMillis() + " , ");
//                        for (Map.Entry<Long, Long> entry : topRank.collect(Collectors.toList())) {
//                            System.out.print(entry.getKey() + " , " + entry.getValue() + " , ");
//                        }
//                        System.out.println();

                        collector.collect(pr);
                    }
                });

        commentsToPost.windowAll(TumblingEventTimeWindows.of(Time.hours(24)))
                .apply(new AllWindowFunction<CommentRecord, PostRank, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow timeWindow, Iterable<CommentRecord> iterable, Collector<PostRank> collector) throws Exception {

                        PostRank pr = new PostRank();

                        for (CommentRecord cr : iterable) {
                            pr.addNewValue(cr.getPost_commented());
                        }

                        collector.collect(pr);
                    }
                });

        commentsToPost.windowAll(TumblingEventTimeWindows.of(Time.days(7)))
                .apply(new AllWindowFunction<CommentRecord, PostRank, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow timeWindow, Iterable<CommentRecord> iterable, Collector<PostRank> collector) throws Exception {

                        PostRank pr = new PostRank();

                        int num = 0;

                        for (CommentRecord cr : iterable) {
                            pr.addNewValue(cr.getPost_commented());
                            num++;
                        }

                        collector.collect(pr);
                    }
                });

        // ---------------------- END QUERY 2 ----------------------

        // running streaming environment
        env.execute(STORMBREAKER_ENV);
    }
}
