import entities.records.CommentRecord;
import entities.records.PostRecord;
import operators.watermarks.CommentRecordsWatermarks;
import operators.watermarks.PostRecordsWatermarks;
import operators.windows.PostRankApply;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import utils.CommentReader;
import utils.PostReader;

public class Query2Main {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);


        //String commentsSamplePath = StormbreakerMain.class.getResource(FRIENDSHIP_DAT_PATH).getPath();
        String commentsSamplePath = "file:///Users/fede9/Downloads/data.tar/data/comments.dat";
        String postsSamplePath = "file:///Users/fede9/Downloads/data.tar/data/posts.dat";

        DataStream<CommentRecord> commentsStream = env.readFile(
                new TextInputFormat(new Path(commentsSamplePath)),
                commentsSamplePath
        ).map(new CommentReader());


        DataStream<PostRecord> postsStream = env.readFile(
                new TextInputFormat(new Path(postsSamplePath)),
                postsSamplePath
        ).map(new PostReader());

        DataStream<PostRecord> postRecordDataStream = postsStream.assignTimestampsAndWatermarks(new PostRecordsWatermarks());


        // Take only comments of Posts
        DataStream<CommentRecord> commentsToPost = commentsStream.filter(new FilterFunction<CommentRecord>() {
            @Override
            public boolean filter(CommentRecord comment) throws Exception {
                return comment.isCommentToPost();
            }
        }).assignTimestampsAndWatermarks(new CommentRecordsWatermarks());


        AllWindowedStream<CommentRecord, TimeWindow> commentsHour = commentsToPost.windowAll(TumblingEventTimeWindows.of(Time.hours(1)));
        AllWindowedStream<CommentRecord, TimeWindow> commentsDay = commentsToPost.windowAll(TumblingEventTimeWindows.of(Time.hours(24)));
        AllWindowedStream<CommentRecord, TimeWindow> commentsWeek = commentsToPost.windowAll(TumblingEventTimeWindows.of(Time.days(7)));


        commentsHour.apply(new PostRankApply());
        commentsDay.apply(new PostRankApply());
        commentsWeek.apply(new PostRankApply());


        KeyedStream<CommentRecord, Long> commentsByUserID = commentsToPost.keyBy(new KeySelector<CommentRecord, Long>() {
            @Override
            public Long getKey(CommentRecord commentRecord) throws Exception {
                return commentRecord.getUser_id();
            }
        });

        DataStream<Tuple2<Long, Integer>> cStream = commentsByUserID
                .windowAll(TumblingEventTimeWindows.of(Time.days(7)))
                .apply(new AllWindowFunction<CommentRecord, Tuple2<Long, Integer>, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow timeWindow, Iterable<CommentRecord> iterable, Collector<Tuple2<Long, Integer>> collector) throws Exception {
                        int num = 0;
                        for (CommentRecord cr : iterable) {
                            num++;
                        }
                        //System.out.println("User " + iterable.iterator().next().getUser_id() + " wrote " + num + " comments in the hour" + timeWindow.getStart());
                        collector.collect(new Tuple2<>(iterable.iterator().next().getUser_id(), num));
                    }
                });

        KeyedStream<PostRecord, Long> postsByUserID = postRecordDataStream.keyBy(new KeySelector<PostRecord, Long>() {
            @Override
            public Long getKey(PostRecord postRecord) throws Exception {
                return postRecord.getUser_id();
            }
        });

        DataStream<Tuple2<Long, Integer>> bStream = postsByUserID
                .windowAll(TumblingEventTimeWindows.of(Time.days(7)))
                .apply(new AllWindowFunction<PostRecord, Tuple2<Long, Integer>, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow timeWindow, Iterable<PostRecord> iterable, Collector<Tuple2<Long, Integer>> collector) throws Exception {
                        int num = 0;
                        for (PostRecord pr : iterable) {
                            num++;
                        }
                        collector.collect(new Tuple2<>(iterable.iterator().next().getUser_id(), num));
                    }
                });

        bStream.union(cStream).keyBy(new KeySelector<Tuple2<Long,Integer>, Long>() {
            @Override
            public Long getKey(Tuple2<Long, Integer> longIntegerTuple2) throws Exception {
                return longIntegerTuple2.f0;
            }
        }).reduce(new ReduceFunction<Tuple2<Long, Integer>>() {
            @Override
            public Tuple2<Long, Integer> reduce(Tuple2<Long, Integer> longIntegerTuple2, Tuple2<Long, Integer> t1) throws Exception {
                return new Tuple2<>(longIntegerTuple2.f0, longIntegerTuple2.f1 + t1.f1);
            }
        });


        env.execute();
    }
}
