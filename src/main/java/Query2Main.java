import entities.records.CommentRecord;
import entities.results.PostRank;
import operators.watermarks.CommentRecordsWatermarks;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import utils.CommentReader;

public class Query2Main {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);


        //String commentsSamplePath = StormbreakerMain.class.getResource(FRIENDSHIP_DAT_PATH).getPath();
        String commentsSamplePath = "file:///Users/fede9/Downloads/data.tar/data/comments.dat";

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

        env.execute();
    }
}
