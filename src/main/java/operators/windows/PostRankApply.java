package operators.windows;

import entities.records.CommentRecord;
import entities.results.PostRank;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * AllWindowFunction Class for PostRank construction
 */
public class PostRankApply implements AllWindowFunction<CommentRecord, PostRank, TimeWindow> {
    @Override
    public void apply(TimeWindow timeWindow, Iterable<CommentRecord> iterable, Collector<PostRank> collector) throws Exception {
        PostRank pr = new PostRank();

        // for each comment
        for (CommentRecord cr : iterable) {
            // add the new comment to the ranking object
            pr.addNewValue(cr.getPost_commented());

            // set the lowest recorded timestamp as the rank timestamp
            if (cr.getTimestamp().toInstant().toEpochMilli() < pr.getTS()) {
                pr.setTs(cr.getTimestamp().toInstant().toEpochMilli());
            }
        }

        collector.collect(pr);
    }
}
