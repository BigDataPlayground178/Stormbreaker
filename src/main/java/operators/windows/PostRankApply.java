package operators.windows;

import entities.records.CommentRecord;
import entities.results.PostRank;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class PostRankApply implements AllWindowFunction<CommentRecord, PostRank, TimeWindow> {
    @Override
    public void apply(TimeWindow timeWindow, Iterable<CommentRecord> iterable, Collector<PostRank> collector) throws Exception {
        PostRank pr = new PostRank();

        for (CommentRecord cr : iterable) {
            pr.addNewValue(cr.getPost_commented());
            if (cr.getTimestamp().toInstant().toEpochMilli() < pr.getTS()) {
                pr.setTs(cr.getTimestamp().toInstant().toEpochMilli());
            }
        }

        collector.collect(pr);
    }
}
