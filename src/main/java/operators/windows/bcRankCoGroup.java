package operators.windows;

import entities.records.CommentRecord;
import entities.records.PostRecord;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

public class bcRankCoGroup implements CoGroupFunction<CommentRecord, PostRecord, Tuple3<Long, Integer, Long>> {
    @Override
    public void coGroup(Iterable<CommentRecord> comments, Iterable<PostRecord> posts, Collector<Tuple3<Long, Integer, Long>> collector) throws Exception {
        int numCom = 0;
        int numPost = 0;
        long timestamp = 0;
        Long userID = null;

        // if the user made comments
        if (comments != null) {
            for (CommentRecord cr : comments) {
                userID = cr.getUser_id();
                numCom++;
                if (cr.getTimestamp().toInstant().toEpochMilli() >= timestamp)
                    timestamp = cr.getTimestamp().toInstant().toEpochMilli();
            }
        }

        // if the user made posts
        if (posts != null) {
            for (PostRecord pr : posts) {
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
}
