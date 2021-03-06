package operators.windows;

import entities.records.CommentRecord;
import entities.records.PostRecord;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

/**
 * This class extends the CoGroupFunction to retrieve the "bc" score of user interactions
 */
public class bcRankCoGroup implements CoGroupFunction<CommentRecord, PostRecord, Tuple3<Long, Integer, Long>> {
    @Override
    public void coGroup(Iterable<CommentRecord> comments, Iterable<PostRecord> posts, Collector<Tuple3<Long, Integer, Long>> collector) throws Exception {
        int numCom = 0;
        int numPost = 0;
        long timestamp = Long.MAX_VALUE;
        Long userID = null;

        // if the user made comments
        if (comments != null) {
            for (CommentRecord cr : comments) {
                // userIDs should all be the same
                userID = cr.getUser_id();
                // increment number of comments
                numCom++;
                // set Tuple timestamp as the lowest record timestamp
                if (cr.getTimestamp().toInstant().toEpochMilli() <= timestamp)
                    timestamp = cr.getTimestamp().toInstant().toEpochMilli();
            }
        }

        // if the user made posts
        if (posts != null) {
            for (PostRecord pr : posts) {
                // userIDs should all be the same
                userID = pr.getUser_id();
                // increment number of posts
                numPost++;
                // set Tuple timestamp as the lowest record timestamp
                if (pr.getTimestamp().toInstant().toEpochMilli() <= timestamp)
                    timestamp = pr.getTimestamp().toInstant().toEpochMilli();
            }
        }

        if (userID != null) {
            collector.collect(new Tuple3<>(userID, numCom + numPost, timestamp));
        }
    }
}
