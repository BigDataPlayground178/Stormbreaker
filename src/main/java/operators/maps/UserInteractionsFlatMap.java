package operators.maps;

import entities.records.FriendshipRecord;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

/**
 * This class if an extension of a FlatMapFunctions to remap a friendship record in a tuple composed
 * by (user_id, initial score (1), timestamp)
 */
public class UserInteractionsFlatMap implements FlatMapFunction<FriendshipRecord, Tuple3<Long, Integer, Long>> {
    @Override
    public void flatMap(FriendshipRecord friendshipRecord, Collector<Tuple3<Long, Integer, Long>> out) throws Exception {
        out.collect(new Tuple3<Long,Integer, Long>(friendshipRecord.getFollowingUser(), 1, friendshipRecord.getFriendshipDate().toInstant().toEpochMilli()));
    }
}
