package operators.maps;

import entities.records.FriendshipRecord;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class UserInteractionsFlatMap implements FlatMapFunction<FriendshipRecord, Tuple2<Long, Integer>> {
    @Override
    public void flatMap(FriendshipRecord friendshipRecord, Collector<Tuple2<Long, Integer>> out) throws Exception {
        out.collect(new Tuple2<Long,Integer>(friendshipRecord.getFollowingUser(), 1));
    }
}
