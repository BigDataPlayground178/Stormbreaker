package operators.maps;

import entities.records.FriendshipRecord;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.stream.StreamSupport;

public class RelationDuplicateFilter extends RichFlatMapFunction<FriendshipRecord, FriendshipRecord> {

    @Override
    public void flatMap(FriendshipRecord friendshipRecord, Collector<FriendshipRecord> out) throws Exception {
        // retrieving list of relations <user1, user2>
        ListState<Tuple2> relationsList = getRuntimeContext().getListState(new ListStateDescriptor<Tuple2>("relations", Tuple2.class));

        // duplicate predicate -> is duplicate if followed user is already a following one
        Tuple2<Integer, Integer> inverseRelation = new Tuple2<>(friendshipRecord.getFollowedUser(), friendshipRecord.getFollowingUser());
        Boolean isDuplicate = StreamSupport.stream(relationsList.get().spliterator(), false)
                                           .anyMatch(relation -> inverseRelation.equals(relation));

        if (!isDuplicate) {
            // updating state
            Tuple2<Integer, Integer> newRelation = new Tuple2<>(friendshipRecord.getFollowingUser(), friendshipRecord.getFollowedUser());
            relationsList.add(newRelation);

            // filtering stream
            out.collect(friendshipRecord);
        }
    }
}
