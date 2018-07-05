package operators.maps;

import entities.records.FriendshipRecord;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.util.Collector;

import java.util.stream.StreamSupport;

/**
 * This class is an extension of a RichFlatMap that leverages upon the use of operator states
 * in order to remove duplicates from a friendship stream
 */
public class RelationDuplicateFilter extends RichFlatMapFunction<FriendshipRecord, FriendshipRecord> implements CheckpointedFunction {

    private ListState<String> relationsList;

    @Override
    public void flatMap(FriendshipRecord friendshipRecord, Collector<FriendshipRecord> out) throws Exception {
        // duplicate predicate -> is duplicate if followed user is already a following one
        String inverseRelation = String.valueOf(friendshipRecord.getFollowedUser()) + "-" + String.valueOf(friendshipRecord.getFollowingUser());
        Boolean isDuplicate = StreamSupport.stream(relationsList.get().spliterator(), false)
                                           .anyMatch(relation -> inverseRelation.equals(relation));

        if (!isDuplicate) {
            // updating state
            String newRelation = String.valueOf(friendshipRecord.getFollowingUser()) + "-" + String.valueOf(friendshipRecord.getFollowedUser());
            relationsList.add(newRelation);

            // filtering stream
            out.collect(friendshipRecord);
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        // retrieving list of relations user1-user2
        relationsList = context.getOperatorStateStore().getListState(new ListStateDescriptor<String>("relations", String.class));
    }


    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {

    }
}
