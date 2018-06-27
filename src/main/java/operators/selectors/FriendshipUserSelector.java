package operators.selectors;

import entities.records.FriendshipRecord;
import org.apache.flink.api.java.functions.KeySelector;

public class FriendshipUserSelector implements KeySelector<FriendshipRecord, Long> {
    public Long getKey(FriendshipRecord friendshipRecord) throws Exception {
        return friendshipRecord.getFollowingUser();
    }
}