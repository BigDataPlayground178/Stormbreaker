package operators.selectors;

import entities.records.FriendshipRecord;
import org.apache.flink.api.java.functions.KeySelector;

/**
 * This class is an extension of a KeySelector to key by following user
 * in a friendship stream
 */
public class FriendshipUserSelector implements KeySelector<FriendshipRecord, Long> {
    public Long getKey(FriendshipRecord friendshipRecord) throws Exception {
        return friendshipRecord.getFollowingUser();
    }
}
