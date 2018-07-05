package entities.records;

import java.time.ZonedDateTime;

/**
 * This is the utility class to retrieve friendship (following -> followed users) records
 */
public class FriendshipRecord {

    private ZonedDateTime friendshipDate;
    private Long followingUser;
    private Long followedUser;

    public FriendshipRecord(ZonedDateTime friendshipDate, Long followingUser, Long followedUser) {
        this.friendshipDate = friendshipDate;
        this.followingUser = followingUser;
        this.followedUser = followedUser;
    }


    public ZonedDateTime getFriendshipDate() {
        return friendshipDate;
    }

    public void setFriendshipDate(ZonedDateTime friendshipDate) {
        this.friendshipDate = friendshipDate;
    }


    public String getRawHour() {
        String hour = String.valueOf(friendshipDate.getHour());
        if (hour.length() == 1) {
            hour = "0" + hour;
        }
        return hour;
    }

    public Long getFollowingUser() {
        return followingUser;
    }

    public void setFollowingUser(Long followingUser) {
        this.followingUser = followingUser;
    }

    public Long getFollowedUser() {
        return followedUser;
    }

    public void setFollowedUser(Long followedUser) {
        this.followedUser = followedUser;
    }
}
