package entities.records;

import java.time.ZonedDateTime;

public class FriendshipRecord {

    private ZonedDateTime friendshipDate;
    private String rawFriendshipDate;
    private Long followingUser;
    private Long followedUser;

    public FriendshipRecord(ZonedDateTime friendshipDate, String rawFriendshipDate, Long followingUser, Long followedUser) {
        this.friendshipDate = friendshipDate;
        this.rawFriendshipDate = rawFriendshipDate;
        this.followingUser = followingUser;
        this.followedUser = followedUser;
    }


    public ZonedDateTime getFriendshipDate() {
        return friendshipDate;
    }

    public void setFriendshipDate(ZonedDateTime friendshipDate) {
        this.friendshipDate = friendshipDate;
    }

    public String getRawFriendshipDate() {
        return rawFriendshipDate;
    }

    public String getRawHour() {
        String[] d = getRawFriendshipDate().split("T")[1].split(":");
        return d[0]; // 00, 01, 02, ... , 22, 23
    }

    public void setRawFriendshipDate(String rawFriendshipDate) {
        this.rawFriendshipDate = rawFriendshipDate;
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
