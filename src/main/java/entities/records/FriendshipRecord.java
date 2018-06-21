package entities.records;

import java.util.Date;

public class FriendshipRecord {

    private Date friendshipDate;
    private String rawFriendshipDate;
    private Integer followingUser;
    private Integer followedUser;

    public FriendshipRecord(Date friendshipDate, String rawFriendshipDate, Integer followingUser, Integer followedUser) {
        this.friendshipDate = friendshipDate;
        this.rawFriendshipDate = rawFriendshipDate;
        this.followingUser = followingUser;
        this.followedUser = followedUser;
    }


    public Date getFriendshipDate() {
        return friendshipDate;
    }

    public void setFriendshipDate(Date friendshipDate) {
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

    public Integer getFollowingUser() {
        return followingUser;
    }

    public void setFollowingUser(Integer followingUser) {
        this.followingUser = followingUser;
    }

    public Integer getFollowedUser() {
        return followedUser;
    }

    public void setFollowedUser(Integer followedUser) {
        this.followedUser = followedUser;
    }
}
