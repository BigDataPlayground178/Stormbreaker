package entities.records;

import java.util.Date;

public class FriendshipRecord {

    private Date friendshipDate;
    private Integer followingUser;
    private Integer followedUser;

    public FriendshipRecord(Date friendshipDate, Integer followingUser, Integer followedUser) {
        this.friendshipDate = friendshipDate;
        this.followingUser = followingUser;
        this.followedUser = followedUser;
    }


    public Date getFriendshipDate() {
        return friendshipDate;
    }

    public void setFriendshipDate(Date friendshipDate) {
        this.friendshipDate = friendshipDate;
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
