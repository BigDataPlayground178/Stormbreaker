package entities.records;

import java.time.ZonedDateTime;

/**
 * This is the utility class to retrieve comments records
 */
public class CommentRecord {
    private ZonedDateTime timestamp;
    private Long comment_id;
    private Long user_id;
    private String comment;
    private String user;
    private Long comment_replied = null;
    private Long post_commented = null;

    public CommentRecord(ZonedDateTime timestamp, Long comment_id, Long user_id, String comment, String user, Long comment_replied, Long post_commented) {
        this.timestamp = timestamp;
        this.comment_id = comment_id;
        this.user_id = user_id;
        this.comment = comment;
        this.user = user;
        this.comment_replied = comment_replied;
        this.post_commented = post_commented;
    }

    public boolean isCommentToPost() {
        return post_commented != null;
    }

    public ZonedDateTime getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(ZonedDateTime timestamp) {
        this.timestamp = timestamp;
    }

    public Long getComment_id() {
        return comment_id;
    }

    public void setComment_id(Long comment_id) {
        this.comment_id = comment_id;
    }

    public Long getUser_id() {
        return user_id;
    }

    public void setUser_id(Long user_id) {
        this.user_id = user_id;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public Long getComment_replied() {
        return comment_replied;
    }

    public void setComment_replied(Long comment_replied) {
        this.comment_replied = comment_replied;
    }

    public Long getPost_commented() {
        return post_commented;
    }

    public void setPost_commented(Long post_commented) {
        this.post_commented = post_commented;
    }
}
