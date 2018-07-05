package entities.records;

import java.time.ZonedDateTime;

/**
 * This is the utility class to retrieve post records
 */
public class PostRecord {
    private ZonedDateTime timestamp;
    private Long post_id;
    private Long user_id;
    private String post;
    private String user;

    public PostRecord(ZonedDateTime timestamp, Long post_id, Long user_id, String post, String user) {
        this.timestamp = timestamp;
        this.post_id = post_id;
        this.user_id = user_id;
        this.post = post;
        this.user = user;
    }

    public ZonedDateTime getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(ZonedDateTime timestamp) {
        this.timestamp = timestamp;
    }

    public Long getPost_id() {
        return post_id;
    }

    public void setPost_id(Long post_id) {
        this.post_id = post_id;
    }

    public Long getUser_id() {
        return user_id;
    }

    public void setUser_id(Long user_id) {
        this.user_id = user_id;
    }

    public String getPost() {
        return post;
    }

    public void setPost(String post) {
        this.post = post;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }
}
