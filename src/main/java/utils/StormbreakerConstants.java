package utils;

public class StormbreakerConstants {

    public static final String STORMBREAKER_ENV        = "Stormbreaker";

    public static final String KAFKA_BOOTSTRAP_SERVER  = "localhost:9092";
    public static final String KAFKA_ZOOKEEPER_SERVER  = "localhost:2181";
    public static final String KAFKA_FRIENDSHIP_TOPIC  = "friendships";
    public static final String KAFKA_COMMENTS_TOPIC    = "comments";
    public static final String KAFKA_POSTS_TOPIC       = "posts";

    public static final String INFLUX_DB_HOST          = "127.0.0.1";
    public static final String INFLUX_DB_PORT          = "8086";

    public static final String FRIENDSHIP_DAT_PATH     = "friendships.dat";
    public static final String COMMENTS_DAT_PATH       = "comments.dat";
    public static final String POSTS_DAT_PATH          = "posts.dat";

    public static final String TIMESTAMP               = "ts";
    public static final String HOUR_COUNTER_PREFIX     = "count_h";

    public static final Long DATASET_START_TIMESTAMP   = 1265001152L * 1000;
    public static final Integer DATASET_STATS_MINUTES  = 60;

    public static final Integer USERS_RANKING_HOUR     = 1;
    public static final Integer USERS_RANKING_DAY_HOUR = 24;
    public static final Integer USERS_RANKING_WEEK_DAY = 7;
    public static final Integer USERS_RANKING_MAX      = 10;

    public static final Integer POST_RANKING_MAX       = 10;
}
