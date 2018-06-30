package utils;

import entities.records.FriendshipRecord;
import org.apache.flink.api.common.functions.MapFunction;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

public class FriendshipReader implements MapFunction<String, FriendshipRecord> {

    public FriendshipRecord map(String s) throws Exception {
        // <timestamp>|<user1>|<user2> lines must be processed
        String[] r = s.split("\\|",  -1);

        // processing timestamp to return a Date
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSX");

        return new FriendshipRecord(ZonedDateTime.parse(r[0], formatter), Long.valueOf(r[1]), Long.valueOf(r[2]));
    }
}
