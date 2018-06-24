package utils;

import entities.records.FriendshipRecord;
import org.apache.flink.api.common.functions.MapFunction;

import java.text.SimpleDateFormat;
import java.util.Date;

public class FriendshipReader implements MapFunction<String, FriendshipRecord> {

    public FriendshipRecord map(String s) throws Exception {
        // <timestamp>|<user1>|<user2> lines must be processed
        String[] r = s.split("\\|",  -1);

        // processing timestamp to return a Date
        SimpleDateFormat sdf = new SimpleDateFormat("YYYY-MM-DD'T'HH:mm:ss.SSSz");
        Date recordDate = sdf.parse(r[0]);

        return new FriendshipRecord(recordDate, r[0], Long.valueOf(r[1]), Long.valueOf(r[2]));
    }
}
