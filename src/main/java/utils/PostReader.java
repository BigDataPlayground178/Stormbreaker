package utils;

import entities.records.PostRecord;
import org.apache.flink.api.common.functions.MapFunction;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

/**
 * This class can be used to map raw strings into PostRecords
 */
public class PostReader implements MapFunction<String, PostRecord> {
    @Override
    public PostRecord map(String s) throws Exception {
        String[] elems = s.split("\\|", -1);
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSX");

        PostRecord postRecord = new PostRecord(
                ZonedDateTime.parse(elems[0], formatter),
                Long.valueOf(elems[1]),
                Long.valueOf(elems[2]),
                elems[3],
                elems[4]
        );
        return postRecord;
    }
}
