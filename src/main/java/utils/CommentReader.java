package utils;

import entities.records.CommentRecord;
import org.apache.flink.api.common.functions.MapFunction;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

public class CommentReader implements MapFunction<String, CommentRecord> {
    @Override
    public CommentRecord map(String s) throws Exception {
        String[] elems = s.split("\\|", -1);
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSX");

        Long comment_replied = elems[5].equals("") ?  null : Long.valueOf(elems[5]);
        Long post_commented = elems[6].equals("") ? null : Long.valueOf(elems[6]);

        CommentRecord comment = new CommentRecord(ZonedDateTime.parse(elems[0], formatter),
                Long.valueOf(elems[1]),
                Long.valueOf(elems[2]),
                elems[3],
                elems[4],
                comment_replied,
                post_commented);

        return comment;
    }
}
