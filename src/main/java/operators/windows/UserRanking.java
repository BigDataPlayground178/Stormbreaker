package operators.windows;

import entities.results.UserRank;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.shaded.curator.org.apache.curator.shaded.com.google.common.collect.Iterables;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Arrays;

import static utils.StormbreakerConstants.USERS_RANKING_MAX;

public class UserRanking implements AllWindowFunction<Tuple3<Long, Integer, Long>, UserRank, TimeWindow> {

    @Override
    public void apply(TimeWindow timeWindow, Iterable<Tuple3<Long, Integer, Long>> collection, Collector<UserRank> out) throws Exception {

        // using aux class
        class User implements Comparable<User> {
            final Integer score;
            final Long userID;

            User(Long userID, Integer score) {
                this.userID = userID;
                this.score = score;
            }

            @Override
            public int compareTo(User u) {
                return (int) (this.score - ((User) u).score);
            }
        }

        // creating auxiliary array
        User[] users = new User[Iterables.size(collection)];

        // iterating over collection
        int i = 0;
        Long ts = 0L;
        for (Tuple3<Long, Integer, Long> record : collection) {
            // retrieving first timestamp
            if (i == 0)
                ts = record.f2;
            // updating users array
            users[i] = new User(record.f0, record.f1);
            i++;
        }

        // sorting by rank
        Arrays.sort(users);

        // creating rank object
        UserRank rank = new UserRank();
        rank.setTs(ts);

        // setting rank attributes
        for (User user : users) {
            rank.incrementCount();
            rank.addUser(new Tuple2<>(user.userID, user.score));
            if (rank.count == USERS_RANKING_MAX)
                break;
        }

        out.collect(rank);
    }
}
