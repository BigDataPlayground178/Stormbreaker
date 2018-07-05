package operators.windows;

import entities.results.UserRank;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class UserRanking implements AllWindowFunction<Tuple3<Long, Integer, Long>, UserRank, TimeWindow> {

    @Override
    public void apply(TimeWindow timeWindow, Iterable<Tuple3<Long, Integer, Long>> collection, Collector<UserRank> out) throws Exception {
        UserRank ur = new UserRank();

        for (Tuple3<Long, Integer, Long> user : collection) {
            ur.addUser(user);
            if (user.f2 < ur.getTs()) {
                ur.setTs(user.f2);
            }
        }

        out.collect(ur);
    }
}
