package operators.windows;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

public class UserRankJoin implements CoGroupFunction<Tuple3<Long, Integer, Long>, Tuple3<Long, Integer, Long>, Tuple3<Long, Integer, Long>> {


    @Override
    public void coGroup(Iterable<Tuple3<Long, Integer, Long>> aTuples, Iterable<Tuple3<Long, Integer, Long>> bcTuples, Collector<Tuple3<Long, Integer, Long>> collector) throws Exception {
        Long userID = 0L;
        Long timestamp = Long.MAX_VALUE;
        Integer score  = 0;

        if (aTuples != null) {
            for (Tuple3<Long, Integer, Long> t : aTuples) {
                userID = t.f0;
                if (t.f2 < timestamp) { timestamp = t.f2; }
                score += t.f1;
            }
        }

        if (bcTuples != null) {
            for (Tuple3<Long, Integer, Long> t : bcTuples) {
                userID = t.f0;
                if (t.f2 < timestamp) { timestamp = t.f2; }
                score += t.f1;
            }
        }

        collector.collect(new Tuple3<Long, Integer, Long>(userID, score, timestamp));
    }
}
