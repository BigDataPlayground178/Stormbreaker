package operators.windows;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

/**
 * This class is an extension of a CoGroupFunction to join "a" and "bc" score streams
 * in order to compute correctly the "abc" score
 */
public class UserRankJoin implements CoGroupFunction<Tuple3<Long, Integer, Long>, Tuple3<Long, Integer, Long>, Tuple3<Long, Integer, Long>> {


    @Override
    public void coGroup(Iterable<Tuple3<Long, Integer, Long>> aTuples, Iterable<Tuple3<Long, Integer, Long>> bcTuples, Collector<Tuple3<Long, Integer, Long>> collector) throws Exception {
        Long userID = 0L;
        Long timestamp = Long.MAX_VALUE;
        Integer score  = 0;

        // if there are "aTuples" (there should be 0 or 1)
        if (aTuples != null) {
            for (Tuple3<Long, Integer, Long> t : aTuples) {
                // set the userID of "abcTuple" as the userID of the "aTuple"
                userID = t.f0;
                // set the "abcTuple" timestamp as the lowest recorded timestamp from all tuples
                if (t.f2 < timestamp) { timestamp = t.f2; }
                // update the score
                score += t.f1;
            }
        }

        // if there are "bcTuples" (there should be 0 or 1)
        if (bcTuples != null) {
            for (Tuple3<Long, Integer, Long> t : bcTuples) {
                // set the userID of "abcTuple" as the userID of the "bcTuple"
                userID = t.f0;
                // set the "abcTuple" timestamp as the lowest recorded timestamp from all tuples
                if (t.f2 < timestamp) { timestamp = t.f2; }
                // update the score
                score += t.f1;
            }
        }

        collector.collect(new Tuple3<Long, Integer, Long>(userID, score, timestamp));
    }
}
