package operators.windows;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple3;

public class UserRankJoin implements JoinFunction<Tuple3<Long, Integer, Long>, Tuple3<Long, Integer, Long>, Tuple3<Long, Integer, Long>> {

    @Override
    public Tuple3<Long, Integer, Long> join(Tuple3<Long, Integer, Long> t1, Tuple3<Long, Integer, Long> t2) throws Exception {
        // computing score
        Integer score = t1.f1 + t2.f1;
        return new Tuple3<>(t1.f0, score, t1.f2);
    }
}
