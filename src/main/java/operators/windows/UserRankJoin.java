package operators.windows;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class UserRankJoin implements JoinFunction<Tuple2<Long, Integer>, Tuple2<Long, Integer>, Tuple2<Long, Integer>> {

    @Override
    public Tuple2<Long, Integer> join(Tuple2<Long, Integer> t1, Tuple2<Long, Integer> t2) throws Exception {
        // computing score
        Integer score = t1.f1 + t2.f1;
        return new Tuple2<>(t1.f0, score);
    }
}
