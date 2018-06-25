package operators.selectors;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;

public class UserKeySelector implements KeySelector<Tuple2<Long, Integer>, Long> {
    @Override
    public Long getKey(Tuple2<Long, Integer> t) throws Exception {
        return t.f0;
    }
}
