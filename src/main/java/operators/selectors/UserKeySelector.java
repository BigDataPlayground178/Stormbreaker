package operators.selectors;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;

public class UserKeySelector implements KeySelector<Tuple3<Long, Integer, Long>, Long> {
    @Override
    public Long getKey(Tuple3<Long, Integer, Long> t) throws Exception {
        return t.f0;
    }
}
