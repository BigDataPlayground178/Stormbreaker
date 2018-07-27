package operators.maps;

import entities.results.FriendshipCount;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.dropwizard.metrics.DropwizardMeterWrapper;
import org.apache.flink.metrics.Meter;

/**
 * Utility class to handle metrics over friendship stream
 */
public class FriendshipCountTp extends RichMapFunction<FriendshipCount, Object> {
    private transient Meter meter;
    private String metername;
    private double maxRate = 0.0;

    public FriendshipCountTp(String name) {
        this.metername = name;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        com.codahale.metrics.Meter dropwizardMeter = new com.codahale.metrics.Meter();
        this.meter = getRuntimeContext().getMetricGroup()
                .meter(metername, new DropwizardMeterWrapper(dropwizardMeter));
    }

    @Override
    public Object map(FriendshipCount friendshipCount) throws Exception {
        this.meter.markEvent();
        if (meter.getRate() > maxRate) {
            maxRate = meter.getRate();
        }
        System.out.println(metername + " rate: " + meter.getRate());
        return friendshipCount;
    }
}

