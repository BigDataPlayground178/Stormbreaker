package operators.sinks;

import entities.results.FriendshipCount;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Point;

import java.util.Map;
import java.util.concurrent.TimeUnit;

public class InfluxDBFriendshipCountSink extends RichSinkFunction<FriendshipCount> {

    private transient InfluxDB influxDB;
    private String series_name;

    public InfluxDBFriendshipCountSink(String series_name) {
        this.series_name = series_name;
    }



    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        InfluxDB influxDB = InfluxDBFactory.connect("http://127.0.0.1:8086");
        influxDB.setDatabase("mydb");
        this.influxDB = influxDB;
    }

    @Override
    public void invoke(FriendshipCount value, Context context) {
        influxDB.write(Point.measurement(series_name)
                .time(value.getTs().toInstant().toEpochMilli(), TimeUnit.MILLISECONDS)
                .fields((Map) value.getCounters())
                .build());
    }

    @Override
    public void close() throws Exception {
        influxDB.close();
        super.close();
    }
}
