package operators.sinks;

import entities.results.PostRank;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Point;

import java.util.concurrent.TimeUnit;

public class InfluxDBPostRankSink extends RichSinkFunction<PostRank> {

    private transient InfluxDB influxDB;

    private String series_name;

    public InfluxDBPostRankSink(String series_name) {
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
    public void invoke(PostRank value, SinkFunction.Context context) {
        influxDB.write(Point.measurement(series_name)
                .time(System.currentTimeMillis(), TimeUnit.MILLISECONDS)
                .addField("rank", value.getTopRank().toString())
                .build());
    }

    @Override
    public void close() throws Exception {
        influxDB.close();
        super.close();
    }
}
