package operators.sinks;

import entities.results.PostRank;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Point;

import java.util.concurrent.TimeUnit;

import static utils.StormbreakerConstants.*;

/**
 * Sink Class that allows the injection of PostRank objects in InfluxDB
 */
public class InfluxDBPostRankSink extends RichSinkFunction<PostRank> {

    private transient InfluxDB influxDB;

    private String series_name;

    /**
     * @param series_name name of the timeseries in InfluxDB
     */
    public InfluxDBPostRankSink(String series_name) {
        this.series_name = series_name;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        InfluxDB influxDB = InfluxDBFactory.connect("http://" + INFLUX_DB_HOST + ":" + INFLUX_DB_PORT);
        influxDB.setDatabase(INFLUX_DB_DB);
        this.influxDB = influxDB;
    }

    @Override
    public void invoke(PostRank value, SinkFunction.Context context) {
        influxDB.write(Point.measurement(series_name)
                .time(value.getTS(), TimeUnit.MILLISECONDS)
                //.addField("rank", value.getTopRank().toString())
                .fields(value.getTopRank())
                .build());

        influxDB.write(Point.measurement(series_name + "_rank")
                .time(value.getTS(), TimeUnit.MILLISECONDS)
                .addField("rank", value.getTopRank().toString())
                .build());
    }

    @Override
    public void close() throws Exception {
        influxDB.close();
        super.close();
    }
}
