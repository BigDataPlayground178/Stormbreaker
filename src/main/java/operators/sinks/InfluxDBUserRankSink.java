package operators.sinks;

import entities.results.UserRank;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Point;

import java.util.concurrent.TimeUnit;

import static utils.StormbreakerConstants.*;


/**
 * Sink Class that allows the injection of UserRank objects in InfluxDB
 */
public class InfluxDBUserRankSink extends RichSinkFunction<UserRank> {

    private transient InfluxDB influxDB;

    private String series_name;

    /**
     * @param series_name name of the timeseries in InfluxDB
     */
    public InfluxDBUserRankSink(String series_name) {
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
    public void invoke(UserRank value, SinkFunction.Context context) {
        influxDB.write(Point.measurement(series_name)
                .time(value.getTs(), TimeUnit.MILLISECONDS)
                //.addField("rank", value.getTopRank().toString())
                .fields(value.getTopRank())
                .build());

        influxDB.write(Point.measurement(series_name + "_rank")
                .time(value.getTs(), TimeUnit.MILLISECONDS)
                .addField("rank", value.getTopRank().toString())
                .build());
    }

    @Override
    public void close() throws Exception {
        influxDB.close();
        super.close();
    }
}
