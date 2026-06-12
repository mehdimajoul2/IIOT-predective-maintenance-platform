package com.iiot.ingestion_service;

import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.QueryApi;
import com.influxdb.query.FluxRecord;
import com.influxdb.query.FluxTable;

@Service
public class TelemetryQueryService {
    
    private final InfluxDBClient influxClient;
    private final String bucket;

    public TelemetryQueryService(@Value("${influxdb.url}") String url, 
                @Value("${influxdb.token}") String token, 
                @Value("${influxdb.org}") String org,
                @Value("${influxdb.bucket}") String bucket){
        this.influxClient = InfluxDBClientFactory.create(url,token.toCharArray(),org);
        this.bucket = bucket;
    }

    public List<TelemetryResponse> getRecentTelemetry(){
        List<TelemetryResponse> results = new ArrayList<>();

        String queryFlux = String.format(
            "from(bucket: \"%s\")" +
            "|> range(start: -1h)" +
            "|> filter(fn: (r) => r._measurement == \"machine_telemetry\")" +
            "|> filter(fn: (r) => r._field ==  \"temperature\")" +
            "|> sort(columns: [\"_time\"], desc: false)",bucket);
        
        QueryApi queryApi = influxClient.getQueryApi();

        List<FluxTable> tables = queryApi.query(queryFlux);
        for (FluxTable table: tables){
            for (FluxRecord record: table.getRecords()){

                String time = record.getTime().toString();
                double temperature = (Double) record.getValueByKey("_value");

                String device_id = (String) record.getValueByKey("device_id");

                results.add(new TelemetryResponse(device_id, time, temperature));
            }
        }

        return results;
    }
}
