package com.iiot.ingestion_service;

import java.time.Instant;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.WriteApiBlocking;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;

import tools.jackson.databind.ObjectMapper;
@Service
public class KafkaToInfluxConsumer {

    private final InfluxDBClient influxClient;
    private final String bucket;
    private final String org;
    private final ObjectMapper objectMapper;

    public record TelemetryPayload(String device_id, double temperature, String timestamp ) {
    }

    public KafkaToInfluxConsumer(@Value("${influxdb.url}") String url, 
                @Value("${influxdb.token}") String token, 
                @Value("${influxdb.org}") String org,
                @Value("${influxdb.bucket}") String bucket,
                ObjectMapper objectMapper){
        this.influxClient = InfluxDBClientFactory.create(url,token.toCharArray(),org);
        this.bucket = bucket;
        this.org = org;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "${kafka.topic}", groupId = "iiot-storage-group")
    public void consumeTelemetry(String message){
        System.out.println("Kafka received data: " + message);
        try{
            TelemetryPayload payload = objectMapper.readValue(message, TelemetryPayload.class);
            System.out.println("Kafka received data: " + payload.temperature);

            Instant eventTime = Instant.parse(payload.timestamp());
            System.out.println("Kafka received data: " + eventTime.toString());

            Point point = Point.measurement("machine_telemetry")
                               .addTag("device_id", payload.device_id())
                               .addField("temperature", payload.temperature())
                               .time(eventTime, WritePrecision.NS);

            WriteApiBlocking writeApi = influxClient.getWriteApiBlocking();
            writeApi.writePoint(bucket,org,point);
            System.out.println("data saved: " + payload.device_id() + ", " + payload.timestamp()); 
        }catch(Exception e){
            System.out.println("Failed to parse: "+ e.getMessage());
        }
    }
}
