package com.iiot.ingestion_service;

public record TelemetryResponse(String device_id, String timestamp, double temperature ) {

    
}
