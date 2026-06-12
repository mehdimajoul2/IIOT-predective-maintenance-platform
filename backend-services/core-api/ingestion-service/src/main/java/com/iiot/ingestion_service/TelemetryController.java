package com.iiot.ingestion_service;

import java.util.List;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.bind.annotation.GetMapping;


@RestController
@RequestMapping("/api/telemetry")
@CrossOrigin(origins = "http://localhost:4200")
public class TelemetryController {

    private TelemetryQueryService queryService;

    public TelemetryController(TelemetryQueryService queryService){
        this.queryService = queryService;
    }

    @GetMapping("/history")
    public ResponseEntity<List<TelemetryResponse>> getTelemetryHistory() {
        
        List<TelemetryResponse> history = queryService.getRecentTelemetry();
        
        return ResponseEntity.ok(history);
    }
    
    
}
