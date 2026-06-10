const mqtt = require('mqtt');

// Connect to your local Mosquitto Docker container
const client = mqtt.connect('mqtt://localhost:1883');

const TOPIC = 'factory/machine1/telemetry';
let baseTemperature = 25.0;

client.on('connect', () => {
    console.log('Simulator connected to local MQTT Broker!');
    
    // Run an infinite loop every 3 seconds (3000 ms)
    setInterval(() => {
        // Create some fake fluctuating temperature data
        baseTemperature += (Math.random() - 0.5) * 2; 
        
        const payload = JSON.stringify({
            device_id: "virtual-node-01",
            temperature: parseFloat(baseTemperature.toFixed(2)),
            timestamp: new Date().toISOString()
        });

        // Publish to the broker
        client.publish(TOPIC, payload);
        console.log(`Published: ${payload}`);
        
    }, 3000);
});

client.on('error', (err) => {
    console.error('Connection error: ', err);
});