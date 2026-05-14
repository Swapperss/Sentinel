logging audit logs.

Json -

{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "VehicleTelemetry",
  "type": "object",
  "properties": {
    "vin": { "type": "string" },
    "timestamp": { "type": "string" },
    "engine_temp": { "type": "number" },
    "speed": { "type": "integer" }
  },
  "required": ["vin", "timestamp", "engine_temp", "speed"]
}

syntax = "proto3";

package sentinel.telemetry;

import "google/protobuf/timestamp.proto";

message VehicleTelemetry {
  string vin = 1;
  google.protobuf.Timestamp event_time = 2;
  
  message Metrics {
    double engine_temp_f = 1;
    int32 speed_kmh = 2;
    int32 battery_soc = 3;
  }
  
  Metrics metrics = 3;
}