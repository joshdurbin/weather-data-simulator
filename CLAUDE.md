# Weather Data Simulator

A high-performance Redis pub/sub stress testing tool that simulates a distributed weather monitoring system with realistic data generation, policy-based alerting, and configurable network chaos injection.

## Purpose

This tool is designed to:

1. **Stress test Redis pub/sub** - Generate high-volume message throughput with configurable numbers of publishers and subscribers
2. **Simulate realistic IoT scenarios** - Model weather stations broadcasting data and consumers monitoring for alerts
3. **Test network resilience** - Inject configurable network failures to validate retry logic and fault tolerance
4. **Benchmark distributed systems** - Measure message rates, latency, and system behavior under load

## Architecture

### Components

**Weather Stations (Publishers)**
- Generate realistic weather data with gradual changes over time
- Publish data to Redis pub/sub channel every second
- Each station has a fixed geographic location
- Weather values evolve realistically (90% no change, 5% increase, 5% decrease)

**Weather Consumers (Subscribers)**
- Subscribe to weather data pub/sub channel
- Each consumer has a fixed geographic location
- Evaluate incoming data against active policies
- Generate alerts when policies are violated within range
- Use Redis client-side caching for policy lookups

**Policy System**
- Store alert rules in Redis set
- Define thresholds for weather metrics (temperature, humidity, wind speed, etc.)
- Include geographic range for location-based filtering
- Policies evaluated by consumers using Haversine distance calculations

**Monitoring Dashboard**
- Real-time TUI displaying message rates and system stats
- Shows Redis network throughput (RX/TX)
- Displays recent alerts with timestamps
- Updates every second

### Data Model

**Location**
```json
{
  "latitude": 37.7749,
  "longitude": -122.4194
}
```

**Weather Data**
```json
{
  "station_id": 42,
  "location": {"latitude": 37.7749, "longitude": -122.4194},
  "temperature": 72.5,
  "humidity": 65.0,
  "wind_direction": 180,
  "wind_speed": 12.5,
  "barometric_pressure": 30.1,
  "cloud_coverage": 45,
  "timestamp": 1234567890
}
```

**Policy**
```json
{
  "id": 1,
  "field": "temperature",
  "operator": ">",
  "value": 100.0,
  "location_range": 5000
}
```

## Weather Simulation

Weather values evolve realistically using a probabilistic state machine:

- **Initial values**: Randomly generated within realistic ranges
- **Updates per second**:
  - 90% probability: no change
  - 5% probability: increase by up to 5% of current value
  - 5% probability: decrease by up to 5% of current value
- **Bounds**: Values clamped to realistic ranges (e.g., temp 60-120°F)

This creates smooth, natural weather patterns rather than random noise.

## Network Chaos Injection

The `ChaosDialer` wraps `net.Dialer` to simulate network failures:

- **Configurable failure rate** per component (stations, consumers)
- **First 3 connections always succeed** to ensure startup
- **Random connection failures** at specified rate
- **Tests resilience**: rueidis client automatically retries failed operations

Default: 5% failure rate simulates realistic network conditions.

## Usage

### Run Simulator

```bash
# Default: Bay Area, CA (37-38.5°N, 123-121°W)
./main run

# Custom configuration
./main run \
  --stations 500 \
  --consumers 500 \
  --station-failure-rate 0.1 \
  --consumer-failure-rate 0.05 \
  --min-lat 37.0 --max-lat 38.5 \
  --min-lon -123.0 --max-lon -121.0

# Different region (Continental US)
./main run \
  --min-lat 25.0 --max-lat 49.0 \
  --min-lon -125.0 --max-lon -66.0
```

### Generate Policies

```bash
# Generate 5-10 random policies (appends)
./main generate-policies

# Clear existing and generate new
./main generate-policies --clear

# Custom Redis
./main generate-policies --redis redis.example.com:6379 --clear
```

### Build

```bash
make build    # Compile binary
make clean    # Remove binary
make deps     # Update dependencies
make run      # Build and run
```

## Performance Characteristics

With default settings (250 stations, 250 consumers):

- **Message rate**: ~250 messages/second published
- **Message consumption**: ~62,500 messages/second received (250 consumers × 250 msgs)
- **Network throughput**: Varies based on policy count and alert rate
- **Redis connections**: 501 total (250 stations + 250 consumers + 1 monitor)

The system is designed to scale to thousands of stations/consumers for stress testing.

## Technical Details

### Distance Calculations

Uses **Haversine formula** for accurate geographic distance on Earth's surface:
- Accounts for Earth's curvature
- Returns distance in meters
- Used for policy location range filtering

### Redis Operations

- **Pub/Sub**: `PUBLISH` for stations, `SUBSCRIBE` for consumers
- **Client-side caching**: Policies cached for 1 minute to reduce load
- **Set operations**: `SADD` for policy storage, `SMEMBERS` for retrieval
- **Info stats**: Periodic `INFO` calls for monitoring

### Concurrency Model

- One goroutine per station (publishes every second)
- One goroutine per consumer (blocking subscription with callback)
- One goroutine for monitoring (TUI and stats aggregation)
- Channels for stats aggregation with non-blocking sends (drops on overflow)

### Error Handling

- Stations/consumers log errors but continue running
- Network failures handled by rueidis retry logic
- Channel overflows silently drop stats (prioritizes message throughput)
- Graceful shutdown on SIGINT/SIGTERM

## Dependencies

- **rueidis**: High-performance Redis client with client-side caching
- **bubbletea**: Terminal UI framework for monitoring dashboard
- **cobra**: Command-line interface and subcommands
- **Standard library**: net, crypto/tls, math (Haversine), encoding/json

## Future Enhancements

Potential additions:
- HTTP API for policy management (currently Redis-only)
- Metrics export (Prometheus format)
- Historical data storage (TimeSeries)
- Geographic visualization of alerts
- Custom weather patterns (storms, fronts)
- Multi-region support with Redis Cluster