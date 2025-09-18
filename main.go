package main

import (
	"container/ring"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"math/rand"
	"net"
	"os"
	"os/signal"
	"reflect"
	"sort"
	"syscall"
	"time"

	"github.com/open-policy-agent/opa/rego"
	metrics "github.com/rcrowley/go-metrics"
	"github.com/redis/rueidis"
	"github.com/spf13/cobra"
	"github.com/umahmood/haversine"
	"golang.org/x/sync/errgroup"
	"gonum.org/v1/gonum/stat"
)

// Location represents a geographic coordinate point (stored as integers: degrees * 1000000)
type Location struct {
	Latitude  int `json:"latitude"`  // microdegrees (degrees * 1000000)
	Longitude int `json:"longitude"` // microdegrees (degrees * 1000000)
}

// generateRandomLocation creates a random location within bounds
func generateRandomLocation(minLat, maxLat, minLon, maxLon float64) Location {
	lat := minLat + rand.Float64()*(maxLat-minLat)
	lon := minLon + rand.Float64()*(maxLon-minLon)
	return Location{
		Latitude:  int(lat * 1000000),
		Longitude: int(lon * 1000000),
	}
}

// LatencyConn wraps a net.Conn to inject random latency on Read/Write operations
type LatencyConn struct {
	net.Conn
	probability float64
	maxLatency  time.Duration
}

// Read injects latency before reading
func (c *LatencyConn) Read(b []byte) (n int, err error) {
	if c.probability > 0 && rand.Float64() < c.probability {
		delay := time.Duration(rand.Int63n(int64(c.maxLatency)))
		time.Sleep(delay)
	}
	return c.Conn.Read(b)
}

// Write injects latency before writing
func (c *LatencyConn) Write(b []byte) (n int, err error) {
	if c.probability > 0 && rand.Float64() < c.probability {
		delay := time.Duration(rand.Int63n(int64(c.maxLatency)))
		time.Sleep(delay)
	}
	return c.Conn.Write(b)
}

// WeatherFields contains the actual weather measurements (all integers)
type WeatherFields struct {
	Temperature        int `json:"temperature"`         // °F * 1000
	Humidity           int `json:"humidity"`            // % * 1000
	WindDirection      int `json:"wind_direction"`      // degrees (0-359)
	WindSpeed          int `json:"wind_speed"`          // mph * 1000
	BarometricPressure int `json:"barometric_pressure"` // inHg * 1000
	CloudCoverage      int `json:"cloud_coverage"`      // % * 1000
}

// WeatherData represents the complete weather information from a station
type WeatherData struct {
	StationID int      `json:"station_id"`
	Location  Location `json:"location"`
	WeatherFields
	Timestamp int64 `json:"timestamp"`
}

// WeatherState maintains the current weather values and handles realistic updates
type WeatherState struct {
	WeatherFields
}

// NewWeatherState creates a new weather state with random initial values
func NewWeatherState() *WeatherState {
	return &WeatherState{
		WeatherFields: WeatherFields{
			Temperature:        60000 + rand.Intn(60000), // 60-120°F * 1000
			Humidity:           rand.Intn(100000),        // 0-100% * 1000
			WindDirection:      rand.Intn(360),           // 0-359°
			WindSpeed:          rand.Intn(60000),         // 0-60 mph * 1000
			BarometricPressure: 28000 + rand.Intn(4000),  // 28-32 inHg * 1000
			CloudCoverage:      rand.Intn(100000),        // 0-100% * 1000
		},
	}
}

// applyRealisticChange applies a realistic change to an integer value
// 90% chance: no change, 5% chance: increase, 5% chance: decrease
// Changes are up to 5% of current value, clamped to min/max bounds
func (ws *WeatherState) applyRealisticChange(current, min, max int) int {
	r := rand.Float64()
	if r > 0.9 {
		// 10% chance of change
		changePercent := rand.Float64() * 0.05 // up to 5%
		change := int(float64(current) * changePercent)
		if r > 0.95 {
			// 5% chance: decrease
			current -= change
		} else {
			// 5% chance: increase
			current += change
		}
	}
	// Clamp to bounds
	if current < min {
		current = min
	}
	if current > max {
		current = max
	}
	return current
}

// Update applies realistic changes to the weather state
// 90% chance: no change, 5% chance: increase, 5% chance: decrease
func (ws *WeatherState) Update() {
	ws.Temperature = ws.applyRealisticChange(ws.Temperature, 60000, 120000)
	ws.Humidity = ws.applyRealisticChange(ws.Humidity, 0, 100000)
	ws.WindSpeed = ws.applyRealisticChange(ws.WindSpeed, 0, 60000)
	ws.BarometricPressure = ws.applyRealisticChange(ws.BarometricPressure, 28000, 32000)
	ws.CloudCoverage = ws.applyRealisticChange(ws.CloudCoverage, 0, 100000)

	// Wind direction changes independently
	if rand.Float64() > 0.9 {
		change := int((rand.Float64()*2 - 1) * 0.05 * float64(ws.WindDirection))
		ws.WindDirection = (ws.WindDirection + change + 360) % 360
	}
}

// ToWeatherData converts the weather state to a WeatherData object
func (ws *WeatherState) ToWeatherData(stationID int, location Location) WeatherData {
	return WeatherData{
		StationID:     stationID,
		Location:      location,
		WeatherFields: ws.WeatherFields,
		Timestamp:     time.Now().Unix(),
	}
}

// WeatherRing wraps container/ring for WeatherData storage
type WeatherRing struct {
	*ring.Ring
}

// NewWeatherRing creates a ring buffer with specified size
func NewWeatherRing(size int) *WeatherRing {
	return &WeatherRing{Ring: ring.New(size)}
}

// Add inserts a sample and advances the ring
func (wr *WeatherRing) Add(sample WeatherData) {
	wr.Value = sample
	wr.Ring = wr.Next()
}

// GetAll returns all non-nil samples in chronological order
func (wr *WeatherRing) GetAll() []WeatherData {
	var result []WeatherData
	wr.Do(func(v interface{}) {
		if v != nil {
			if data, ok := v.(WeatherData); ok {
				result = append(result, data)
			}
		}
	})
	return result
}

// StationTracker manages tracking of weather data from stations
type StationTracker struct {
	stationData map[int]*WeatherRing
	sampleSize  int
}

// NewStationTracker creates a new station tracker
func NewStationTracker(sampleSize int) *StationTracker {
	return &StationTracker{
		stationData: make(map[int]*WeatherRing),
		sampleSize:  sampleSize,
	}
}

// AddSample adds a weather data sample for a station
func (st *StationTracker) AddSample(data WeatherData) {
	// Lazy initialize ring buffer for this station
	if st.stationData[data.StationID] == nil {
		st.stationData[data.StationID] = NewWeatherRing(st.sampleSize)
	}

	st.stationData[data.StationID].Add(data)
}

// GetStationSamples returns all samples for a specific station
func (st *StationTracker) GetStationSamples(stationID int) []WeatherData {
	if rb := st.stationData[stationID]; rb != nil {
		return rb.GetAll()
	}
	return nil
}

// FieldStats contains statistical information about a weather field
type FieldStats struct {
	Min        float64
	Max        float64
	Mean       float64
	Trend      string // "increasing", "decreasing", "stable"
	SampleSize int
}

// extractFieldValue uses reflection to get a field value by name
func extractFieldValue(sample WeatherData, fieldName string) (float64, bool) {
	val := reflect.ValueOf(sample)
	field := val.FieldByName(fieldName)
	if !field.IsValid() {
		return 0, false
	}

	intVal := field.Int()
	// WindDirection doesn't need division by 1000
	if fieldName == "WindDirection" {
		return float64(intVal), true
	}
	return float64(intVal) / 1000.0, true
}

// GetFieldStats computes statistics using reflection for any WeatherFields field
func (st *StationTracker) GetFieldStats(stationID int, fieldName string) *FieldStats {
	samples := st.GetStationSamples(stationID)
	if len(samples) == 0 {
		return nil
	}

	values := make([]float64, 0, len(samples))
	for _, sample := range samples {
		if val, ok := extractFieldValue(sample, fieldName); ok {
			values = append(values, val)
		}
	}

	if len(values) == 0 {
		return nil
	}

	// Sort values for quantile calculation (required by gonum/stat)
	sort.Float64s(values)

	min := stat.Quantile(0, stat.Empirical, values, nil)
	max := stat.Quantile(1, stat.Empirical, values, nil)
	mean := stat.Mean(values, nil)

	trend := "stable"
	if len(values) >= 10 {
		midpoint := len(values) / 2
		firstHalf := stat.Mean(values[:midpoint], nil)
		secondHalf := stat.Mean(values[midpoint:], nil)
		percentChange := (secondHalf - firstHalf) / firstHalf
		if percentChange > 0.01 {
			trend = "increasing"
		} else if percentChange < -0.01 {
			trend = "decreasing"
		}
	}

	return &FieldStats{
		Min:        min,
		Max:        max,
		Mean:       mean,
		Trend:      trend,
		SampleSize: len(values),
	}
}

// GetAllFieldStats returns stats for all weather fields using reflection
func (st *StationTracker) GetAllFieldStats(stationID int) map[string]*FieldStats {
	stats := make(map[string]*FieldStats)
	fieldsType := reflect.TypeOf(WeatherFields{})

	for i := 0; i < fieldsType.NumField(); i++ {
		fieldName := fieldsType.Field(i).Name
		if fieldStats := st.GetFieldStats(stationID, fieldName); fieldStats != nil {
			stats[fieldName] = fieldStats
		}
	}

	return stats
}

// WeatherPolicy represents a policy rule for weather alerts using OPA Rego for dynamic evaluation
type WeatherPolicy struct {
	ID          int    `json:"id"`
	MaxDistance int    `json:"max_distance"` // meters, for pre-filtering stations by proximity
	Rego        string `json:"rego"`         // OPA Rego policy module
	query       *rego.PreparedEvalQuery
}

// Alert represents a triggered alert for JSON serialization
type Alert struct {
	Timestamp        string                 `json:"timestamp"`
	ConsumerID       int                    `json:"consumer_id"`
	PolicyID         int                    `json:"policy_id"`
	PolicyRego       string                 `json:"policy_rego"`
	StationID        int                    `json:"station_id"`
	StationLatitude  float64                `json:"station_latitude"`
	StationLongitude float64                `json:"station_longitude"`
	WeatherData      map[string]interface{} `json:"weather_data"`
}

// Compile prepares the Rego policy for evaluation
func (p *WeatherPolicy) Compile(ctx context.Context) error {
	// Create a new Rego query that evaluates data.weather.allow
	r := rego.New(
		rego.Query("data.weather.allow"),
		rego.Module("policy.rego", p.Rego),
	)

	// Prepare the query for repeated evaluation
	query, err := r.PrepareForEval(ctx)
	if err != nil {
		return err
	}

	p.query = &query
	return nil
}

// Evaluate checks if weather data violates this policy using OPA Rego
func (p *WeatherPolicy) Evaluate(ctx context.Context, data WeatherData, consumerLocation Location) bool {
	if p.query == nil {
		return false
	}

	// Calculate distance between station and consumer
	point1 := haversine.Coord{
		Lat: float64(data.Location.Latitude) / 1000000.0,
		Lon: float64(data.Location.Longitude) / 1000000.0,
	}
	point2 := haversine.Coord{
		Lat: float64(consumerLocation.Latitude) / 1000000.0,
		Lon: float64(consumerLocation.Longitude) / 1000000.0,
	}
	_, km := haversine.Distance(point1, point2)
	distanceMeters := km * 1000

	// Build input for Rego evaluation (convert integers to floats)
	input := map[string]interface{}{
		"Temperature":        float64(data.Temperature) / 1000.0,
		"Humidity":           float64(data.Humidity) / 1000.0,
		"WindSpeed":          float64(data.WindSpeed) / 1000.0,
		"BarometricPressure": float64(data.BarometricPressure) / 1000.0,
		"CloudCoverage":      float64(data.CloudCoverage) / 1000.0,
		"WindDirection":      float64(data.WindDirection), // Already in degrees, no conversion
		"Distance":           distanceMeters,
	}

	// Evaluate the policy
	results, err := p.query.Eval(ctx, rego.EvalInput(input))
	if err != nil {
		return false
	}

	// Check if policy allows (violation detected)
	if len(results) > 0 && len(results[0].Expressions) > 0 {
		if allow, ok := results[0].Expressions[0].Value.(bool); ok {
			return allow
		}
	}

	return false
}

var (
	redisAddr          string
	numStations        int
	numConsumers       int
	minLatitude        float64
	maxLatitude        float64
	minLongitude       float64
	maxLongitude       float64
	sampleSize         int
	alertsFile         string
	latencyProbability float64
	maxLatency         time.Duration
	weatherTopic       = "weather:data"
)

func main() {
	var rootCmd = &cobra.Command{
		Use:   "weather-data-simulator",
		Short: "Redis pub/sub stress testing with weather stations",
	}

	// Run command - default behavior
	var runCmd = &cobra.Command{
		Use:   "run",
		Short: "Run the weather data simulator",
		Run:   runApplication,
	}
	runCmd.Flags().StringVar(&redisAddr, "redis", "localhost:6379", "Redis server address")
	runCmd.Flags().IntVar(&numStations, "stations", 250, "Number of weather stations")
	runCmd.Flags().IntVar(&numConsumers, "consumers", 250, "Number of weather data consumers")
	runCmd.Flags().Float64Var(&minLatitude, "min-lat", 37.0, "Minimum latitude")
	runCmd.Flags().Float64Var(&maxLatitude, "max-lat", 38.5, "Maximum latitude")
	runCmd.Flags().Float64Var(&minLongitude, "min-lon", -123.0, "Minimum longitude")
	runCmd.Flags().Float64Var(&maxLongitude, "max-lon", -121.0, "Maximum longitude")
	runCmd.Flags().IntVar(&sampleSize, "sample-size", 600, "Number of samples to keep per station in each consumer")
	runCmd.Flags().StringVar(&alertsFile, "alerts-file", "alerts.log", "File to write alert outputs")
	runCmd.Flags().Float64Var(&latencyProbability, "latency-probability", 0.0, "Probability (0.0-1.0) of injecting latency on network operations")
	runCmd.Flags().DurationVar(&maxLatency, "max-latency", 0, "Maximum latency to inject (e.g., 100ms, 1s)")

	rootCmd.AddCommand(runCmd)

	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func runApplication(cmd *cobra.Command, args []string) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create metrics meters
	messagesSent := metrics.NewMeter()
	messagesProcessed := metrics.NewMeter()
	alertsTriggered := metrics.NewMeter()
	messagesDropped := metrics.NewMeter()

	// Create errgroup
	g, ctx := errgroup.WithContext(ctx)

	// Start weather stations
	for i := 1; i <= numStations; i++ {
		stationID := i
		g.Go(func() error {
			return runWeatherStation(ctx, stationID, messagesSent)
		})
	}

	// Start weather consumers
	for i := 1; i <= numConsumers; i++ {
		consumerID := i
		g.Go(func() error {
			return runWeatherConsumer(ctx, consumerID, messagesProcessed, alertsTriggered, messagesDropped)
		})
	}

	// Start monitor
	g.Go(func() error {
		return runMonitor(ctx, messagesSent, messagesProcessed, alertsTriggered, messagesDropped)
	})

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-sigChan
		cancel()
	}()

	// Wait for all goroutines
	if err := g.Wait(); err != nil && err != context.Canceled {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func runWeatherStation(ctx context.Context, stationID int, messagesSent metrics.Meter) error {
	client, err := createRedisClient()
	if err != nil {
		return fmt.Errorf("station %d failed to create Redis client: %w", stationID, err)
	}
	defer client.Close()

	location := generateRandomLocation(minLatitude, maxLatitude, minLongitude, maxLongitude)
	weatherState := NewWeatherState()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			weatherState.Update()
			weatherData := weatherState.ToWeatherData(stationID, location)

			data, _ := json.Marshal(weatherData)
			result := client.Do(ctx, client.B().Publish().Channel(weatherTopic).Message(string(data)).Build())

			if result.Error() == nil {
				messagesSent.Mark(1)
			}
		}
	}
}

// createLatencyDialer returns a custom dialer that wraps connections with latency injection
func createLatencyDialer() func(string, *net.Dialer, *tls.Config) (net.Conn, error) {
	return func(addr string, dialer *net.Dialer, tlsConfig *tls.Config) (net.Conn, error) {
		conn, err := dialer.Dial("tcp", addr)
		if err != nil {
			return nil, err
		}

		// Wrap with latency injection if enabled
		if latencyProbability > 0 && maxLatency > 0 {
			return &LatencyConn{
				Conn:        conn,
				probability: latencyProbability,
				maxLatency:  maxLatency,
			}, nil
		}

		return conn, nil
	}
}

func createRedisClient() (rueidis.Client, error) {
	opts := rueidis.ClientOption{
		InitAddress: []string{redisAddr},
	}

	// Add custom dialer if latency injection is enabled
	if latencyProbability > 0 && maxLatency > 0 {
		opts.DialFn = createLatencyDialer()
	}

	return rueidis.NewClient(opts)
}

// getTrendStatus returns a human-readable trend status from field stats
func getTrendStatus(stats map[string]*FieldStats, fieldName string) string {
	if stats == nil {
		return "unknown"
	}
	if fieldStats, ok := stats[fieldName]; ok && fieldStats != nil {
		return fieldStats.Trend
	}
	return "unknown"
}

func runWeatherConsumer(ctx context.Context, consumerID int, messagesProcessed, alertsTriggered, messagesDropped metrics.Meter) error {
	var cachedPolicies []WeatherPolicy

	opts := rueidis.ClientOption{
		InitAddress: []string{redisAddr},
	}

	// Add custom dialer if latency injection is enabled
	if latencyProbability > 0 && maxLatency > 0 {
		opts.DialFn = createLatencyDialer()
	}

	client, err := rueidis.NewClient(opts)
	if err != nil {
		return fmt.Errorf("consumer %d failed to create Redis client: %w", consumerID, err)
	}
	defer client.Close()

	// Open alerts file in append mode
	alertFile, err := os.OpenFile(alertsFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("consumer %d failed to open alerts file: %w", consumerID, err)
	}
	defer alertFile.Close()

	consumerLocation := generateRandomLocation(minLatitude, maxLatitude, minLongitude, maxLongitude)

	// Create station tracker for dynamic station monitoring
	stationTracker := NewStationTracker(sampleSize)

	// Policy management goroutine - polls Redis every minute
	go func() {
		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()

		// Initial load
		result := client.Do(ctx, client.B().Smembers().Key("weather:policies").Build())
		if result.Error() == nil {
			if members, err := result.AsStrSlice(); err == nil {
				newPolicies := make([]WeatherPolicy, 0, len(members))
				for _, member := range members {
					var policy WeatherPolicy
					if json.Unmarshal([]byte(member), &policy) == nil {
						if err := policy.Compile(ctx); err == nil {
							newPolicies = append(newPolicies, policy)
						}
					}
				}
				cachedPolicies = newPolicies
			}
		}

		// Periodic reload
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				result := client.Do(ctx, client.B().Smembers().Key("weather:policies").Build())
				if result.Error() == nil {
					if members, err := result.AsStrSlice(); err == nil {
						newPolicies := make([]WeatherPolicy, 0, len(members))
						for _, member := range members {
							var policy WeatherPolicy
							if json.Unmarshal([]byte(member), &policy) == nil {
								if err := policy.Compile(ctx); err == nil {
									newPolicies = append(newPolicies, policy)
								}
							}
						}
						cachedPolicies = newPolicies
					}
				}
			}
		}
	}()

	// Subscribe to weather data channel
	err = client.Receive(ctx, client.B().Subscribe().Channel(weatherTopic).Build(), func(msg rueidis.PubSubMessage) {
		var weatherData WeatherData
		if json.Unmarshal([]byte(msg.Message), &weatherData) == nil {
			// Skip all messages if no policies exist
			if len(cachedPolicies) == 0 {
				messagesDropped.Mark(1)
				return
			}

			// Calculate distance between consumer and station
			point1 := haversine.Coord{
				Lat: float64(weatherData.Location.Latitude) / 1000000.0,
				Lon: float64(weatherData.Location.Longitude) / 1000000.0,
			}
			point2 := haversine.Coord{
				Lat: float64(consumerLocation.Latitude) / 1000000.0,
				Lon: float64(consumerLocation.Longitude) / 1000000.0,
			}
			_, km := haversine.Distance(point1, point2)
			distanceMeters := km * 1000

			// Check if station is within range of any policy
			inRange := false
			for _, policy := range cachedPolicies {
				if distanceMeters <= float64(policy.MaxDistance) {
					inRange = true
					break
				}
			}

			// Skip messages from stations outside policy range
			if !inRange {
				messagesDropped.Mark(1)
				return
			}

			messagesProcessed.Mark(1)

			// Store the sample in the ring buffer
			stationTracker.AddSample(weatherData)

			// Evaluate cached policies
			for _, policy := range cachedPolicies {
				if policy.Evaluate(ctx, weatherData, consumerLocation) {
					alertsTriggered.Mark(1)

					// Get field statistics for trend information
					allStats := stationTracker.GetAllFieldStats(weatherData.StationID)

					// Create alert object
					alert := Alert{
						Timestamp:        time.Now().UTC().Format(time.RFC3339),
						ConsumerID:       consumerID,
						PolicyID:         policy.ID,
						PolicyRego:       policy.Rego,
						StationID:        weatherData.StationID,
						StationLatitude:  float64(weatherData.Location.Latitude) / 1000000.0,
						StationLongitude: float64(weatherData.Location.Longitude) / 1000000.0,
						WeatherData: map[string]interface{}{
							"temperature": map[string]interface{}{
								"value": float64(weatherData.Temperature) / 1000.0,
								"unit":  "°F",
								"trend": getTrendStatus(allStats, "Temperature"),
							},
							"humidity": map[string]interface{}{
								"value": float64(weatherData.Humidity) / 1000.0,
								"unit":  "%",
								"trend": getTrendStatus(allStats, "Humidity"),
							},
							"wind_speed": map[string]interface{}{
								"value": float64(weatherData.WindSpeed) / 1000.0,
								"unit":  "mph",
								"trend": getTrendStatus(allStats, "WindSpeed"),
							},
							"wind_direction": map[string]interface{}{
								"value": weatherData.WindDirection,
								"unit":  "°",
								"trend": getTrendStatus(allStats, "WindDirection"),
							},
							"barometric_pressure": map[string]interface{}{
								"value": float64(weatherData.BarometricPressure) / 1000.0,
								"unit":  "inHg",
								"trend": getTrendStatus(allStats, "BarometricPressure"),
							},
							"cloud_coverage": map[string]interface{}{
								"value": float64(weatherData.CloudCoverage) / 1000.0,
								"unit":  "%",
								"trend": getTrendStatus(allStats, "CloudCoverage"),
							},
						},
					}

					// Marshal to JSON and write to file
					alertJSON, err := json.Marshal(alert)
					if err == nil {
						fmt.Fprintf(alertFile, "%s\n", alertJSON)
					}
				}
			}
		}
	})

	return err
}

func runMonitor(ctx context.Context, messagesSent, messagesProcessed, alertsTriggered, messagesDropped metrics.Meter) error {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	startTime := time.Now()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			elapsed := int(time.Since(startTime).Seconds())

			sentRate := messagesSent.Rate1()
			procRate := messagesProcessed.Rate1()
			alertRate := alertsTriggered.Rate1()
			droppedRate := messagesDropped.Rate1()

			fmt.Print("\033[H\033[2J")
			fmt.Printf("Uptime: %ds\n", elapsed)
			fmt.Printf("Messages Sent:      %d (%.1f/sec)\n", messagesSent.Count(), sentRate)
			fmt.Printf("Messages Processed: %d (%.1f/sec)\n", messagesProcessed.Count(), procRate)
			fmt.Printf("Messages Dropped:   %d (%.1f/sec)\n", messagesDropped.Count(), droppedRate)
			fmt.Printf("Alerts Triggered:   %d (%.1f/sec)\n", alertsTriggered.Count(), alertRate)
		}
	}
}
