package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"lockfreemachine/src/pkg/commons"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/Workiva/go-datastructures/queue"
	"github.com/avast/retry-go"
	"github.com/gorilla/mux"
	"github.com/prometheus/client_golang/prometheus"
    "github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
    "go.uber.org/zap/zapcore"
    "gopkg.in/natefinch/lumberjack.v2"
)

var logger *zap.Logger

var ( 
	httpRequests *prometheus.CounterVec
	requestDuration *prometheus.HistogramVec
	apiCallDuration *prometheus.HistogramVec
	setupTime *prometheus.GaugeVec
)

func initMetrics(brokerID int) {

	 // Create a wrapped Registerer that injects labels
	wrappedRegisterer := prometheus.WrapRegistererWith(
			prometheus.Labels{"brokerID": fmt.Sprint(brokerID)}, // <- static label here
			prometheus.DefaultRegisterer,
	)

	httpRequests = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "http_requests_total",
			Help: "Count of all HTTP requests grouped by endpoint",
		},
		[]string{"path", "method"},
	)

	requestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "api_request_duration_milliseconds",
			Help:    "Duration of API requests in milliseconds",
			Buckets: prometheus.DefBuckets, // You can customize this
		},
		[]string{"path", "method","srcID","srcType"}, // group by endpoint/method if you want
	)

	apiCallDuration = prometheus.NewHistogramVec(
        prometheus.HistogramOpts{
            Name:    "outbound_api_call_duration_seconds",
            Help:    "Duration of outbound API calls in seconds",
            Buckets: prometheus.DefBuckets,
        },
        []string{ "method","targetID"},
    )

	setupTime = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "setup_time",
			Help: "Time taken to set up the broker",
		},
		[]string{},
	)
	
    wrappedRegisterer.MustRegister(httpRequests)
	wrappedRegisterer.MustRegister(requestDuration)
	wrappedRegisterer.MustRegister(apiCallDuration)
	wrappedRegisterer.MustRegister(setupTime)
}

const (
	BUFFER_SIZE = 10000
)

type Broker struct {
	ID             int
	IP             string
	Port           int
	TransactionQueue   *queue.RingBuffer
	DirectoryAddr  string
	DirectoryInfo  *commons.NodesMap
	httpServer         *http.Server
	isTest bool
}
// Middleware to measure duration with dynamic labels
func MetricsMiddleware(next http.Handler) http.Handler {
    return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        nodeID := r.Header.Get("X-NodeID") // Grab dynamic tenant ID from headers
		nodeType := r.Header.Get("X-NodeType") // Grab dynamic tenant ID from headers
        if nodeID == "" {
            nodeID = "unknown"
			nodeType = "unknown"
        }

        start := time.Now()

        // Serve the real handler
        next.ServeHTTP(w, r)

        duration := time.Since(start).Seconds()

        // Record with dynamic label
        requestDuration.WithLabelValues(r.URL.Path, r.Method, nodeID,nodeType).Observe(duration)
    })
}

func NewBroker(id int, ip string, port int, directoryIP string, directoryPort int,isTest bool) (*Broker, error) {
	transactionQueue := queue.NewRingBuffer(BUFFER_SIZE)
	directoryAddr := fmt.Sprintf("http://%s:%d", directoryIP, directoryPort)
	directoryInfo := &commons.NodesMap{
		ServerMap: &commons.DirectoryMap{Data: make(map[int]*commons.NodeInfo)},
		BrokerMap: &commons.DirectoryMap{Data: make(map[int]*commons.NodeInfo)},
	}

	broker := &Broker{
		ID:            id,
		IP:            ip,
		Port:          port,
		TransactionQueue:  transactionQueue,
		DirectoryAddr: directoryAddr,
		DirectoryInfo: directoryInfo,
		isTest: isTest,
	}

	if err := broker.register(); err != nil {
		return nil, fmt.Errorf("error registering broker: %v", err)
	}

	if err := commons.BroadcastNodesInfo(logger,broker.ID,commons.BrokerType,broker.DirectoryInfo); err != nil {
		return nil, fmt.Errorf("error broadcasting nodes info: %v", err)
	}

	r := mux.NewRouter()
	r.Use(MetricsMiddleware)

	r.HandleFunc(commons.BROKER_UPDATE_DIRECTORY, commons.HandleUpdateDirectory(logger,broker.DirectoryInfo)).Methods("POST")
	r.HandleFunc(commons.BROKER_TRANSACTION, broker.handleTransactionRequest).Methods("POST")

	// Prometheus metrics
	initMetrics(id)
	r.Path("/metrics").Handler(promhttp.Handler())

	broker.httpServer = &http.Server{
		Addr:    fmt.Sprintf(":%d", broker.Port),
		Handler: r,
	}

	return broker, nil
}

func (b *Broker) register() error {
	data := map[string]interface{}{
		"ip":   b.IP,
		"port": b.Port,
	}

	jsonData, err := json.Marshal(data)
	if err != nil {
		logger.Error("Error marshalling JSON", zap.Error(err))
		return fmt.Errorf("error encoding JSON: %v", err)
	}

	resp, err := http.Post(fmt.Sprintf("%s%s", b.DirectoryAddr,commons.DIRECTORY_REGISTER_BROKER), "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		logger.Error("Error sending request to directory", zap.Error(err))
		return fmt.Errorf("error sending request to directory: %v, could not start broker", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		logger.Error("Unexpected status code from directory", zap.Int("statusCode", resp.StatusCode))
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	body, _ := io.ReadAll(resp.Body)


	if err := json.Unmarshal(body, &b.DirectoryInfo); err != nil {
		logger.Error("Error unmarshalling JSON", zap.Error(err))
		return fmt.Errorf("error unmarshalling JSON: %v", err)
	}

	logger.Info("Directory info received", zap.Any("directoryInfo", b.DirectoryInfo))
	b.ID = b.DirectoryInfo.BrokerMap.Version

	logger.Info("Broker registered", zap.Int("brokerID", b.ID))
	return nil
}

func (b *Broker) handleTransactionRequest(w http.ResponseWriter, r *http.Request) {
	// Read data from request
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Error reading request body", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	// Process the data
	transaction := &commons.Transaction{}
	json.Unmarshal(body, &transaction)

	// Example: Print the data
	b.TransactionQueue.Put(transaction)
	w.WriteHeader(http.StatusOK)
}

func (b *Broker) sendPackage(serverURL string,jsonData []byte) func() error {
	return func() error {
		// Create an HTTP client with a per-request timeout
		client := &http.Client{
			Timeout: commons.BROKER_REQUEST_TIMEOUT,
		}

		req, err := http.NewRequest("POST", serverURL, bytes.NewBuffer(jsonData))
		if err != nil {
			panic(err)
		}

		// Set headers
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-NodeID", fmt.Sprint(b.ID))
		req.Header.Set("X-NodeType", commons.GetNodeType(commons.BrokerType))

		resp, err := client.Do(req)

		if err != nil {
			return fmt.Errorf("error sending request to %s: %s", serverURL, err)
		}
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("error reading response body: %v", err)
		}
		bodyString := string(body)


		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("unexpected status code: %d for server %s: error: %s", resp.StatusCode, serverURL,bodyString)
		}
		return nil
	}
}

func (b *Broker) protocolDaemon(nextRun time.Time) {
	packageCounter := 0
	logger.Info("Starting protocol daemon...")

	// We want the ticker to have millisecond precision
	ticker := time.NewTicker(time.Millisecond)
	logger.Info("Next run will happen at", zap.Time("nextRun", nextRun))

	for tc := range ticker.C {
		if tc.Before(nextRun) {
			continue
		}
		nextRun = nextRun.Add(commons.EPOCH_PERIOD)
		packageCounter++

		var pkg *commons.Package
		if b.isTest {
			pkg = b.DummyPackage(packageCounter,10,10)
		} else {
			pkg= b.CreatePackage(packageCounter)
		}

		logger.Info("Creating package", zap.Int("packageID", pkg.PackageID), zap.Int("transactionsCount", len(pkg.Transactions)))

		jsonData, err := json.Marshal(pkg)
		if err != nil {
			logger.Error("Error marshalling package", zap.Error(err))
			continue
		}


		var wg sync.WaitGroup
		for serverID, node := range b.DirectoryInfo.ServerMap.Data {
			wg.Add(1)
			go func(serverID int, node *commons.NodeInfo) {
				defer wg.Done()

				if err != nil {
					logger.Error("Error marshalling node info", zap.Int("serverID", serverID), zap.Error(err))
					return
				}

				logger.Info("Sending package to server", zap.Int("serverID", serverID), zap.String("ip", node.IP), zap.Int64("port", node.Port))

				retry.Do(
					b.sendPackage(fmt.Sprintf("http://%s:%d%s", node.IP, node.Port,commons.SERVER_ADD_PACKAGE),jsonData),
					retry.Attempts(commons.BROKER_RETRY),               // Number of retry attempts
					retry.Delay(0),      // No delay
					retry.DelayType(retry.FixedDelay), // Use fixed delay strategy
					retry.OnRetry(func(n uint, err error) {
						logger.Warn("Retrying package send", zap.Int("serverID", serverID), zap.Int("attempt", int(n)), zap.Error(err))
					}),
				)
			}(serverID, node)
		}
	
		if err := commons.BroadcastNodesInfo(logger,b.ID,commons.BrokerType,b.DirectoryInfo); err != nil {
			logger.Error("Error broadcasting nodes info", zap.Error(err))
		}

		wg.Wait()
		logger.Info("Next run will happen at", zap.Time("nextRun", nextRun))
	}
}

func (b *Broker) DummyPackage(packageCounter,nOperation,nTransaction int) *commons.Package {
	var transactions []*commons.Transaction
	for j:=0 ; j<nTransaction; j++ {
		var operations []*commons.Operation
		for i := 0; i < nOperation; i++ {
			operation := &commons.Operation{
				Key: fmt.Sprintf("key%d", i),
				Value: fmt.Sprintf("value%d", i),
				Op: 1,
				Timestamp:   time.Now().UnixNano(),
			}
			operations = append(operations, operation)
		}
		transaction := &commons.Transaction{
			Operations:  operations,
		}
		transactions = append(transactions, transaction)
	}

	pkg := &commons.Package{
		BrokerID:       b.ID,
		Transactions:   transactions,
		PackageID: 	 packageCounter,
	}
	
	logger.Info("Dummy package created",zap.Int("packageCounter",packageCounter),zap.Int("transactionsCount",len(pkg.Transactions)))
	return pkg
}

func (b *Broker) CreatePackage(packageCounter int) *commons.Package {
	var transactions []*commons.Transaction
	for b.TransactionQueue.Len()>0 {
		if b.TransactionQueue.Len() == 0 {
			logger.Info("Transaction array is empty")
			break
		}

		transactionIntf, err := b.TransactionQueue.Poll(2 * time.Second)
		if err != nil {
			logger.Error("Error getting transaction from queue", zap.Error(err))
			continue
		}

		transaction, ok := transactionIntf.(*commons.Transaction)
		if !ok {
			logger.Error("Error type assertion for transaction", zap.Any("transaction", transactionIntf))
			continue
		}

		transactions = append(transactions, transaction)
	}

	pkg := &commons.Package{
		BrokerID:       b.ID,
		Transactions:   transactions,
		PackageID: 	 packageCounter,
	}

	return pkg
}

func main() {
		// Parse command line arguments
	var directoryIP string
	var directoryPort int

	var logFile string
	flag.StringVar(&logFile, "logFile", "./logs/broker.log", "Path to the log file")


	flag.StringVar(&directoryIP, "directoryIP", "", "IP of directory")
	flag.IntVar(&directoryPort, "directoryPort", 0, "Port of directory")

	var brokerIP string
	var brokerPort int

	flag.StringVar(&brokerIP, "brokerIP", "", "IP of current broker")
	flag.IntVar(&brokerPort, "brokerPort", 0, "Port of broker")

	var startTimestamp int64
	flag.Int64Var(&startTimestamp, "startTimestamp", 0, "Start timestamp")
	
	isTest:=flag.Bool("test", false, "Run in test mode")
	flag.Parse()

	if directoryIP == "" {
		fmt.Print("directoryIP is required")
		return
	}
	if directoryPort == 0 {
		fmt.Print("directoryPort is required")
		return
	}
	if brokerIP == "" {
		fmt.Printf("brokerIP is required")
		return
	}
	if brokerPort == 0 {
		fmt.Printf("brokerPort is required")
		return
	}
	if startTimestamp == 0 {
		fmt.Printf("startTimestamp is required")
		return
	}

	err := os.Mkdir("./logs", 0755)
    if err != nil && !os.IsExist(err) {
        // Only log or handle real errors
        panic(err)
    }

	// Configure Lumberjack for log rotation
	lumberjackLogger := &lumberjack.Logger{
		Filename:   logFile, // Log file path
		MaxSize:    10,    // Max megabytes before log is rotated
		MaxBackups: 5,     // Max old log files to keep
		MaxAge:     28,    // Max days to retain old log files
		Compress:   true,  // Compress old files (.gz)
	}
		
	// Create Zap core with Lumberjack
	writeSyncer := zapcore.AddSync(lumberjackLogger)
	encoderConfig := zap.NewDevelopmentEncoderConfig()
	encoderConfig.TimeKey = "timestamp"
	encoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
		
	core := zapcore.NewCore(
		zapcore.NewJSONEncoder(encoderConfig),
		writeSyncer,
		zapcore.InfoLevel,
	)
			
	logger = zap.New(core)
	logger = logger.With(zap.String("service", fmt.Sprintf("broker %s",brokerIP)))

	defer logger.Sync()

	broker, err := NewBroker(0, brokerIP, brokerPort, directoryIP, directoryPort,*isTest)
	if err != nil {
		logger.Error("Error setting up broker", zap.Error(err))
		return
	}

	// Graceful shutdown handling
	go func() {
		if err := broker.httpServer.ListenAndServe(); err != http.ErrServerClosed {
			logger.Error("Error starting broker", zap.Error(err))
		}
	}()

	// Print the broker's address
	logger.Info("Broker running on http://localhost:%d", zap.Int("port", broker.Port))

	go func() {
		broker.protocolDaemon(time.Unix(0, startTimestamp))
		defer fmt.Println("Daemon routine stopped.")
	}()
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt)
	<-quit

	logger.Info("Shutting down broker...")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	broker.httpServer.Shutdown(ctx)
	logger.Info("Broker shutdown complete.")
}