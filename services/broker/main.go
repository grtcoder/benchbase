package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"lockfreemachine/pkg/commons"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
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
	httpRequests    *prometheus.CounterVec
	requestDuration *prometheus.HistogramVec
	apiCallDuration *prometheus.HistogramVec
	//setupTime       *prometheus.GaugeVec
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
			Name:    "api_request_duration_seconds",
			Help:    "Duration of API requests in seconds",
			Buckets: prometheus.DefBuckets, // You can customize this
		},
		[]string{"path", "method", "srcType"}, // group by endpoint/method if you want
	)

	apiCallDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "outbound_api_call_duration_seconds",
			Help:    "Duration of outbound API calls in seconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"method", "targetID"},
	)

	wrappedRegisterer.MustRegister(httpRequests)
	wrappedRegisterer.MustRegister(requestDuration)
	wrappedRegisterer.MustRegister(apiCallDuration)
}

const (
	BUFFER_SIZE = 10000
)

type Broker struct {
	ID                int
	IP                string
	Port              int
	ReaderPort        int // same as Port ->for consistency, otherwise I would need to make major changes in the directory
	TransactionQueue  *queue.RingBuffer
	DirectoryAddr     string
	DirectoryInfo     *commons.NodesMap
	httpServer        *http.Server
	isTest            bool
	dropRate          float64 // Used to randomly drop packages received
	packageCountStart int
	startTimestamp    time.Time
}

// Middleware to measure duration with dynamic labels
func (b *Broker) MetricsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/metrics" {
			next.ServeHTTP(w, r)
			return
		}

		if rand.Float64() < b.dropRate {
			// increment a “dropped” metric if you want:
			httpRequests.WithLabelValues(r.URL.Path, r.Method).Inc()
			http.Error(w, "simulated network failure", http.StatusServiceUnavailable)
			return
		}

		nodeID := r.Header.Get("X-NodeID")
		nodeType := r.Header.Get("X-NodeType")
		if nodeID == "" {
			nodeID = "unknown"
			nodeType = "unknown"
		}

		httpRequests.WithLabelValues(r.URL.Path, r.Method).Inc()
		start := time.Now()

		// Serve the real handler
		next.ServeHTTP(w, r)

		duration := time.Since(start).Seconds()

		// Record with dynamic label
		requestDuration.WithLabelValues(r.URL.Path, r.Method, nodeType).Observe(duration)
	})
}

var sharedTr = &http.Transport{
	MaxIdleConns:        512,
	MaxIdleConnsPerHost: 128,
	IdleConnTimeout:     90 * time.Second,
	MaxConnsPerHost:     256,
	ForceAttemptHTTP2:   true,
	DialContext: (&net.Dialer{
		Timeout:   commons.BROKER_PER_ATTEMPT_TIMEOUT, // TCP connect cap
		KeepAlive: 30 * time.Second,
	}).DialContext,
	TLSHandshakeTimeout:   commons.BROKER_PER_ATTEMPT_TIMEOUT,
	ResponseHeaderTimeout: commons.BROKER_PER_ATTEMPT_TIMEOUT,
	ExpectContinueTimeout: 50 * time.Millisecond,
}

func makeInstrumentedClient(targetID string) *http.Client {
	// Curry the targetID label, leaving method to be filled per-request
	curried := apiCallDuration.MustCurryWith(prometheus.Labels{
		"targetID": targetID,
	})

	// Wrap the default transport
	instrumentedTransport := promhttp.InstrumentRoundTripperDuration(
		curried,  // ObserverVec for apiCallDuration
		sharedTr, // underlying RoundTripper
	)

	return &http.Client{
		Transport: instrumentedTransport,
		Timeout:   commons.BROKER_OVERALL_TIMEOUT,
	}
}

func NewBroker(id int, ip string, port int, directoryIP string, directoryPort int, isTest bool, dropRate float64) (*Broker, error) {
	transactionQueue := queue.NewRingBuffer(BUFFER_SIZE)
	directoryAddr := fmt.Sprintf("http://%s:%d", directoryIP, directoryPort)
	directoryInfo := &commons.NodesMap{
		ServerMap: &commons.DirectoryMap{Data: make(map[int]*commons.NodeInfo)},
		BrokerMap: &commons.DirectoryMap{Data: make(map[int]*commons.NodeInfo)},
	}

	broker := &Broker{
		ID:               id,
		IP:               ip,
		Port:             port,
		ReaderPort:       port,
		TransactionQueue: transactionQueue,
		DirectoryAddr:    directoryAddr,
		DirectoryInfo:    directoryInfo,
		isTest:           isTest,
		dropRate:         dropRate,
	}

	if err := broker.register(); err != nil {
		return nil, fmt.Errorf("error registering broker: %v", err)
	}

	if err := commons.BroadcastNodesInfo(logger, broker.ID, commons.BrokerType, broker.DirectoryInfo, sharedTr); err != nil {
		return nil, fmt.Errorf("error broadcasting nodes info: %v", err)
	}

	r := mux.NewRouter()
	r.Use(broker.MetricsMiddleware)

	r.HandleFunc(commons.BROKER_UPDATE_DIRECTORY, commons.HandleUpdateDirectory(logger, broker.DirectoryInfo)).Methods("POST")
	r.HandleFunc(commons.BROKER_TRANSACTION, broker.handleTransactionRequest).Methods("POST")

	// Prometheus metrics
	initMetrics(broker.ID)
	r.Path("/metrics").Handler(promhttp.Handler())

	broker.httpServer = &http.Server{
		Addr:    fmt.Sprintf(":%d", broker.Port),
		Handler: r,
	}

	return broker, nil
}

func (b *Broker) delete_server(ServerID int) error {
	data := map[string]interface{}{
		"id": ServerID,
	}

	jsonData, err := json.Marshal(data)
	if err != nil {
		logger.Error("Error marshalling JSON", zap.Error(err))
		return fmt.Errorf("error encoding JSON: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), commons.BROKER_PER_ATTEMPT_TIMEOUT)
	defer cancel()

	client := makeInstrumentedClient("delete-server")

	req, err := http.NewRequestWithContext(ctx,
		"POST",
		fmt.Sprintf("%s%s", b.DirectoryAddr, commons.DIRECTORY_REMOVE_SERVER),
		bytes.NewBuffer(jsonData),
	)
	if err != nil {
		return fmt.Errorf("build req: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		logger.Error("Error sending request to directory", zap.Error(err))
		return fmt.Errorf("error sending request to directory: %v, could not delete server entry", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		defer io.Copy(io.Discard, resp.Body)
		logger.Error("Unexpected status code from directory", zap.Int("statusCode", resp.StatusCode))
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	body, _ := io.ReadAll(resp.Body)

	newDirectoryMap := &commons.NodesMap{}
	if err := json.Unmarshal(body, &newDirectoryMap); err != nil {
		logger.Error("Error unmarshalling JSON", zap.Error(err))
	}
	ndm := newDirectoryMap
	go func(m *commons.NodesMap) {
		b.DirectoryInfo.CheckAndUpdateMap(m)
	}(ndm)
	logger.Info("Directory info received", zap.Any("directoryInfo", b.DirectoryInfo))

	logger.Info("Server Removed", zap.Int("serverID", ServerID))
	return nil
}

type registerResp struct {
	Timestamp   time.Time        `json:"timestamp"`
	EpochNumber int64            `json:"epochNumber"`
	NodesInfo   commons.NodesMap `json:"nodesInfo"`
}

func (b *Broker) register() error {
	data := map[string]interface{}{
		"ip":         b.IP,
		"port":       b.Port,
		"readerport": b.ReaderPort,
	}

	jsonData, err := json.Marshal(data)
	if err != nil {
		logger.Error("Error marshalling JSON", zap.Error(err))
		return fmt.Errorf("error encoding JSON: %v", err)
	}

	resp, err := http.Post(fmt.Sprintf("%s%s", b.DirectoryAddr, commons.DIRECTORY_REGISTER_BROKER), "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		logger.Error("Error sending request to directory", zap.Error(err))
		return fmt.Errorf("error sending request to directory: %v, could not start broker", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		defer io.Copy(io.Discard, resp.Body)
		logger.Error("Unexpected status code from directory", zap.Int("statusCode", resp.StatusCode))
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	body, _ := io.ReadAll(resp.Body)

	var respStruct registerResp

	if err := json.Unmarshal(body, &respStruct); err != nil {
		logger.Error("Error unmarshalling JSON", zap.Error(err))
		return fmt.Errorf("error unmarshalling JSON: %v", err)
	}

	//ETime := time.Unix(0, respStruct.Timestamp)
	ETime := respStruct.Timestamp
	ENumber := respStruct.EpochNumber
	b.DirectoryInfo = &respStruct.NodesInfo

	logger.Info("Directory info received", zap.Any("directoryInfo", b.DirectoryInfo))
	b.ID = b.DirectoryInfo.BrokerMap.Version
	b.startTimestamp = ETime
	b.packageCountStart = int(ENumber)
	logger.Info("Broker registered", zap.Int("brokerID", b.ID))
	if ENumber > 0 {
		logger.Info("Broker is recovered", zap.Int("brokerID", b.ID))
	}
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

// isConnRefused returns true if err or any wrapped error is ECONNREFUSED.
func isConnRefused(err error) bool {
	if err == nil {
		return false
	}

	// 1) Unwrap url.Error
	var uerr *url.Error
	if errors.As(err, &uerr) {
		err = uerr.Err
	}

	// 2) Unwrap net.OpError
	var opErr *net.OpError
	if errors.As(err, &opErr) {
		err = opErr.Err
	}

	// 3) Unwrap os.SyscallError
	var sysErr *os.SyscallError
	if errors.As(err, &sysErr) {
		// The inner Err here *should* be a syscall.Errno
		if errno, ok := sysErr.Err.(syscall.Errno); ok && errno == syscall.ECONNREFUSED {
			return true
		}
		logger.Error("connref1", zap.Error(err))
		// If not, fall back to the unwrapped Err
		err = sysErr.Err
	}

	// 4) Direct Errno check
	if errors.Is(err, syscall.ECONNREFUSED) {
		logger.Error("connref2", zap.Error(err))
		return true
	}

	// 5) Last‑ditch: substring match on the error string
	if strings.Contains(err.Error(), "connection refused") {
		logger.Error("connref3", zap.Error(err))
		return true
	}

	return false
}

func (b *Broker) sendPackage(serverURL string, jsonData []byte) func() error {
	return func() error {
		client := makeInstrumentedClient("send-package")

		ctx, cancel := context.WithTimeout(context.Background(), commons.BROKER_PER_ATTEMPT_TIMEOUT)
		defer cancel()
		req, err := http.NewRequestWithContext(ctx, "POST", serverURL, bytes.NewBuffer(jsonData))
		if err != nil {
			panic(err)
		}

		// Set headers
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-NodeID", fmt.Sprint(b.ID))
		req.Header.Set("X-NodeType", commons.GetNodeType(commons.BrokerType))
		if len(jsonData) > 64*1024 { // only for payloads >64 KB
			req.Header.Set("Expect", "100-continue")
		}
		resp, err := client.Do(req)
		if err != nil {
			return fmt.Errorf("error sending request to %s: %s", serverURL, err)
		}
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("error reading response body: %v", err)
		}
		bodyString := string(body)
		//_, _ = io.Copy(io.Discard, resp.Body) // read until EOF, throw away data
		if (resp.StatusCode != http.StatusOK) && (resp.StatusCode != http.StatusAccepted) {
			return fmt.Errorf("unexpected status code: %d for server %s: error: %s", resp.StatusCode, serverURL, bodyString)
		}
		return nil
	}
}

func (b *Broker) protocolDaemon(ctx context.Context, nextRun time.Time) {
	packageCounter := b.packageCountStart
	logger.Warn("Starting protocol daemon...")

	// We want the ticker to have millisecond precision
	//ticker := time.NewTicker(time.Millisecond)
	logger.Warn("Next run will happen at", zap.Time("nextRun", nextRun))

	for {
		// Sleep until the next deadline or until we're asked to stop
		if d := time.Until(nextRun); d > 0 {
			t := time.NewTimer(d)
			select {
			case <-t.C:
				// time to run
			case <-ctx.Done():
				t.Stop()
				logger.Error("Protocol daemon: context cancelled before next run")
				return
			}
		} else {
			// If we’re late (e.g., long previous work or clock jump), run immediately
		}
		nextRun = nextRun.Add(commons.EPOCH_PERIOD)

		packageCounter++

		var pkg *commons.Package
		if b.isTest {
			pkg = b.DummyPackage(packageCounter, 10, 10)
		} else {
			//pkg = b.CreatePackage(packageCounter)
			pkg = b.DummyPackage(packageCounter, 10, 10)
		}

		logger.Warn("Creating package", zap.Int("packageID", pkg.PackageID), zap.Int("transactionsCount", len(pkg.Transactions)))

		jsonData, err := json.Marshal(pkg)
		if err != nil {
			logger.Error("Error marshalling package", zap.Error(err))
			continue
		}

		if err := commons.BroadcastNodesInfo(logger, b.ID, commons.BrokerType, b.DirectoryInfo, sharedTr); err != nil {
			logger.Error("Error broadcasting nodes info", zap.Error(err))
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

				logger.Warn("Sending package to server", zap.Int("serverID", serverID), zap.String("ip", node.IP), zap.Int64("port", node.Port))
				url := fmt.Sprintf("http://%s:%d%s", node.IP, node.Port, commons.SERVER_ADD_PACKAGE)
				err := retry.Do(
					b.sendPackage(url, jsonData),
					retry.Attempts(commons.BROKER_RETRY_ATTEMPTS), // 2 attempts
					retry.Delay(2*time.Millisecond),
					retry.MaxDelay(commons.BROKER_RETRY_MAX_DELAY),
					retry.DelayType(retry.RandomDelay), // small jitter
					retry.OnRetry(func(n uint, err error) {
						logger.Warn("Retrying package send",
							zap.Int("serverID", serverID),
							zap.Int("attempt", int(n)+1),
							zap.Error(err))
					}),
				)

				if err != nil {
					// final failure after retries
					if isConnRefused(err) {
						logger.Error("Server appears down, removing from pool",
							zap.Int("serverID", serverID),
							zap.String("url", url),
							zap.Error(err),
						)
						// deregister server
						go func(m int) {
							b.delete_server(m)
						}(serverID)

					} else {
						logger.Error("Failed to send package after retries",
							zap.Int("serverID", serverID),
							zap.String("url", url),
							zap.Error(err),
						)
					}
				}

			}(serverID, node)
		}

		wg.Wait()
		logger.Warn("Next run will happen at", zap.Time("nextRun", nextRun))
		if packageCounter == 500 {
			logger.Warn("5000 epochs done, ending experiment")
			return
		}
	}

}

func (b *Broker) DummyPackage(packageCounter, nOperation, nTransaction int) *commons.Package {
	var transactions []*commons.Transaction
	for j := 0; j < nTransaction; j++ {
		var operations []*commons.Operation
		for i := 0; i < nOperation; i++ {
			operation := &commons.Operation{
				Key:       fmt.Sprintf("key%d", i),
				Value:     fmt.Sprintf("value%d", i),
				Op:        1,
			}
			operations = append(operations, operation)
		}
		transaction := &commons.Transaction{
			Timestamp: time.Now().UnixNano(),
			Operations: operations,
		}
		transactions = append(transactions, transaction)
	}

	pkg := &commons.Package{
		BrokerID:     b.ID,
		Transactions: transactions,
		PackageID:    packageCounter,
	}

	logger.Info("Dummy package created", zap.Int("packageCounter", packageCounter), zap.Int("transactionsCount", len(pkg.Transactions)))
	return pkg
}

func (b *Broker) CreatePackage(packageCounter int) *commons.Package {
	var transactions []*commons.Transaction
	for b.TransactionQueue.Len() > 0 {
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
		BrokerID:     b.ID,
		Transactions: transactions,
		PackageID:    packageCounter,
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

	var dropRate float64
	flag.Float64Var(&dropRate, "dropRate", 0, "drop rate")
	rand.Seed(time.Now().UnixNano())

	isTest := flag.Bool("test", false, "Run in test mode")
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

	err := os.Mkdir("./logs", 0755)
	if err != nil && !os.IsExist(err) {
		// Only log or handle real errors
		panic(err)
	}

	// Configure Lumberjack for log rotation
	lumberjackLogger := &lumberjack.Logger{
		Filename:   logFile, // Log file path
		MaxSize:    10,      // Max megabytes before log is rotated
		MaxBackups: 5,       // Max old log files to keep
		MaxAge:     28,      // Max days to retain old log files
		Compress:   true,    // Compress old files (.gz)
	}

	// Create Zap core with Lumberjack
	writeSyncer := zapcore.AddSync(lumberjackLogger)
	encoderConfig := zap.NewDevelopmentEncoderConfig()
	encoderConfig.TimeKey = "timestamp"
	encoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder

	core := zapcore.NewCore(
		zapcore.NewJSONEncoder(encoderConfig),
		writeSyncer,
		zapcore.WarnLevel,
	)

	logger = zap.New(core)
	logger = logger.With(zap.String("service", fmt.Sprintf("broker %s", brokerIP)))

	defer logger.Sync()

	broker, err := NewBroker(0, brokerIP, brokerPort, directoryIP, directoryPort, *isTest, dropRate)
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
	logger.Warn("Broker running on http://localhost:%d", zap.Int("port", broker.Port))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigs := make(chan os.Signal, 4)
	signal.Notify(sigs, os.Interrupt, syscall.SIGTERM, syscall.SIGHUP, syscall.SIGQUIT)
	go func() {
		for s := range sigs {
			logger.Warn("received signal", zap.String("sig", s.String()))
			cancel()
			return
		}
	}()

	go broker.protocolDaemon(ctx, broker.startTimestamp)

	// Block until context is cancelled
	<-ctx.Done()

	// Graceful shutdown
	logger.Warn("Shutting down broker...")
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	if err := broker.httpServer.Shutdown(shutdownCtx); err != nil {
		logger.Error("HTTP shutdown error", zap.Error(err))
	}
	logger.Warn("Broker shutdown complete.")

}
