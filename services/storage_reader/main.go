package storage_reader

import (
	"context"
	"flag"
	"fmt"
	"lockfreemachine/pkg/commons"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"time"

	"encoding/json"

	"github.com/gorilla/mux"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
)

// zap logger instance to log messages
var logger *zap.Logger

// Server struct represents the server instance.
type ReadingServer struct {
	ip       string // IP address of the server
	port     int    // Port number of the server
	serverID int    // Server ID
}

func (s *ReadingServer) readPackage(brokerID, packageID int) (*commons.Package, error) {
	// I'm also returning an error code to check the type of error in the upstream function.
	// Write to file
	jsonData, err := os.ReadFile(fmt.Sprintf("./packages_%d/pkg_%d_%d.json", s.serverID, brokerID, packageID))
	if err != nil {
		logger.Error("Failed to read file", zap.String("filePath", fmt.Sprintf("./packages_%d/pkg_%d_%d.json", s.serverID, brokerID, packageID)), zap.Error(err))
		return nil, fmt.Errorf("error reading file: %s", err)
	}

	// Encode package to JSON
	pkg := &commons.Package{}
	if err := json.Unmarshal(jsonData, pkg); err != nil {
		logger.Error("Failed to decode JSON", zap.String("filePath", fmt.Sprintf("./packages_%d/pkg_%d_%d.json", s.serverID, brokerID, packageID)), zap.Error(err))
		return nil, fmt.Errorf("error decoding JSON: %s", err)
	}

	logger.Info("Package read from file", zap.String("filePath", fmt.Sprintf("./packages_%d/pkg_%d_%d.json", s.serverID, brokerID, packageID)), zap.Any("package", pkg))

	return pkg, nil
}

func (s *ReadingServer) handleReadPackageStorage(w http.ResponseWriter, r *http.Request) {
	params := r.URL.Query()
	// both brokerID and packageID is sent by requesting server
	brokerID := params.Get("brokerID")
	packageID := params.Get("packageID")

	logger.Info("Received read package (storage) request", zap.String("brokerID", brokerID))
	brokerIDInt, err := strconv.Atoi(brokerID)
	if err != nil {
		logger.Error("error converting brokerID to int", zap.String("brokerID", brokerID), zap.Error(err))
		http.Error(w, "Invalid brokerID", http.StatusBadRequest)
		return
	}
	logger.Info("Received read package (storage) request", zap.String("brokerID", brokerID))
	packageIDInt, err := strconv.Atoi(packageID)
	if err != nil {
		logger.Error("error converting packageID to int", zap.String("packageID", packageID), zap.Error(err))
		http.Error(w, "Invalid packageID", http.StatusBadRequest)
		return
	}

	// I need to check now if I can actually find the package file
	pkg, err := s.readPackage(brokerIDInt, packageIDInt)
	if err != nil {
		logger.Error("error reading package", zap.String("filePath", fmt.Sprintf("./packages_%d/pkg_%d_%d.json", s.serverID, brokerIDInt, packageIDInt)), zap.Error(err))
		http.Error(w, "Error reading package", http.StatusBadRequest)
		return
	}

	if err := json.NewEncoder(w).Encode(pkg); err != nil {
		http.Error(w, "Failed to encode response", http.StatusInternalServerError)
	}

	// Perform necessary operations with packageID and brokerID
	//w.WriteHeader(http.StatusOK)
}

func (s *ReadingServer) handleRegisterID(w http.ResponseWriter, r *http.Request) {
	params := r.URL.Query()
	// both brokerID and packageID is sent by requesting server
	ServerID := params.Get("ServerID")

	logger.Info("Received register ID request", zap.String("ServerID", ServerID))
	ServerIDInt, err := strconv.Atoi(ServerID)
	if err != nil {
		logger.Error("error converting ServerID to int", zap.String("ServerID", ServerID), zap.Error(err))
		http.Error(w, "Invalid ServerID", http.StatusBadRequest)
		return
	}

	s.serverID = ServerIDInt

	// Perform necessary operations with packageID and brokerID
	w.WriteHeader(http.StatusOK)
}

func (s *ReadingServer) setupServer() (*http.Server, error) {
	r := mux.NewRouter()
	r.HandleFunc(commons.READING_SERVER_READ_STORAGE, s.handleReadPackageStorage).Methods("GET")
	r.HandleFunc(commons.READING_SERVER_REGISTER_ID, s.handleRegisterID).Methods("GET")

	// Create HTTP request
	return &http.Server{
		Addr:    fmt.Sprintf(":%d", s.port),
		Handler: r,
	}, nil
}

func Main() {

	// Parse command line arguments
	var directoryIP string
	var directoryPort int

	flag.StringVar(&directoryIP, "directoryIP", "", "IP of directory")
	flag.IntVar(&directoryPort, "directoryPort", 0, "Port of directory")

	var serverIP string
	var readerPort int

	flag.StringVar(&serverIP, "serverIP", "", "IP of current Server")
	flag.IntVar(&readerPort, "readerPort", 0, "Port of Server")

	var startTimestamp int64
	flag.Int64Var(&startTimestamp, "startTimestamp", 0, "Start timestamp")

	var logFile string
	flag.StringVar(&logFile, "logFile", "./logs/broker.log", "Path to the log file")

	flag.Parse()

	if directoryIP == "" {
		fmt.Print("directoryIP is required")
		return
	}
	if directoryPort == 0 {
		fmt.Print("directoryPort is required")
		return
	}

	if serverIP == "" {
		fmt.Printf("serverIP is required")
		return
	}

	if readerPort == 0 {
		fmt.Printf("serverPort is required")
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
		zapcore.InfoLevel,
	)

	logger = zap.New(core)
	logger = logger.With(zap.String("service", fmt.Sprintf("server %s", serverIP)))

	defer logger.Sync()

	server := &ReadingServer{
		ip:   serverIP,
		port: readerPort,
	}

	httpServer, err := server.setupServer()
	if err != nil {
		logger.Error("Error setting up server", zap.Error(err))
	}

	// Graceful shutdown handling
	go func() {
		if err := httpServer.ListenAndServe(); err != http.ErrServerClosed {
			logger.Error("Server Error", zap.Error(err))
		}
	}()

	logger.Info("Storage Server running on http://localhost:%d", zap.Int("port", server.port))

	var wg sync.WaitGroup
	wg.Add(1)

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt)
	<-quit

	logger.Info("\nShutting down Storage server...")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	httpServer.Shutdown(ctx)
	logger.Info("Storage Server stopped.")
	// close(server.writeChan)
}
