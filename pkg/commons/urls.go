package commons

const (
	// Directory endpoints
	DIRECTORY_REGISTER_BROKER = "/registerBroker"
	DIRECTORY_REGISTER_SERVER = "/registerServer"
	DIRECTORY_REMOVE_SERVER   = "/deRegisterServer"

	// Broker endpoints
	BROKER_TRANSACTION      = "/addTransaction"
	BROKER_UPDATE_DIRECTORY = "/updateDirectory"

	// Server endpoints
	SERVER_UPDATE_DIRECTORY = "/updateDirectory"
	SERVER_ADD_PACKAGE      = "/addPackage"
	SERVER_REQUEST_PACKAGE  = "/requestPackage"
	SERVER_READ_STORAGE     = "/readPackageStorage"
	SERVER_IGNORE_BROKER    = "/ignoreBroker"
	SERVER_BROKER_OK        = "/brokerOk"

	// Reader endpoints
	READING_SERVER_READ_STORAGE = "/readPackageStorage"
	READING_SERVER_REGISTER_ID  = "/registerID"
)
