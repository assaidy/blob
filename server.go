package blob

// TODO: add api doc

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/logger"
)

// ServerConfig holds the configuration for initializing a Server instance.
type ServerConfig struct {
	MaxChunkSize DataUnite // Maximum size of data chunks in bytes (in upload and download).
	SecretKey    string    // Secret key used for authentication.
	RootDir      string    // Root directory for storing data.
	MetadataDir  string    // Directory for storing metadata.
}

// NewServer initializes a new Server instance based on the provided configuration.
func NewServer(config ServerConfig) *Server {
	// Ensure the root directory exists; create it if necessary.
	if err := os.MkdirAll(config.RootDir, os.ModePerm); err != nil {
		panic(fmt.Sprintf("error creating root dir: %+v", err))
	}
	// Ensure the metadata directory exists; create it if necessary.
	if err := os.MkdirAll(config.MetadataDir, os.ModePerm); err != nil {
		panic(fmt.Sprintf("error creating metadata path: %+v", err))
	}

	server := &Server{
		rootDir:      config.RootDir,
		secretKey:    config.SecretKey,
		maxChunkSize: config.MaxChunkSize,
		metadata:     NewMetadataStorage(config.MetadataDir),
		router: fiber.New(fiber.Config{
			BodyLimit:    int(config.MaxChunkSize),
			ErrorHandler: errorHandler,
		}),
	}
	server.regesterRoutes()
	server.router.Use(logger.New())

	return server
}

// Listen starts the server and listens on the specified address.
func (me *Server) Listen(addr string) error {
	return me.router.Listen(addr)
}

// regesterRoutes defines all API routes for the server.
func (me *Server) regesterRoutes() {
	var (
		closed = me.router.Group("/", me.mwWithSecreteKey)
		open   = me.router.Group("/")
	)

	// Bucket-related routes.
	closed.Post("/buckets", me.handleCreateBucket)
	closed.Get("/buckets", me.handleGetAllBuckets)
	closed.Get("/buckets/:bucket_id", me.handleGetBucket)
	closed.Delete("/buckets/:bucket_id", me.handleDeleteBucket)

	// Blob-related routes.
	closed.Post("/buckets/:bucket_id/blobs", me.handleCreateBlob)
	closed.Put("/buckets/:bucket_id/blobs/:blob_id", me.handleWriteToBlob)
	closed.Get("/buckets/:bucket_id/blobs", me.handleGetAllBlobs)
	closed.Delete("/buckets/:bucket_id/blobs/:blob_id", me.handleDeleteBlob)
	closed.Get("/buckets/:bucket_id/blobs/:blob_id", me.handleGetBlob)

	// Access key management routes.
	closed.Post("/access", me.handleCreateAccess)
	open.Get("/access/:key", me.handleDownloadWithAccess)
	closed.Delete("/access/:key", me.handleDeleteAccess)
}

// mwWithSecreteKey is middleware that validates the secret key for authenticated routes.
func (me *Server) mwWithSecreteKey(c *fiber.Ctx) error {
	key := strings.TrimSpace(c.Get("Secret-Key"))
	if key == "" || key != me.secretKey {
		return UnauthorizedError()
	}
	return c.Next()
}

// errorHandler is a custom error handler that formats errors for the API response.
func errorHandler(c *fiber.Ctx, err error) error {
	var apiE *APIError
	// Handle API-specific errors and respond with JSON.
	if errors.As(err, &apiE) {
		c.Set(fiber.HeaderContentType, fiber.MIMEApplicationJSON)
		return c.Status(apiE.Code).JSON(apiE)
	}
	// Default to an internal server error.
	code := fiber.StatusInternalServerError
	var fiberE *fiber.Error
	// Handle Fiber-specific errors and extract the status code.
	if errors.As(err, &fiberE) {
		code = fiberE.Code
	}
	// Respond with a plain text error message.
	c.Set(fiber.HeaderContentType, fiber.MIMETextPlainCharsetUTF8)
	return c.Status(code).SendString(err.Error())
}
