package blob

// TODO: add `Client` struct interface
// TODO: add login and accounts with admin page
// TODO: add api doc

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/logger"
)

type ServerConfig struct {
	MaxChunkSize DataUnite
	SecretKey    string
	RootDir      string
	MetadataDir  string
}

func NewServer(config ServerConfig) *Server {
	if err := os.MkdirAll(config.RootDir, os.ModePerm); err != nil {
		panic(fmt.Sprintf("error creating root dir: %+v", err))
	}
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

func (me *Server) Listen(addr string) error {
	return me.router.Listen(addr)
}

func (me *Server) regesterRoutes() {
	var (
		closed = me.router.Group("/", me.mwWithSecreteKey)
		open   = me.router.Group("/")
	)

	closed.Post("/buckets", me.handleCreateBucket)
	closed.Get("/buckets", me.handleGetAllBuckets)
	closed.Get("/buckets/:bucket_id", me.handleGetBucket)
	closed.Delete("/buckets/:bucket_id", me.handleDeleteBucket)

	closed.Post("/buckets/:bucket_id/blobs", me.handleCreateBlob)
	closed.Put("/buckets/:bucket_id/blobs/:blob_id", me.handleWriteToBlob)
	closed.Get("/buckets/:bucket_id/blobs", me.handleGetAllBlobs)
	closed.Delete("/buckets/:bucket_id/blobs/:blob_id", me.handleDeleteBlob)
	closed.Get("/buckets/:bucket_id/blobs/:blob_id", me.handleGetBlob)

	closed.Post("/access", me.handleCreateAccess)
	open.Get("/access/:key", me.handleAccess)
	closed.Delete("/access/:key", me.handleDeleteAccess)
}

func (me *Server) mwWithSecreteKey(c *fiber.Ctx) error {
	bearer := c.Get(fiber.HeaderAuthorization)
	if bearer == "" || !(strings.HasPrefix(bearer, "Bearer ")) {
		return fiber.ErrUnauthorized
	}
	key := strings.TrimPrefix(bearer, "Bearer ")
	if key != me.secretKey {
		return fiber.ErrUnauthorized
	}
	return c.Next()
}

func errorHandler(c *fiber.Ctx, err error) error {
	var apiE *ApiError
	if errors.As(err, &apiE) {
		c.Set(fiber.HeaderContentType, fiber.MIMEApplicationJSON)
		return c.Status(apiE.Code).JSON(apiE)
	}
	code := fiber.StatusInternalServerError
	var fiberE *fiber.Error
	if errors.As(err, &fiberE) {
		code = fiberE.Code
	}
	c.Set(fiber.HeaderContentType, fiber.MIMETextPlainCharsetUTF8)
	return c.Status(code).SendString(err.Error())
}
