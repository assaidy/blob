package blob

// TODO: add `Client` struct interface
// TODO: add login and accounts with admin page
// TODO: add api doc

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/logger"
	"github.com/gotd/contrib/http_range"
	"github.com/oklog/ulid/v2"
)

// bodyLimit indicates the max chunk size in a request.
// -1 will decline any limit.
func NewServer(bodyLimit DataUnite, secreteKey, rootDir, metadataDir string) *Server {
	// TODO: make server config param, instead of all of these params
	// it also has more flexibility
	if err := os.MkdirAll(rootDir, os.ModePerm); err != nil {
		panic(fmt.Sprintf("error creating rootPath: %+v", err))
	}
	if err := os.MkdirAll(metadataDir, os.ModePerm); err != nil {
		panic(fmt.Sprintf("error creating metadataPath: %+v", err))
	}
	server := &Server{
		rootDir:   rootDir,
		secretKey: secreteKey,
		router:    fiber.New(fiber.Config{BodyLimit: int(bodyLimit)}),
		metadata:  NewMetadataStorage(metadataDir),
	}
	server.regesterRoutes()
	server.router.Use(logger.New())
	return server
}

func (me *Server) Listen(addr string) error {
	me.regesterRoutes()
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

	closed.Post("/buckets/:bucket_id/blobs", me.handleCreateBlob)          // used befor uploading a blob, to create it or erase it if exist
	closed.Put("/buckets/:bucket_id/blobs/:blob_id", me.handleWriteToBlob) // upload chunks and write it to a blob
	closed.Get("/buckets/:bucket_id/blobs", me.handleGetAllBlobs)
	closed.Delete("/buckets/:bucket_id/blobs/:blob_id", me.handleDeleteBlob)
	closed.Get("/buckets/:bucket_id/blobs/:blob_id", me.handleGetBlob)

	closed.Post("/access", me.handleCreateAccess)
	open.Get("/access/:key", me.handleAccess)
	closed.Delete("/access/:key", me.handleDeleteAccess)
}

func (me *Server) handleCreateBucket(c *fiber.Ctx) error {
	bucketId := strings.TrimSpace(c.Query("bucket_id"))

	if bucketId == "" {
		return fiber.ErrBadRequest
	}

	if exists, err := me.metadata.checkIfBucketExists(bucketId); err != nil {
		return fiber.ErrInternalServerError
	} else if exists {
		return fiber.ErrConflict
	}

	bucket := &Bucket{
		Id:        bucketId,
		CreatedAt: time.Now().UTC(),
		Blobs:     []*Blob{},
	}

	if err := me.metadata.createBucket(bucket); err != nil {
		return fiber.ErrInternalServerError
	}

	path := filepath.Join(me.rootDir, bucket.Id)
	if err := os.Mkdir(path, os.ModePerm); err != nil {
		return fiber.ErrInternalServerError
	}

	return c.Status(fiber.StatusCreated).JSON(bucket)
}

func (me *Server) handleGetAllBuckets(c *fiber.Ctx) error {
	buckets, err := me.metadata.getAllBuckets()
	if err != nil {
		return fiber.ErrInternalServerError
	}
	return c.Status(fiber.StatusOK).JSON(buckets)
}

func (me *Server) handleGetBucket(c *fiber.Ctx) error {
	id := c.Query("bucket_id")

	if exists, err := me.metadata.checkIfBucketExists(id); err != nil {
		return fiber.ErrInternalServerError
	} else if !exists {
		return fiber.ErrNotFound
	}

	bucket, err := me.metadata.getBucket(id)
	if err != nil {
		return fiber.ErrInternalServerError
	}

	return c.Status(fiber.StatusOK).JSON(bucket)
}

func (me *Server) handleDeleteBucket(c *fiber.Ctx) error {
	bucketId := strings.TrimSpace(c.Params("bucket_id"))

	if exists, err := me.metadata.checkIfBucketExists(bucketId); err != nil {
		return fiber.ErrInternalServerError
	} else if !exists {
		return fiber.ErrNotFound
	}

	if err := me.metadata.deleteBucket(bucketId); err != nil {
		return fiber.ErrInternalServerError
	}

	path := filepath.Join(me.rootDir, bucketId)
	if err := os.RemoveAll(path); err != nil {
		return fiber.ErrInternalServerError
	}

	return c.SendStatus(fiber.StatusNoContent)
}

func (me *Server) handleCreateBlob(c *fiber.Ctx) error {
	var (
		bucketId = strings.TrimSpace(c.Params("bucket_id"))
		blobId   = strings.TrimSpace(c.Query("blob_id"))
	)
	if blobId == "" {
		return fiber.ErrBadRequest
	}

	if exists, err := me.metadata.checkIfBucketExists(bucketId); err != nil {
		return fiber.ErrInternalServerError
	} else if !exists {
		return fiber.ErrNotFound
	}

	if exists, err := me.metadata.checkIfBlobExists(bucketId, blobId); err != nil {
		return fiber.ErrInternalServerError
	} else if exists {
		return fiber.ErrConflict
	}

	blob := &Blob{
		Id:        blobId,
		BucketId:  bucketId,
		Size:      0,
		CreatedAt: time.Now().UTC(),
	}

	if err := me.metadata.createBlob(blob); err != nil {
		return fiber.ErrInternalServerError
	}

	return c.SendStatus(fiber.StatusCreated)
}

func (me *Server) handleWriteToBlob(c *fiber.Ctx) error {
	var (
		bucketId = strings.TrimSpace(c.Params("bucket_id"))
		blobId   = strings.TrimSpace(c.Params("blob_id"))
	)

	if exists, err := me.metadata.checkIfBlobExists(bucketId, blobId); err != nil {
		return fiber.ErrInternalServerError
	} else if !exists {
		return fiber.ErrNotFound
	}

	path := filepath.Join(me.rootDir, bucketId, blobId)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, os.ModePerm)
	if err != nil {
		return fiber.ErrInternalServerError
	}
	defer file.Close()

	chunk := c.Body()
	if _, err := file.Write(chunk); err != nil {
		return fiber.ErrInternalServerError
	}

	// OPTIM: might replace with updateBlob() in future
	if err := me.metadata.incrementBlobSize(bucketId, blobId, len(chunk)); err != nil {
		return fiber.ErrInternalServerError
	}

	return c.SendStatus(fiber.StatusOK)
}

func (me *Server) handleGetAllBlobs(c *fiber.Ctx) error {
	bucketId := strings.TrimSpace(c.Params("bucket_id"))

	if exists, err := me.metadata.checkIfBucketExists(bucketId); err != nil {
		return fiber.ErrInternalServerError
	} else if !exists {
		return fiber.ErrNotFound
	}

	blobs, err := me.metadata.getBlobsPerBucket(bucketId)
	if err != nil {
		return fiber.ErrInternalServerError
	}

	return c.Status(fiber.StatusOK).JSON(blobs)
}

func (me *Server) handleGetBlob(c *fiber.Ctx) error {
	var (
		bucketId = strings.TrimSpace(c.Params("bucket_id"))
		blobId   = strings.TrimSpace(c.Params("blob_id"))
	)

	if exists, err := me.metadata.checkIfBlobExists(bucketId, blobId); err != nil {
		return fiber.ErrInternalServerError
	} else if !exists {
		return fiber.ErrNotFound
	}

	blob, err := me.metadata.getBlob(bucketId, blobId)
	if err != nil {
		return fiber.ErrInternalServerError
	}

	return c.Status(fiber.StatusOK).JSON(blob)
}

func (me *Server) handleDeleteBlob(c *fiber.Ctx) error {
	var (
		bucketId = strings.TrimSpace(c.Params("bucket_id"))
		blobId   = strings.TrimSpace(c.Params("blob_id"))
	)

	if exists, err := me.metadata.checkIfBlobExists(bucketId, blobId); err != nil {
		return fiber.ErrInternalServerError
	} else if !exists {
		return fiber.ErrNotFound
	}

	if err := me.metadata.deleteBlob(bucketId, blobId); err != nil {
		return fiber.ErrInternalServerError
	}

	path := filepath.Join(me.rootDir, bucketId, blobId)
	if err := os.Remove(path); err != nil {
		return fiber.ErrInternalServerError
	}

	return c.SendStatus(fiber.StatusNoContent)
}

func (me *Server) handleCreateAccess(c *fiber.Ctx) error {
	var (
		bucketId = strings.TrimSpace(c.Query("bucket_id"))
		blobId   = strings.TrimSpace(c.Query("blob_id"))
	)

	if bucketId == "" || blobId == "" {
		return fiber.ErrBadRequest
	}

	if exists, err := me.metadata.checkIfBlobExists(bucketId, blobId); err != nil {
		return fiber.ErrInternalServerError
	} else if !exists {
		return fiber.ErrNotFound
	}

	access := &Access{
		Key:       ulid.Make().String(),
		BucketId:  bucketId,
		BlobId:    blobId,
		CreatedAt: time.Now().UTC(),
	}

	if err := me.metadata.createAccess(access); err != nil {
		return fiber.ErrInternalServerError
	}

	return c.Status(fiber.StatusCreated).JSON(access)
}

func (me *Server) handleAccess(c *fiber.Ctx) error {
	key := strings.TrimSpace(c.Params("key"))

	// OPTIM: these two queries might be summarized into
	// one query with `blob, ok, err := me.metadata.getBlobInAccess(key)`
	if exists, err := me.metadata.checkIfAccessExists(key); err != nil {
		return fiber.ErrInternalServerError
	} else if !exists {
		return fiber.ErrNotFound
	}

	blob, err := me.metadata.getBlobOfAccess(key)
	if err != nil {
		return fiber.ErrInternalServerError
	}

	requestRange := strings.TrimSpace(c.Get("Range"))
	if requestRange == "" { // no range specified -> send the whole file
		path := filepath.Join(me.rootDir, blob.BucketId, blob.Id)

		fileBytes, err := os.ReadFile(path)
		if err != nil {
			return fiber.ErrInternalServerError
		}

		c.Set(fiber.HeaderContentLength, fmt.Sprintf("%d", blob.Size))
		c.Set(fiber.HeaderContentType, fiber.MIMEOctetStream)

		return c.Status(fiber.StatusOK).Send(fileBytes)
	}

	// handle ranged request
	ranges, err := http_range.ParseRange(requestRange, int64(blob.Size))
	if err != nil {
		return c.Status(fiber.StatusBadRequest).SendString("invalid Range header")
	}
	if len(ranges) > 1 {
		return c.Status(fiber.StatusBadRequest).SendString("can only accept a single ragne")
	}
	r := ranges[0]

	path := filepath.Join(me.rootDir, blob.BucketId, blob.Id)
	file, err := os.Open(path)
	if err != nil {
		return fiber.ErrInternalServerError
	}
	defer file.Close()

	data := make([]byte, r.Length)
	_, err = file.ReadAt(data, r.Start)
	if err != nil {
		return fiber.ErrInternalServerError
	}

	c.Set(fiber.HeaderContentRange, r.ContentRange(int64(blob.Size)))
	c.Set(fiber.HeaderAcceptRanges, "bytes")
	c.Set(fiber.HeaderContentLength, fmt.Sprintf("%d", r.Length))
	c.Set(fiber.HeaderContentType, fiber.MIMEOctetStream)

	return c.Status(fiber.StatusPartialContent).Send(data)
}

func (me *Server) handleDeleteAccess(c *fiber.Ctx) error {
	key := strings.TrimSpace(c.Params("key"))

	if exists, err := me.metadata.checkIfAccessExists(key); err != nil {
		return fiber.ErrInternalServerError
	} else if exists {
		return fiber.ErrConflict
	}

	if err := me.metadata.deleteAccess(key); err != nil {
		return fiber.ErrInternalServerError
	}

	return c.SendStatus(fiber.StatusNoContent)
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
