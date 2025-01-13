package blob

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/assaidy/blob/utils"
	"github.com/gofiber/fiber/v2"
	"github.com/gotd/contrib/http_range"
	"github.com/oklog/ulid/v2"
)

func (me *Server) handleCreateBucket(c *fiber.Ctx) error {
	bucketId := strings.TrimSpace(c.Query("bucket_id"))
	if bucketId == "" {
		return utils.BadRequestError("invalid value for query param bucket_id")
	}

	if exists, err := me.metadata.checkIfBucketExists(bucketId); err != nil {
		return utils.InternalServerError(err)
	} else if exists {
		return utils.ConflictError("bucket already exists")
	}

	bucket := &Bucket{
		Id:        bucketId,
		CreatedAt: time.Now().UTC(),
		Blobs:     []*Blob{},
	}

	if err := me.metadata.createBucket(bucket); err != nil {
		return utils.InternalServerError(err)
	}

	path := filepath.Join(me.rootDir, bucket.Id)
	if err := os.Mkdir(path, os.ModePerm); err != nil {
		return utils.InternalServerError(err)
	}

	return c.Status(fiber.StatusCreated).JSON(bucket)
}

func (me *Server) handleGetAllBuckets(c *fiber.Ctx) error {
	buckets, err := me.metadata.getAllBuckets()
	if err != nil {
		return utils.InternalServerError(err)
	}
	return c.Status(fiber.StatusOK).JSON(buckets)
}

func (me *Server) handleGetBucket(c *fiber.Ctx) error {
	bucketId := strings.TrimSpace(c.Query("bucket_id"))
	if bucketId == "" {
		return utils.BadRequestError("invalid value for query param bucket_id")
	}

	if exists, err := me.metadata.checkIfBucketExists(bucketId); err != nil {
		return utils.InternalServerError(err)
	} else if !exists {
		return utils.NotFoundError("bucket not found")
	}

	bucket, err := me.metadata.getBucket(bucketId)
	if err != nil {
		return utils.InternalServerError(err)
	}

	return c.Status(fiber.StatusOK).JSON(bucket)
}

func (me *Server) handleDeleteBucket(c *fiber.Ctx) error {
	bucketId := strings.TrimSpace(c.Params("bucket_id"))
	if bucketId == "" {
		return utils.BadRequestError("invalid value for query param bucket_id")
	}

	if exists, err := me.metadata.checkIfBucketExists(bucketId); err != nil {
		return utils.InternalServerError(err)
	} else if !exists {
		return utils.NotFoundError("but not found")
	}

	if err := me.metadata.deleteBucket(bucketId); err != nil {
		return utils.InternalServerError(err)
	}

	path := filepath.Join(me.rootDir, bucketId)
	if err := os.RemoveAll(path); err != nil {
		return utils.InternalServerError(err)
	}

	return c.SendStatus(fiber.StatusNoContent)
}

func (me *Server) handleCreateBlob(c *fiber.Ctx) error {
	var (
		bucketId = strings.TrimSpace(c.Params("bucket_id"))
		blobId   = strings.TrimSpace(c.Query("blob_id"))
	)
	if bucketId == "" {
		return utils.BadRequestError("invalid value for path param bucket_id")
	}
	if blobId == "" {
		return utils.BadRequestError("invalid value for query param blob_id")
	}

	if exists, err := me.metadata.checkIfBucketExists(bucketId); err != nil {
		return utils.InternalServerError(err)
	} else if !exists {
		return utils.NotFoundError("bucket not found")
	}

	if exists, err := me.metadata.checkIfBlobExists(bucketId, blobId); err != nil {
		return utils.InternalServerError(err)
	} else if exists {
		return utils.ConflictError("blob already exists")
	}

	blob := &Blob{
		Id:        blobId,
		BucketId:  bucketId,
		Size:      0,
		CreatedAt: time.Now().UTC(),
	}

	if err := me.metadata.createBlob(blob); err != nil {
		return utils.InternalServerError(err)
	}

	return c.SendStatus(fiber.StatusCreated)
}

func (me *Server) handleWriteToBlob(c *fiber.Ctx) error {
	var (
		bucketId = strings.TrimSpace(c.Params("bucket_id"))
		blobId   = strings.TrimSpace(c.Params("blob_id"))
	)
	if bucketId == "" {
		return utils.BadRequestError("invalid value for path param bucket_id")
	}
	if blobId == "" {
		return utils.BadRequestError("invalid value for path param blob_id")
	}

	if exists, err := me.metadata.checkIfBlobExists(bucketId, blobId); err != nil {
		return utils.InternalServerError(err)
	} else if !exists {
		return utils.NotFoundError("blob not found")
	}

	path := filepath.Join(me.rootDir, bucketId, blobId)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, os.ModePerm)
	if err != nil {
		return utils.InternalServerError(err)
	}
	defer file.Close()

	chunk := c.Body()
	if _, err := file.Write(chunk); err != nil {
		return utils.InternalServerError(err)
	}

	// OPTIM: might replace with updateBlob() in future
	if err := me.metadata.incrementBlobSize(bucketId, blobId, len(chunk)); err != nil {
		return utils.InternalServerError(err)
	}

	return c.SendStatus(fiber.StatusOK)
}

func (me *Server) handleGetAllBlobs(c *fiber.Ctx) error {
	bucketId := strings.TrimSpace(c.Params("bucket_id"))
	if bucketId == "" {
		return utils.BadRequestError("invalid value for path param bucket_id")
	}

	if exists, err := me.metadata.checkIfBucketExists(bucketId); err != nil {
		return utils.InternalServerError(err)
	} else if !exists {
		return utils.NotFoundError("bucket not found")
	}

	blobs, err := me.metadata.getBlobsPerBucket(bucketId)
	if err != nil {
		return utils.InternalServerError(err)
	}

	return c.Status(fiber.StatusOK).JSON(blobs)
}

func (me *Server) handleGetBlob(c *fiber.Ctx) error {
	var (
		bucketId = strings.TrimSpace(c.Params("bucket_id"))
		blobId   = strings.TrimSpace(c.Params("blob_id"))
	)
	if bucketId == "" {
		return utils.BadRequestError("invalid value for path param bucket_id")
	}
	if blobId == "" {
		return utils.BadRequestError("invalid value for path param blob_id")
	}

	if exists, err := me.metadata.checkIfBlobExists(bucketId, blobId); err != nil {
		return utils.InternalServerError(err)
	} else if !exists {
		return utils.NotFoundError("blob not found")
	}

	blob, err := me.metadata.getBlob(bucketId, blobId)
	if err != nil {
		return utils.InternalServerError(err)
	}

	return c.Status(fiber.StatusOK).JSON(blob)
}

func (me *Server) handleDeleteBlob(c *fiber.Ctx) error {
	var (
		bucketId = strings.TrimSpace(c.Params("bucket_id"))
		blobId   = strings.TrimSpace(c.Params("blob_id"))
	)
	if bucketId == "" {
		return utils.BadRequestError("invalid value for path param bucket_id")
	}
	if blobId == "" {
		return utils.BadRequestError("invalid value for path param blob_id")
	}

	if exists, err := me.metadata.checkIfBlobExists(bucketId, blobId); err != nil {
		return utils.InternalServerError(err)
	} else if !exists {
		return utils.NotFoundError("blob not found")
	}

	if err := me.metadata.deleteBlob(bucketId, blobId); err != nil {
		return utils.InternalServerError(err)
	}

	path := filepath.Join(me.rootDir, bucketId, blobId)
	if err := os.Remove(path); err != nil {
		return utils.InternalServerError(err)
	}

	return c.SendStatus(fiber.StatusNoContent)
}

func (me *Server) handleCreateAccess(c *fiber.Ctx) error {
	var (
		bucketId = strings.TrimSpace(c.Query("bucket_id"))
		blobId   = strings.TrimSpace(c.Query("blob_id"))
	)
	if bucketId == "" {
		return utils.BadRequestError("invalid value for query param bucket_id")
	}
	if blobId == "" {
		return utils.BadRequestError("invalid value for query param blob_id")
	}

	if exists, err := me.metadata.checkIfBlobExists(bucketId, blobId); err != nil {
		return utils.InternalServerError(err)
	} else if !exists {
		return utils.NotFoundError("blob not found")
	}

	access := &Access{
		Key:       ulid.Make().String(),
		BucketId:  bucketId,
		BlobId:    blobId,
		CreatedAt: time.Now().UTC(),
	}

	if err := me.metadata.createAccess(access); err != nil {
		return utils.InternalServerError(err)
	}

	return c.Status(fiber.StatusCreated).JSON(access)
}

func (me *Server) handleDownloadWithAccess(c *fiber.Ctx) error {
	key := strings.TrimSpace(c.Params("key"))
	if key == "" {
		return utils.BadRequestError("invalid value for path param key")
	}

	// OPTIM: these two queries might be summarized into
	// one query with `blob, ok, err := me.metadata.getBlobInAccess(key)`
	if exists, err := me.metadata.checkIfAccessExists(key); err != nil {
		return utils.InternalServerError(err)
	} else if !exists {
		return utils.NotFoundError("access not found")
	}

	blob, err := me.metadata.getBlobOfAccess(key)
	if err != nil {
		return utils.InternalServerError(err)
	}

	requestRange := strings.TrimSpace(c.Get("Range"))
	if requestRange == "" { // no range specified -> send the whole file
		path := filepath.Join(me.rootDir, blob.BucketId, blob.Id)

		fileBytes, err := os.ReadFile(path)
		if err != nil {
			return utils.InternalServerError(err)
		}

		c.Set(fiber.HeaderContentLength, fmt.Sprintf("%d", blob.Size))
		c.Set(fiber.HeaderContentType, fiber.MIMEOctetStream)

		return c.Status(fiber.StatusOK).Send(fileBytes)
	}

	// handle ranged request
	ranges, err := http_range.ParseRange(requestRange, int64(blob.Size))
	if err != nil {
		return utils.BadRequestError("invalid range header")
	}
	if len(ranges) > 1 {
		return utils.BadRequestError("can only accept a single ragne")
	}
	r := ranges[0]

	if r.Length > int64(me.maxChunkSize) {
		return utils.BadRequestError("range length exceeds server's max chunk size")
	}

	path := filepath.Join(me.rootDir, blob.BucketId, blob.Id)
	file, err := os.Open(path)
	if err != nil {
		return utils.InternalServerError(err)
	}
	defer file.Close()

	data := make([]byte, r.Length)
	_, err = file.ReadAt(data, r.Start)
	if err != nil {
		return utils.InternalServerError(err)
	}

	c.Set(fiber.HeaderContentRange, r.ContentRange(int64(blob.Size)))
	c.Set(fiber.HeaderAcceptRanges, "bytes")
	c.Set(fiber.HeaderContentLength, fmt.Sprintf("%d", r.Length))
	c.Set(fiber.HeaderContentType, fiber.MIMEOctetStream)

	return c.Status(fiber.StatusPartialContent).Send(data)
}

func (me *Server) handleDeleteAccess(c *fiber.Ctx) error {
	key := strings.TrimSpace(c.Params("key"))
	if key == "" {
		return utils.BadRequestError("invalid value for path param key")
	}

	if exists, err := me.metadata.checkIfAccessExists(key); err != nil {
		return utils.InternalServerError(err)
	} else if !exists {
		return utils.NotFoundError("access not found")
	}

	if err := me.metadata.deleteAccess(key); err != nil {
		return utils.InternalServerError(err)
	}

	return c.SendStatus(fiber.StatusNoContent)
}
