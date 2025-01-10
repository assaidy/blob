package blob

import (
	"time"

	"github.com/gofiber/fiber/v2"
)

type DataUnite int

const (
	B  DataUnite = 1
	KB DataUnite = 1024
	MB DataUnite = 1024 * KB
	GB DataUnite = 1024 * MB
	TB DataUnite = 1024 * GB
)

type Server struct {
	rootDir      string
	secretKey    string
	maxChunkSize DataUnite
	router       *fiber.App
	metadata     *MetadataStorage
}

type Bucket struct {
	Id        string    `json:"id"`
	CreatedAt time.Time `json:"createdAt"`
	Blobs     []*Blob   `json:"blobs"`
}

type Blob struct {
	Id        string    `json:"id"`
	BucketId  string    `json:"bucketId"`
	Size      int       `json:"size"`
	CreatedAt time.Time `json:"createdAt"`
}

type Access struct {
	Key       string    `json:"key"`
	BucketId  string    `json:"bucketId"`
	BlobId    string    `json:"blobId"`
	CreatedAt time.Time `json:"createdAt"`
}
