package blob

import (
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"

	_ "github.com/mattn/go-sqlite3"
)

type MetadataStorage struct {
	db *sql.DB
}

func NewMetadataStorage(dir string) *MetadataStorage {
	db, err := sql.Open("sqlite3", filepath.Join(dir, "metadata.db"))
	if err != nil {
		panic(fmt.Sprintf("error connecting to db: %+v", err))
	}

	if err := db.Ping(); err != nil {
		panic(fmt.Sprintf("error pinging db: %+v", err))
	}

	metadata := &MetadataStorage{db: db}

	if err := metadata.migrate(); err != nil {
		panic(fmt.Sprintf("error migrating db: %+v", err))
	}

	return metadata
}

func (me *MetadataStorage) checkIfBucketExists(id string) (bool, error) {
	query := `SELECT 1 FROM buckets WHERE id = ?;`
	if err := me.db.QueryRow(query, id).Scan(new(int)); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (me *MetadataStorage) createBucket(bucket *Bucket) error {
	query := `
    INSERT INTO buckets (id, created_at)
    VALUES (?, ?);
    `
	if _, err := me.db.Exec(query, bucket.Id, bucket.CreatedAt); err != nil {
		return err
	}
	return nil
}

func (me *MetadataStorage) createBlob(blob *Blob) error {
	query := `
    INSERT INTO blobs (id, bucket_id, size, created_at)
    VALUES (?, ?, ?, ?);
    `
	if _, err := me.db.Exec(query, blob.Id, blob.BucketId, blob.Size, blob.CreatedAt); err != nil {
		return err
	}
	return nil
}

func (me *MetadataStorage) getAllBuckets() ([]*Bucket, error) {
	query := `
    SELECT 
        id,
        created_at
    FROM buckets;
    `
	rows, err := me.db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	buckets := []*Bucket{}

	for rows.Next() {
		bucket := &Bucket{}
		if err := rows.Scan(&bucket.Id, &bucket.CreatedAt); err != nil {
			return nil, err
		}
		buckets = append(buckets, bucket)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	for _, bkt := range buckets {
		blobs, err := me.getBlobsPerBucket(bkt.Id)
		if err != nil {
			return nil, err
		}
		bkt.Blobs = blobs
	}

	return buckets, nil
}

func (me *MetadataStorage) getBucket(id string) (*Bucket, error) {
	query := `
    SELECT
        created_at
    FROM buckets
    WHERE id = ?;
    `
	bucket := &Bucket{Id: id}
	if err := me.db.QueryRow(query, id).Scan(&bucket.CreatedAt); err != nil {
		return nil, err
	}

	blobs, err := me.getBlobsPerBucket(id)
	if err != nil {
		return nil, err
	}
	bucket.Blobs = blobs

	return bucket, nil
}

func (me *MetadataStorage) deleteBucket(id string) error {
	query := `DELETE FROM buckets WHERE id = ?;`
	if _, err := me.db.Exec(query, id); err != nil {
		return err
	}
	return nil
}

func (me *MetadataStorage) checkIfBlobExists(bucketId, blobId string) (bool, error) {
	query := `SELECT 1 FROM blobs WHERE id = ? AND bucket_id = ?;`
	if err := me.db.QueryRow(query, blobId, bucketId).Scan(new(int)); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (me *MetadataStorage) getBlobsPerBucket(id string) ([]*Blob, error) {
	query := `
    select
        id,
        size,
        created_at
    FROM blobs
    WHERE bucket_id = ?;
    `
	rows, err := me.db.Query(query, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	blobs := []*Blob{}

	for rows.Next() {
		blob := &Blob{BucketId: id}
		if err := rows.Scan(&blob.Id, &blob.Size, &blob.CreatedAt); err != nil {
			return nil, err
		}
		blobs = append(blobs, blob)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return blobs, nil
}

func (me *MetadataStorage) getBlob(bucketId, blobId string) (*Blob, error) {
	query := `
    SELECT 
        size,
        created_at
    FROM blobs 
    WHERE id = ? AND bucket_id = ?;
    `
	blob := &Blob{Id: blobId, BucketId: bucketId}

	if err := me.db.QueryRow(query, blobId, bucketId).Scan(&blob.Size, &blob.CreatedAt); err != nil {
		return nil, err
	}

	return blob, nil
}

func (me *MetadataStorage) incrementBlobSize(bucketId, blobId string, amount int) error {
	query := `
    UPDATE blobs 
    SET size = size + ? 
    WHERE bucket_id = ? AND id = ?;
    `
	if _, err := me.db.Exec(query, amount, bucketId, blobId); err != nil {
		return err
	}
	return nil
}

func (me *MetadataStorage) deleteBlob(bucketId, blobId string) error {
	query := `DELETE FROM blobs WHERE id = ? AND bucket_id = ?;`
	if _, err := me.db.Exec(query, blobId, bucketId); err != nil {
		return err
	}
	return nil
}

func (me *MetadataStorage) createAccess(accessKey *Access) error {
	query := `
    INSERT INTO accesses (key, bucket_id, blob_id, created_at)
    VALUES (?, ?, ?, ?);
    `
	if _, err := me.db.Exec(query, accessKey.Key, accessKey.BucketId, accessKey.BlobId, accessKey.CreatedAt); err != nil {
		return err
	}
	return nil
}

func (me *MetadataStorage) checkIfAccessExists(key string) (bool, error) {
	query := `SELECT 1 FROM accesses WHERE key = ?;`
	if err := me.db.QueryRow(query, key).Scan(new(int)); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (me *MetadataStorage) deleteAccess(key string) error {
	query := `DELETE FROM accesses WHERE key = ?;`
	if _, err := me.db.Exec(query, key); err != nil {
		return err
	}
	return nil
}

func (me *MetadataStorage) getBlobOfAccess(key string) (*Blob, error) {
	query := `
    SELECT 
        blobs.id,
        blobs.bucket_id,
        blobs.size,
        blobs.created_at
    FROM accesses
    INNER JOIN blobs ON blobs.bucket_id = accesses.bucket_id AND blobs.id = accesses.blob_id
    WHERE key = ?;
    `
	blob := &Blob{}

	if err := me.db.QueryRow(query, key).Scan(&blob.Id, &blob.BucketId, &blob.Size, &blob.CreatedAt); err != nil {
		return nil, err
	}

	return blob, nil
}

func (me *MetadataStorage) migrate() error {
	// NOTE: I want expirations for accesses to be managed by the client.
	query := `
    CREATE TABLE IF NOT EXISTS buckets (
        id TEXT,
        created_at TIMESTAMP,

        PRIMARY KEY (id)
    );
    CREATE TABLE IF NOT EXISTS blobs (
        id TEXT,
        bucket_id TEXT,
        size INTEGER,
        created_at TIMESTAMP,

        PRIMARY KEY (id, bucket_id),
        FOREIGN KEY (bucket_id) REFERENCES buckets(id) ON DELETE CASCADE 
    );
    CREATE TABLE IF NOT EXISTS accesses (
        key TEXT,
        bucket_id TEXT,
        blob_id TEXT,
        created_at TIMESTAMP,

        PRIMARY KEY (key),
        FOREIGN KEY (bucket_id, blob_id) REFERENCES blobs(bucket_id, id) ON DELETE CASCADE
    );
    `
	if _, err := me.db.Exec(query); err != nil {
		return err
	}
	return nil
}
