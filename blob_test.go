package blob

import (
	"io"
	"net/http"
	"os"
	"testing"
	"time"
)

func createSmallFile() {
	file, err := os.OpenFile("small_file", os.O_CREATE|os.O_WRONLY|os.O_APPEND, os.ModePerm)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	for i := 0; i < 2000; i++ {
		_, err := file.Write([]byte("This is a line.\n"))
		if err != nil {
			panic(err)
		}
	}
}

func createBigFile() {
	file, err := os.OpenFile("big_file", os.O_CREATE|os.O_WRONLY, os.ModePerm)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	for i := 0; i < 2000000; i++ {
		_, err := file.Write([]byte("This is a line.\n"))
		if err != nil {
			panic(err)
		}
	}
}

func TestBlobServer(t *testing.T) {
	t.Log("creating small/big files...")
	createSmallFile()
	createBigFile()

	go func() {
		t.Log("starting blob server...")
		s := NewServer(1*MB, "1234", "./root_dir", "./metadata_dir")
		if err := s.Listen(":3000"); err != nil {
			panic(err)
		}
	}()

	time.Sleep(2 * time.Second)

	serverURL := "http://localhost:3000"

	// ========================================================

	t.Log("creating 'bucket1'...")
	req, err := http.NewRequest(http.MethodPost, serverURL+"/buckets?bucket_id=bucket1", http.NoBody)
	if err != nil {
		t.Fatal("error creating req: ", err)
	}
	req.Header.Set("Authorization", "Bearer 1234")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal("error sending req: ", err)
	}
	if resp.StatusCode != http.StatusCreated {
		t.Fatal("expected 201 status code, got ", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal("error reading resp body: ", err)
	}
	t.Logf("resp: %s", body)

	// ========================================================

	t.Log("creating small_blob...")
	req, err = http.NewRequest(http.MethodPost, serverURL+"/buckets/bucket1/blobs?blob_id=small_blob", http.NoBody)
	if err != nil {
		t.Fatal("error creating request: ", err)
	}
	req.Header.Set("Authorization", "Bearer 1234")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal("error sending request: ", err)
	}
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201 status code for creation, got %d", resp.StatusCode)
	}

	// ========================================================

	t.Log("uploading small_blob...")
	smallFile, err := os.Open("small_file")
	if err != nil {
		t.Fatal("failed to open small file: ", err)
	}
	defer smallFile.Close()
	req, err = http.NewRequest(http.MethodPut, serverURL+"/buckets/bucket1/blobs/small_blob", smallFile)
	if err != nil {
		t.Fatal("error creating upload request: ", err)
	}
	req.Header.Set("Authorization", "Bearer 1234")
	req.Header.Set("Content-Type", "application/octet-stream")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal("error sending upload request: ", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 status code for upload, got %d", resp.StatusCode)
	}

	// ========================================================

	t.Log("creating big_blob...")
	req, err = http.NewRequest(http.MethodPost, serverURL+"/buckets/bucket1/blobs?blob_id=big_blob", http.NoBody)
	if err != nil {
		t.Fatal("error creating request: ", err)
	}
	req.Header.Set("Authorization", "Bearer 1234")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal("error sending request: ", err)
	}
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201 status code for creation, got %d", resp.StatusCode)
	}

	// ========================================================

	// Delete the blob
	t.Log("deleting 'small_blob'...")
	deleteBlobReq, err := http.NewRequest(http.MethodDelete, serverURL+"/buckets/bucket1/blobs/small_blob", nil)
	if err != nil {
		t.Fatal("error creating delete blob request: ", err)
	}
	deleteBlobReq.Header.Set("Authorization", "Bearer 1234")
	deleteBlobResp, err := http.DefaultClient.Do(deleteBlobReq)
	if err != nil {
		t.Fatal("error sending delete blob request: ", err)
	}
	if deleteBlobResp.StatusCode != http.StatusNoContent {
		t.Fatalf("expected 204 status code for blob deletion, got %d", deleteBlobResp.StatusCode)
	}
	t.Log("Blob 'small_blob' deleted successfully")

	// Delete the bucket
	t.Log("deleting 'bucket1'...")
	deleteBucketReq, err := http.NewRequest(http.MethodDelete, serverURL+"/buckets/bucket1", nil)
	if err != nil {
		t.Fatal("error creating delete bucket request: ", err)
	}
	deleteBucketReq.Header.Set("Authorization", "Bearer 1234")
	deleteBucketResp, err := http.DefaultClient.Do(deleteBucketReq)
	if err != nil {
		t.Fatal("error sending delete bucket request: ", err)
	}
	if deleteBucketResp.StatusCode != http.StatusNoContent {
		t.Fatalf("expected 204 status code for bucket deletion, got %d", deleteBucketResp.StatusCode)
	}
	t.Log("Bucket 'bucket1' deleted successfully")
}
