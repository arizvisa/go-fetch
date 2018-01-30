package source

import "context"
import "github.com/arizvisa/go-fetch"

func test_SourceHttp_Setup() {
	ctx := context.Background()
	// Test if no CKSourcePath is specified
	ctx = context.WithValue(fetch.CKSourcePath{}, "")
	// Test that url.Parse fails on an invalid url
	// Test that we fail on an unsupported url scheme
	// Test state of this.request.Close
	// Check that this.Done is set
	// Check if peek_method is not defined
}

func test_SourceHttp_Peek() {
	// Check peek with timeout
	// Check peek with a 404
	// Check peek with a 301 (redirect)
	// Check peek with a 200
	// Check resulting context includes content-length
	// Check resulting context includes fixed content-length if requested is larger than server's
}

func test_SourceHttp_Config() {
	// Test invalid types for each key
	// Test invalid key failure
}

func test_SourceHttp_Read() {
	// Check read with a failed Range header
	// Check read with a successful Range heade
	// Check read with a non-200 status code
	// Check read with Content-Length == -1
	// Check read with a positive context offset set and a server that doesn't have the Accept-Ranges header
	// Check read with an unexpected error during read
	// Check read with a 0-byte file
	// Check read with an x-byte file (x < segment)
	// Check read with an x-byte file (x > segment)
	// Check that read is cancellable with a cancellable context
}
