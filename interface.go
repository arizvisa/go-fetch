package fetch

import "context"

// Context key types
type CKSourceError struct{}
type CKSourcePath struct{}
type CKSourceOffset struct{}
type CKSourceSize struct{}
type CKSourceSegment struct{}

/*
	Interface that describes where to fetch data from.
*/
type Source interface {
	Setup(ctx context.Context) (context.Context, error)	// Equivalent to WithContext
	Read(ctx context.Context) (<-chan []byte, error)
}

/*
	Interface that describes how to transform some input data.
*/
type Filter interface {
	Setup(ctx context.Context) (context.Context, error)	// Equivalent to WithContext
	Transfer(ctx context.Context, in <-chan []byte, out chan<- []byte) error
}

/*
	Terminal interface that describes how to store/persist an input data.
	Required for termination of a data pipeline.
*/
type Target interface {
	Setup(ctx context.Context) (context.Context, error)	// Equivalent to WithContext
	Write(ctx context.Context, in <-chan []byte) error
}
