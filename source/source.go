package source

import (
	"time"
	"github.com/arizvisa/go-fetch"
)

type SourceLocal interface {
	fetch.Source
	LocalName() string
	LocalPath() string
	LocalTime() time.Time
}

type SourceRemote interface {
	fetch.Source
	RemoteName() string
	RemotePath() string
	RemoteTime() time.Time
}
