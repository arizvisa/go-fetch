package source

import (
	"fmt"
	"context"
	"runtime"
	"path"
	"path/filepath"
	"net/url"
	"time"
	"os"
	"github.com/arizvisa/go-fetch"
)

const SMB_DEFAULT_BLOCKSIZE = 0x4104
const SMB_UNC_PREFIX = string(os.PathSeparator) + string(os.PathSeparator)

type SourceSamba struct {
	block_size int64
	calculated_path string
	Done context.CancelFunc
}

func NewSamba() *SourceSamba {
	return &SourceSamba{
		block_size: SMB_DEFAULT_BLOCKSIZE,
	}
}

func (this* SourceSamba) Config(key string, value interface{}) error {
	switch key {
		case `Set-BlockSize`:
			bs, ok := value.(int64)
			if !ok {
				return fmt.Errorf("%s: Unsupported type %T passed as value. Expected %T.", "Set-BlockSize", value, int64(0))
			}
			this.block_size = bs
	}
	return fmt.Errorf("Unsupported option %#v with value %#v.", key, value)
}

func (this *SourceSamba) Setup(ctx context.Context) (context.Context, error) {
	// Ensure that a url was specified
	if ctx.Value(fetch.CKSourcePath{}) == nil {
		return ctx, fmt.Errorf("Source path was not specified.")
	}

	uri, err := url.Parse(ctx.Value(fetch.CKSourcePath{}).(string))
	if err != nil {
		return ctx, fmt.Errorf("Unable to parse path %#v. (%#v)", ctx.Value(fetch.CKSourcePath{}).(string), err)
	}

	// Ensure that we're using the correct protocol scheme
	if uri.Scheme != "smb" {
		return ctx, fmt.Errorf("Unsupported URL Scheme, %s. (%#v)", uri.Scheme, uri)
	}

	// This only works on windows, so fail if we're on something else
	if runtime.GOOS != "windows" {
		return ctx, fmt.Errorf("smb:// sources are not supported on non-windows platforms. (runtime.GOOS != %#v)", runtime.GOOS)
	}

	// Convert the smb:// url into a properly formatted UNC path
	rp := PathCreateFromSmbUrl(uri)

	// Store the final calculated path
	this.calculated_path = rp

	// Nowe we can update the context with the calculated path
	ctx = context.WithValue(ctx, fetch.CKSourcePath{}, rp)

	// That was pretty simple
	ctx, this.Done = context.WithCancel(ctx)

	return ctx, nil
}

func PathCreateFromSmbUrl(uri *url.URL) string {
	return SMB_UNC_PREFIX + filepath.FromSlash(path.Join(uri.Host, uri.Path))
}

func SmbUrlCreateFromPath(path string, volume string) (*url.URL, error) {
	return nil, fmt.Errorf("Not yet implemented!")
}

/*
	Fortunately, smb is weird on windows because local apis can
	access it as well, so this source is both a Local and Remote one.
*/
func (this *SourceSamba) LocalPath() string {
	return this.RemotePath()
}

func (this *SourceSamba) LocalName() string {
	return this.RemoteName()
}

func (this *SourceSamba) LocalTime() *time.Time {
	return this.RemoteTime()
}

func (this *SourceSamba) RemotePath() string {
	return this.calculated_path
}

func (this *SourceSamba) RemoteName() string {
	// Take our UNC path
	p := this.calculated_path
	// Strip off the volume (\\host\share)
	vol := filepath.VolumeName(p)
	// Convert the rest into a regular path
	rp := filepath.ToSlash(p[len(vol):])
	// Now we can grab the name
	_, name := path.Split(rp)
	return name
}

func (this *SourceSamba) RemoteTime() *time.Time {
	// Stat the path to our file on whatever our share is
	fi, err := os.Stat(this.calculated_path)
	if err != nil {
		return nil
	}

	// Pretty simple, eh? Let's return the modification time.
	lm := fi.ModTime()
	return &lm
}

func (this *SourceSamba) Read(ctx context.Context) (<-chan []byte, error) {
	/*
		This may be cheating, but SMB transfers are really just local
		file transfers. So we'll can just forward our context
		to the .Read() method of SourceFile.
	*/
	nf := NewFile()
	nf.Config(`Set-BlockSize`, this.block_size)
	nf.Done = this.Done
	return nf.Read(ctx)
}

