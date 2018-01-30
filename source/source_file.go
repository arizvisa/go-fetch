package source

import (
	"fmt"
	"context"
	"io"
	"path"
	"path/filepath"
	"net/url"
	"time"
	"os"
	"github.com/arizvisa/go-fetch"
	"strings"
)

const FILE_DEFAULT_BLOCKSIZE = 512 * 4

type SourceFile struct {
	block_size int64
	root_directory string
	working_directory string
	calculated_path string
	follow_symlinks bool
	Done context.CancelFunc
}

func NewFile() *SourceFile {
	wd, err := os.Getwd()
	if err != nil {
		// FIXME: Log the reason for failure
		return nil
	}

	return &SourceFile{
		block_size: FILE_DEFAULT_BLOCKSIZE,
		root_directory: filepath.VolumeName(wd),
		follow_symlinks: false,
		working_directory: filepath.ToSlash(wd),
	}
}

func (this *SourceFile) Config(key string, value interface{}) error {
	switch key {
		case `Set-BlockSize`:
			bs, ok := value.(int64)
			if !ok {
				return fmt.Errorf("%s: Unsupported type %T passed as value. Expected %T.", "Set-BlockSize", value, int64(0))
			}
			this.block_size = bs
		case `Set-Root-Path`:
			p, ok := value.(string)
			if !ok {
				return fmt.Errorf("%s: Unsupported type %T passed as value. Expected %T.", "Set-Root-Path", value, string(""))
			}
			this.root_directory = filepath.ToSlash(p)
		case `Set-Working-Path`:
			p, ok := value.(string)
			if !ok {
				return fmt.Errorf("%s: Unsupported type %T passed as value. Expected %T.", "Set-Working-Path", value, string(""))
			}
			this.working_directory = filepath.ToSlash(p)
		case `Set-Follow-Symlinks`:
			fs, ok := value.(bool)
			if !ok {
				return fmt.Errorf("%s: Unsupported type %T passed as value. Expected %T.", "Set-Follow-Symlinks", value, true)
			}
			this.follow_symlinks = fs
	}
	return fmt.Errorf("Unsupported option %#v with value %#v.", key, value)
}

func (this *SourceFile) Setup(ctx context.Context) (context.Context, error) {
	// Ensure that a url was specified
	if ctx.Value(fetch.CKSourcePath{}) == nil {
		return ctx, fmt.Errorf("Source path was not specified.")
	}

	uri, err := url.Parse(ctx.Value(fetch.CKSourcePath{}).(string))
	if err != nil {
		return ctx, fmt.Errorf("Unable to parse path %#v. (%#v)", ctx.Value(fetch.CKSourcePath{}).(string), err)
	}

	// Ensure that we're using the correct protocol scheme
	if uri.Scheme != "file" {
		return ctx, fmt.Errorf("Unsupported URL Scheme, %s. (%#v)", uri.Scheme, uri)
	}

	// Convert it to the actual path, and check to ensure it exists
	// just so we can make it through the setup here..
	rp := PathCreateFromFileUrl(this.working_directory, uri)
	if _, err := os.Stat(filepath.FromSlash(rp)); err != nil {
		return ctx, fmt.Errorf("Unable to get FileInfo for file %s. (%#v)", rp, err)
	}

	// If follow_symlinks is specified, then do as told.
	if this.follow_symlinks {
		p := filepath.FromSlash(rp)

		// If we failed, then restore the original path
		sp, err := filepath.EvalSymlinks(p)
		if err != nil {
			// XXX: Log error during call to EvalSymlinks
			sp = rp
		}
		rp = sp

	// Otherwise, just clean up the path and use it.
	} else {
		rp = filepath.Clean(rp)
	}

	// Store the final calculated path
	this.calculated_path = filepath.ToSlash(rp)

	// Now update the context with the new path formatted according
	// to the platform's desires.
	ctx = context.WithValue(ctx, fetch.CKSourcePath{}, rp)

	// That was all we needed to do
	ctx, this.Done = context.WithCancel(ctx)

	return ctx, nil
}

// Convert a url and it's different versions to a regular path
func PathCreateFromFileUrl(wd string, uri *url.URL) string {
	var result string

	// absolute path -- file://c:/absolute/path -> c:/absolute/path
	if strings.HasSuffix(uri.Host, ":") {
		result = path.Join(uri.Host, uri.Path)

		// semi-absolute path (current drive letter)
		//	-- file:///absolute/path -> drive:/absolute/path
	} else if uri.Host == "" && strings.HasPrefix(uri.Path, "/") {
		apath := uri.Path
		components := strings.Split(apath, "/")
		volume := filepath.VolumeName(wd)

		// semi-absolute absolute path (includes volume letter)
		// -- file://drive:/path -> drive:/absolute/path
		if len(components) > 1 && strings.HasSuffix(components[1], ":") {
			volume = components[1]
			apath = path.Join(components[2:]...)
		}

		result = path.Join(volume, apath)

		// relative path -- file://./relative/path -> ./relative/path
	} else if uri.Host == "." {
		result = path.Join(wd, uri.Path)

		// relative path -- file://relative/path -> ./relative/path
	} else {
		result = path.Join(wd, uri.Host, uri.Path)
	}
	return filepath.ToSlash(result)
}

func FileUrlCreateFromPath(path string, rel bool) (*url.URL, error) {
	return nil, fmt.Errorf("Not yet implemented!")
}

func (this *SourceFile) LocalPath() string {
	return filepath.FromSlash(this.calculated_path)
}

func (this *SourceFile) LocalName() string {
	_, p := path.Split(this.calculated_path)
	return p
}

func (this *SourceFile) LocalTime() *time.Time {
	rp := filepath.FromSlash(this.calculated_path)

	// Check if the file doesn't exist
	fi, err := os.Stat(rp)
	if err != nil {
		return nil
	}

	// Now that we know the file exists, return it's modification time
	lm := fi.ModTime()
	return &lm
}

func (this *SourceFile) Read(ctx context.Context) (<-chan []byte, error) {
	var out chan []byte

	if ctx.Value(fetch.CKSourceError{}) == nil {
		return out, fmt.Errorf("Unable to extract the error stream from the context")
	}
	err_s := ctx.Value(fetch.CKSourceError{}).(chan error)

	// Grab the path out of our context
	if ctx.Value(fetch.CKSourcePath{}) == nil {
		return out, fmt.Errorf("Unable to extract the source path from the context")
	}
	path := ctx.Value(fetch.CKSourcePath{}).(string)

	// Open the file so we can be sure it exists and that it's locked
	f, err := os.Open(path)
	if err != nil {
		return out, fmt.Errorf("Unable to open the file %#v. (%#v)", path, err)
	}

	fi, err := f.Stat()
	if err != nil {
		f.Close()
		return out, fmt.Errorf("Unable to read the stats from file %#v. (%#v)", f.Name(), err)
	}

	// Determine the offset, size, and blocksize to read
	ro := int64(0)
	if ctx.Value(fetch.CKSourceOffset{}) != nil {
		ro = ctx.Value(fetch.CKSourceOffset{}).(int64)
	}

	rs := fi.Size()
	if ctx.Value(fetch.CKSourceSize{}) != nil {
		rs = ctx.Value(fetch.CKSourceSize{}).(int64)
	}

	bs := this.block_size
	if ctx.Value(fetch.CKSourceSegment{}) != nil {
		bs = ctx.Value(fetch.CKSourceSegment{}).(int64)
	}

	// Check that the requested size is not absurd and fix it if so
	if rs < 0 || rs > fi.Size() {
		rs = fi.Size()
	}

	// Now we can seek the fp to correct position
	if _, err := f.Seek(ro, 0); err != nil {
		f.Close()
		return out, fmt.Errorf("Unable to seek file %#v to offset %d. (%#v)", f.Name(), ro, err)
	}

	// We should be good to go...
	out = make(chan []byte)

	go func(f *os.File, size, bs int64) {
		offset := int64(0)
		data := make([]byte, bs)
		defer f.Close()

	Transfer:
		for offset < size {
			// Read from the file
			n, err := f.Read(data)

			// Update our channels
			select {
			case <-ctx.Done():
				err_s <- fmt.Errorf("[%d/%d] Cancellation was requested during transfer", ro + offset, ro + size)
				this.Done()
				return

			case out <- data[:n]:
				offset += int64(n)

				// Check if there was an error during the read
				if err != nil {
					// If it was an EOF, then leave peacefully
					if err == io.EOF {
						err = err

					// Otherwise we should complain
					} else {
						err_s <- fmt.Errorf("[%d/%d] Unexpected error : %#v", ro + offset, ro + size, err)
					}

					// And then leave
					break Transfer
				}
			}
		}

		// We're done! Now we can check what happened...
		if offset < size {
			err_s <- fmt.Errorf("[%d/%d] Expected %d more bytes before terminating", ro + offset, ro + size, size - offset)
		} else if offset > size {
			err_s <- fmt.Errorf("[%d/%d] Received %d more bytes than expected", ro + offset, ro + size, offset - size)
		}
		this.Done()
	}(f, rs, bs)

	return out, nil
}
