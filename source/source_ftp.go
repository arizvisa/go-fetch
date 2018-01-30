package source

import (
	"fmt"
	"context"
	"strings"
	"time"
	"path"
	"io"
	"net"
	"net/url"
	"github.com/jlaffaye/ftp"
	"github.com/arizvisa/go-fetch"
)

const FTP_DEFAULT_MTU = 1500 /* ethernet */ - 20 /* ipv4 */ - 20 /* tcp */

const FTP_DEFAULT_USERNAME = "anonymous"
const FTP_DEFAULT_PASSWORD = "anonymous@"

const FTP_PROTOCOL_NAME = "ftp"
const FTP_PROTOCOL_PORT = 21

type SourceFtp struct {
	// configuration
	mtu int64
	userinfo *url.Userinfo
	timeout *time.Duration

	// entry cache
	location *url.URL
	currentDir string
	entry *ftp.Entry

	// context info
	Done context.CancelFunc
}

func NewFtp() *SourceFtp {
	return &SourceFtp{
		mtu: FTP_DEFAULT_MTU,
		userinfo: url.User(FTP_DEFAULT_USERNAME),
		timeout: nil,
	}
}

func (this *SourceFtp) Config(key string, value interface{}) error {
	switch key {
		case `Set-Username`:
			u, ok := value.(string)
			if !ok {
				return fmt.Errorf("%s: Unsupported type %T passed as value. Expected %T.", "Set-Username", value, string(""))
			}
			pw, passwordQ := this.userinfo.Password()
			if passwordQ {
				this.userinfo = url.UserPassword(u, pw)
			} else {
				this.userinfo = url.User(u)
			}
		case `Set-Password`:
			pw, ok := value.(string)
			if !ok {
				return fmt.Errorf("%s: Unsupported type %T passed as value. Expected %T.", "Set-Password", value, string(""))
			}
			u := this.userinfo.Username()
			this.userinfo = url.UserPassword(u, pw)

		case `Set-Timeout`:
			to, ok := value.(time.Duration)
			if !ok {
				return fmt.Errorf("%s: Unsupported type %T passed as value. Expected %T.", "Set-Timeout", value, time.Second)
			}
			this.timeout = &to
	}
	return fmt.Errorf("Unsupported option %#v with value %#v.", key, value)
}

func (this *SourceFtp) Setup(ctx context.Context) (context.Context, error) {
	if ctx.Value(fetch.CKSourcePath{}) == nil {
		return ctx, fmt.Errorf("Source path was not specified.")
	}

	// Check the source is understood by net/url
	uri, err := url.Parse(ctx.Value(fetch.CKSourcePath{}).(string))
	if err != nil {
		return ctx, fmt.Errorf("Unable to parse path %#v. (%#v)", ctx.Value(fetch.CKSourcePath{}).(string), err)
	}

	// Check that we're using the only valid scheme for ftp
	if uri.Scheme != "ftp" {
		return ctx, fmt.Errorf("Unsupported URL Scheme, %s. (%#v)", uri.Scheme, uri)
	}

	// Check to see if we need to figure out the port number because
	// the user chose to not specify one (This is more common than you think, jlaffaye!)
	if uri.Port() == "" {
		port, err := net.LookupPort("ip4", "ftp")
		if err != nil {
			port = FTP_PROTOCOL_PORT
		}
		// Fix up the hostname to include the port number
		uri.Host = fmt.Sprintf("%s:%d", uri.Hostname(), port)

		// Re-insert the fixed CKSourcePath back into the context
		ctx = context.WithValue(ctx, fetch.CKSourcePath{}, uri.String())
	}

	// Fix up the Userinfo with a default pass if there was none specified
	if _, ok := this.userinfo.Password(); !ok {
		this.userinfo = url.UserPassword(this.userinfo.Username(), FTP_DEFAULT_PASSWORD)
	}

	// Cache the url just because
	this.location = uri

	// Yep, that was it.
	ctx, this.Done = context.WithCancel(ctx)

	return ctx, nil
}

func (this *SourceFtp) RemotePath() string {
	if this.currentDir == "" || this.entry == nil {
		return this.location.String()
	}

	cd := this.currentDir
	if strings.HasSuffix(cd, "/") {
		return fmt.Sprintf("%s://%s", this.location.Scheme, this.location.Host + this.currentDir + this.entry.Name)
	}
	return fmt.Sprintf("%s://%s/%s", this.location.Scheme, this.location.Host + this.currentDir, this.entry.Name)
}

func (this *SourceFtp) RemoteName() string {
	if this.entry == nil {
		return ""
	}
	return this.entry.Name
}

func (this *SourceFtp) RemoteTime() *time.Time {
	if this.entry == nil {
		return nil
	}
	ts := this.entry.Time
	return &ts
}

func (this *SourceFtp) Read(ctx context.Context) (<-chan []byte, error) {
	var out chan []byte

	// Grab the error stream out of our context
	if ctx.Value(fetch.CKSourceError{}) == nil {
		return out, fmt.Errorf("Unable to locate error stream within context")
	}
	err_s := ctx.Value(fetch.CKSourceError{}).(chan error)

	// Grab the path out of our context and make it a url
	if ctx.Value(fetch.CKSourcePath{}) == nil {
		return out, fmt.Errorf("Unable to extract the source path from the context")
	}

	uri, err := url.Parse(ctx.Value(fetch.CKSourcePath{}).(string))
	if err != nil {
		return out, fmt.Errorf("Unable to parse path %#v. (%#v)", ctx.Value(fetch.CKSourcePath{}).(string), err)
	}

	// Create our ftp client
	cli, err := ftp.Dial(fmt.Sprintf("%s:%s", uri.Hostname(), uri.Port()))
	if err != nil {
		return out, fmt.Errorf("Unable to connect to %s:%s. (%#v)", uri.Hostname(), uri.Port(), err)
	}

	// Handle authentication
	userinfo := this.userinfo
	if uri.User != nil {
		userinfo = uri.User
	}

	pass, ok := userinfo.Password()
	if !ok {
		pass = FTP_DEFAULT_PASSWORD
	}

	err = cli.Login(userinfo.Username(), pass)
	if err != nil {
		cli.Quit()
		return out, fmt.Errorf("Unable to authenticate to %s:%d with user `%s`. (%#v)", uri.Hostname(), uri.Port(), userinfo.Username(), err)
	}

	// Locate the specified path
	p := path.Dir(uri.Path)

	err = cli.ChangeDir(p)
	if err != nil {
		cli.Quit()
		return out, fmt.Errorf("Unable to change directory to `%s`. (%#v)", p, err)
	}

	this.currentDir, err = cli.CurrentDir()
	if err != nil {
		cli.Quit()
		return out, fmt.Errorf("Unable to confirm that current directory is `%s`. (%#v)", p, err)
	}

	// Enumerate files in current directory
	_, name := path.Split(uri.Path)

	entries, err := cli.List(".")
	if err != nil {
		cli.Quit()
		return out, fmt.Errorf("Unable to list files in directory `%s`. (#v)", this.currentDir, err)
	}
	if len(entries) == 0 {
		cli.Quit()
		return out, fmt.Errorf("Unable to find any files in directory `%s`.", this.currentDir)
	}

	// Gather stats about requested file
	for _, e := range entries {
		if e.Type == ftp.EntryTypeFile && e.Name == name {
			this.entry = e
			break
		}
	}
	if this.entry == nil {
		cli.Quit()
		return out, fmt.Errorf("Unable to locate file `%s` in directory `%s`.", name, this.currentDir)
	}

	// We got some info that we can use to dtermine the offset and filesize to read
	ro := int64(0)
	if ctx.Value(fetch.CKSourceOffset{}) != nil {
		ro = ctx.Value(fetch.CKSourceOffset{}).(int64)
	}
	if ro < 0 {
		ro = 0
	}

	rs := int64(this.entry.Size)
	if ctx.Value(fetch.CKSourceSize{}) != nil {
		rs = ctx.Value(fetch.CKSourceSize{}).(int64)
	}
	if rs > int64(this.entry.Size) {
		rs = int64(this.entry.Size)
	}

	// Determine the segment size to read the body with
	ss := this.mtu
	if ctx.Value(fetch.CKSourceSegment{}) != nil {
		ss = ctx.Value(fetch.CKSourceSegment{}).(int64)
	}

	// Now we can begin to download the specified file
	reader, err := cli.RetrFrom(uri.Path, uint64(ro))
	if err != nil {
		cli.Quit()
		return out, fmt.Errorf("Unable to read file `%s` at offset %d.", uri.Path, ro)
	}

	// Okay, that was it...so now we can transfer things
	out = make(chan []byte)

	go func(ctx context.Context, client ftp.ServerConn, r io.Reader, size, segment int64) {
		offset := int64(0)
		data := make([]byte, segment)
		defer client.Quit()

	Transfer:
		for offset < size {
			// Read from the ftp.Request
			n, err := r.Read(data)

			// Check if we were cancelled via the context
			select {
			case <-ctx.Done():
				err_s <- fmt.Errorf("[%d/%d] Cancellation was requested during transfer", ro + offset, ro + size)
				this.Done()
				return

			// Update our output channel
			case out <- data[:n]:
				offset += int64(n)

				// Check if there was an error while transferring
				if err != nil {
					// If it was an EOF, then leave peacefully
					if err == io.EOF {
						err = err

					// Otherwise we can bitch and complain
					} else {
						err_s <- fmt.Errorf("[%d/%d] Unexpected error : %#v", ro + offset, ro + size, err)
					}

					// And then leave
					break Transfer
				}
			}
		}

		// Whee...check and see what happened and update the error stream
		if offset < size {
			err_s <- fmt.Errorf("[%d/%d] Expected %d more bytes before terminating", ro + offset, ro + size, size - offset)
		} else if offset > size {
			err_s <- fmt.Errorf("[%d/%d] Received %d more bytes than expected", ro + offset, ro + size, offset - size)
		}
		this.Done()

	}(ctx, *cli, reader, rs, ss)
	return out, nil
}
