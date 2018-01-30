package source

import (
	"net"
	"net/url"
	"net/http"
	"context"
	"fmt"
	"strings"
	"bytes"
	"time"
	"mime"
	"path"
	"io"
	"io/ioutil"
	"github.com/arizvisa/go-fetch"
)

// Default configuration
const HTTP_DEFAULT_METHOD = "GET"
const HTTP_DEFAULT_FOLLOW_REDIRECT = 10
const HTTP_DEFAULT_PEEK_METHOD = "HEAD"
const HTTP_DEFAULT_MTU = 1500 /* ethernet */ - 20 /* ipv4 */ - 20 /* tcp */

// Configuration of HTTP source
type SourceHttp struct {
	peek_method string
	follow_redirect int

	transport *http.Transport

	request http.Request
	response *http.Response

	Done context.CancelFunc
}

// Instantiate new HTTP source
func NewHttp() *SourceHttp {
	body := strings.NewReader("")

	req, err := http.NewRequest(HTTP_DEFAULT_METHOD, "", body)
	if err == nil {
		return &SourceHttp{
			request: *req, response: nil,
			peek_method: HTTP_DEFAULT_PEEK_METHOD,
			follow_redirect: HTTP_DEFAULT_FOLLOW_REDIRECT,
			transport: http.DefaultTransport.(*http.Transport),
		}
	}
	return nil
}

func (this *SourceHttp) SetTransport(transport *http.Transport) *http.Transport {
	var result *http.Transport
	result, this.transport = this.transport, transport
	return result
}

func (this *SourceHttp) Config(key string, value interface{}) error {
	switch key {
		case `Add-Cookie`:
			c, ok := value.(http.Cookie)
			if !ok {
				return fmt.Errorf("%s: Unsupported type %T passed as value. Expected %T.", "Add-Cookie", value, http.Cookie{})
			}
			this.request.AddCookie(&c)
			return nil

		case `Set-Method`:
			meth, ok := value.(string)
			if !ok {
				return fmt.Errorf("%s: Unsupported type %T passed as value. Expected %T.", "Set-Method", value, string(""))
			}
			this.request.Method = meth
			return nil

		case `Set-UserAgent`:
			agent, ok := value.(string)
			if !ok {
				return fmt.Errorf("%s: Unsupported type %T passed as value. Expected %T.", "Set-UserAgent", value, string(""))
			}
			this.request.Header.Set("User-Agent", agent)
			return nil

		case `Set-Header`:
			var row []string
			switch value.(type) {
				case string:
					row = strings.SplitN(value.(string), ":", 2)
					row[1] = strings.TrimPrefix(row[1], " ")
				case []string:
					row = value.([]string)
				default:
					return fmt.Errorf("%s: Unsupported type %T passed as value. Expected %T or %T.", "Set-Header", value, string(""), [2]string{})
			}
			this.request.Header.Set(row[0], row[1])
			return nil

		case `Remove-Header`:
			key, ok := value.(string)
			if !ok {
				return fmt.Errorf("%s: Unsupported type %T passed as value. Expected %T.", "Remove-Header", value, string(""))
			}
			this.request.Header.Del(key)
			return nil

		case `Set-Url`:
			uri, ok := value.(*url.URL)
			if !ok {
				return fmt.Errorf("%s: Unsupported type %T passed as value. Expected %T.", "Set-Url", value, url.URL{})
			}
			this.request.URL = uri
			return nil

		case `Set-Content`:
			switch value.(type) {
				case []byte:
					this.request.Body = ioutil.NopCloser(bytes.NewReader(value.([]byte)))
					this.request.ContentLength = int64(len(value.([]byte)))
				case string:
					this.request.Body = ioutil.NopCloser(strings.NewReader(value.(string)))
					this.request.ContentLength = int64(len(value.(string)))
				case bytes.Buffer:
					res := value.(*bytes.Buffer)
					this.request.Body = ioutil.NopCloser(res)
					this.request.ContentLength = int64(res.Len())
				case bytes.Reader:
					res := value.(*bytes.Reader)
					this.request.Body = ioutil.NopCloser(res)
					this.request.ContentLength = int64(res.Len())
				case strings.Reader:
					res := value.(*strings.Reader)
					this.request.Body = ioutil.NopCloser(res)
					this.request.ContentLength = int64(res.Len())
				case io.Reader:
					this.request.Body = value.(io.ReadCloser)
				default:
					return fmt.Errorf("%s: Unsupported type %T passed as value. Expected something like %s.", "Set-Content", value, "io.Reader")
			}
			return nil

		case `Set-ContentLength`:
			length, ok := value.(int64)
			if !ok {
				return fmt.Errorf("%s: Unsupported type %T passed as value. Expected %T.", "Set-ContentLength", value, int64(0))
			}
			this.request.ContentLength = length
			return nil

		case `Set-Host`:
			if _, ok := value.(string); !ok {
				return fmt.Errorf("%s: Unsupported type %T passed as value. Expected %T.", "Set-Host", value, string(""))
			}
			this.request.Host = value.(string)
			return nil

		case `Set-FollowRedirectCount`:
			if _, ok := value.(int); !ok {
				return fmt.Errorf("%s: Unsupported type %T passed as value. Expected %T.", "Set-FollowRedirectCount", value, int(0))
			}
			this.follow_redirect = value.(int)
			return nil

		case `Set-Close`:
			if _, ok := value.(bool); !ok {
				return fmt.Errorf("%s: Unsupported type %T passed as value. Expected %T.", "Set-Close", value, bool(true))
			}
			this.request.Close = value.(bool)
			return nil

		case `Set-PeekMethod`:
			if _, ok := value.(string); !ok {
				return fmt.Errorf("%s: Unsupported type %T passed as value. Expected %T.", "Set-PeekMethod", value, string(""))
			}
			this.peek_method = value.(string)
			return nil

		case `Set-Timeout`:
			if _, ok := value.(time.Duration); !ok {
				return fmt.Errorf("%s: Unsupported type %T passed as value. Expected %T.", "Set-Timeout", value, time.Second)
			}
			to := value.(time.Duration)

			// Tweak the transport using the new timeout
			this.transport.DialContext = (&net.Dialer{Timeout: to}).DialContext
			this.transport.TLSHandshakeTimeout = to
			return nil
	}
	return fmt.Errorf("Unsupported option %#v with value %#v.", key, value)
}

func (this *SourceHttp) peek_CheckRedirect(req *http.Request, via []*http.Request) error {
	if len(via) < this.follow_redirect {
		return nil
	}
	return http.ErrUseLastResponse
}

func (this *SourceHttp) default_CheckRedirect(req *http.Request, via []*http.Request) error {
	if len(via) < this.follow_redirect {
		return nil
	}
	return fmt.Errorf("CheckRedirect: Stopped following after %d redirects.", this.follow_redirect)
}

func (this *SourceHttp) Setup(ctx context.Context) (context.Context, error) {
	if ctx.Value(fetch.CKSourcePath{}) == nil {
		return ctx, fmt.Errorf("Source path was not specified.")
	}

	uri, err := url.Parse(ctx.Value(fetch.CKSourcePath{}).(string))
	if err != nil {
		return ctx, fmt.Errorf("Unable to parse path %#v. (%#v)", ctx.Value(fetch.CKSourcePath{}).(string), err)
	}

	if uri.Scheme != "http" && uri.Scheme != "https" {
		return ctx, fmt.Errorf("Unsupported URL Scheme, %s. (%#v)", uri.Scheme, uri)
	}

	// Specify the URL in the request (this is used for peek anyways)
	err = this.Config(`Set-Url`, uri)
	if err != nil {
		return ctx, fmt.Errorf("Unable to set URL inside %T. (%#v)", this.request, err)
	}

	// Set the close flag for the requiest
	err = this.Config(`Set-Close`, true)
	if err != nil {
		return ctx, fmt.Errorf("Unable to set close flag inside %T. (%#v)", this.request, err)
	}

	// Our request will be based on the requested context
	this.request = *this.request.WithContext(ctx)

	// Make a HEAD request to figure out the content-disposition
	// FIXME: Split this up into another method so it's testable
	if this.peek_method != "" {
		// Make a copy of our current request
		/*
		pr, err := http.NewRequest(this.peek_method, this.request.URL.String(), this.request.Body)
		if err != nil {
			return ctx, fmt.Errorf("Unable to make a copy of %T required to peek at HTTP Source: %#v", this.request, err)
		}
		*/

		// Make a shallow copy of the request with .WithContext, and set our peek parameters
		pr := this.request.WithContext(ctx)
		pr.Method = this.peek_method

		// Now we can make a client with a timeout
		cl := &http.Client{
			Timeout: this.transport.TLSHandshakeTimeout,
			Transport: this.transport,
			CheckRedirect: this.peek_CheckRedirect,
		}

		// And then peek at the HTTP headers with our temporary peek request
		resp, err := cl.Do(pr.WithContext(ctx))
		if err != nil {
			return ctx, fmt.Errorf("Unable to peek at HTTP Source: %#v", err.Error())
		}
		resp.Body.Close()	// We don't care about the body, so ignore.

		// Check to see that we have what we're looking for..
		if resp.StatusCode != 200 {
			return ctx, fmt.Errorf("HTTP Status %d while trying to peek at HTTP Source: %#v", resp.StatusCode, resp)
		}

		// We got a successful response
		this.response = resp

		// Set the maximum file size if one hasn't been set already
		if ctx.Value(fetch.CKSourceSize{}) == nil {
			ctx = context.WithValue(ctx, fetch.CKSourceSize{}, resp.ContentLength)

		// Fix the source size if the server thinks we're smaller than desired
		} else if resp.ContentLength < ctx.Value(fetch.CKSourceSize{}).(int64) {
			// FIXME: Log this error and that we fix it up.
			ctx = context.WithValue(ctx, fetch.CKSourceSize{}, resp.ContentLength)
		}

		// Check to see if it's possible to resume things...
		if resp.Header.Get("Accept-Ranges") != "bytes" {
			// FIXME: Log that we're unable to resume and we'll need to "simulate" it.
		}
	}

	// We've gotten through all the requirements, so assign a cancel handler
	ctx, this.Done = context.WithCancel(ctx)

	return ctx, nil
}

func (this *SourceHttp) RemoteName() string {
	if this.response == nil {
		// FIXME: Log error
		return ""
	}

	headers := this.response.Header
	cd := headers.Get("Content-Disposition")

	// Try and parse the media type
	if _, params, err := mime.ParseMediaType(cd); err == nil {
		// Does the filename key exist? ok, then we won.
		if result, ok := params["filename"]; ok {
			return result
		}
		// XXX: Log an error saying the content-disposition was not found
	}

	// Okay, so let's try the .RemotePath()
	rpath := this.RemotePath()

	// This should be a URL, so we should be able to just grab the filename.
	_, name := path.Split(rpath)

	return name
}

func (this *SourceHttp) RemotePath() string {
	if this.response == nil {
		return ""
	}

	// FIXME: Log error or check and see if ErrNoLocation was returned
	if uri, err := this.response.Location(); err == nil {
		return uri.String()
	}

	// Okay, we can't determine the request that got this response...so fail.
	if this.response.Request == nil {
		return ""
	}
	return this.response.Request.URL.String()
}

func (this *SourceHttp) RemoteTime() *time.Time {
	if this.response == nil {
		return nil
	}

	// First try the Last-Modified header
	lm := this.response.Header.Get("Last-Modified")

	// Nope, so okay...try the Date?
	if lm == "" {
		lm = this.response.Header.Get("Date")
	}

	// Parse it and use it.
	if result, err := http.ParseTime(lm); err == nil {
		return &result
	}

	// FIXME: Log an error as we had a parsing issue
	return nil
}

func (this *SourceHttp) Read(ctx context.Context) (<-chan []byte, error) {
	var out chan []byte

	if ctx.Value(fetch.CKSourceError{}) == nil {
		return out, fmt.Errorf("Unable to locate error stream within context")
	}
	err_s := ctx.Value(fetch.CKSourceError{}).(chan error)

	// Create the client for our transfer
	client := &http.Client{
		Timeout: this.transport.TLSHandshakeTimeout,
		Transport: this.transport,
		CheckRedirect: this.default_CheckRedirect,
	}

	// Determine the offset and filesize to read
	ro := int64(0)
	if ctx.Value(fetch.CKSourceOffset{}) != nil {
		ro = ctx.Value(fetch.CKSourceOffset{}).(int64)
	}

	rs := int64(-1)
	if ctx.Value(fetch.CKSourceSize{}) != nil {
		rs = ctx.Value(fetch.CKSourceSize{}).(int64)
	}

	// Determine the segment size to read the body with
	ss := int64(HTTP_DEFAULT_MTU)
	if ctx.Value(fetch.CKSourceSegment{}) != nil {
		ss = ctx.Value(fetch.CKSourceSegment{}).(int64)
	}

	// Specify the range header in our request regardless of whether or not
	// the server accepts it.
	if rs > 0 && rs > ro {
		this.request.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", ro, rs))
	}

	// Make our HTTP request
	response, err := client.Do(this.request.WithContext(ctx))
	if err != nil {
		return out, fmt.Errorf("Unable to make HTTP request to %s (%#v)", this.request.URL.String(), err)
	}

	// Check the status code
	if !(200 <= response.StatusCode && response.StatusCode < 300) {
		return out, fmt.Errorf("Received a non-successful HTTP status (%d)", response.StatusCode)
	}

	/* FIXME:
	If status that specifies that "bytes" is not a valid header, fix up request
	and resend it.
	*/

	// If we haven't peeked a response prior, then save this one.
	if this.response == nil {
		this.response = response
	}

	// Update the request size with what the http response returns if one wasn't
	// hardcoded
	if rs < 0 {
		rs = response.ContentLength
	}

	// FIXME: Check that the ContentLength matches the size requested

	// So far we should be good to go..
	out = make(chan []byte)

	go func(ctx context.Context, response http.Response, size, segment int64) {
		offset := int64(0)
		data := make([]byte, segment)

		// If ranges aren't accepted, then simulate it by discarding unnecessary
		// bytes...
		if response.Header.Get("Accept-Ranges") != "bytes" {
			ioffset := int64(0)
			for ioffset < ro {
				n, err := response.Body.Read(data)
				if err != nil {
					break
				}
				ioffset += int64(n)
			}
			// We read too much data, so some of it needs to be submitted instead
			// of discarding it like the prior data.
			if ro < ioffset {
				overread := ioffset - ro
				out <- data[ int64(len(data)) - overread:]
			}
		}

		// When this function is done, we shouldn't need the response anymore...
		defer response.Body.Close()

	Transfer:
		for size < 0 || offset <= size {
			// Read some data from the body
			n, err := response.Body.Read(data)
			if err != nil {
				// If we error out, make sure we submit it regardless
				out <- data[:n]
				offset += int64(n)

				// We hit an EOF, so we can safely leave..
				if err == io.EOF {
					// If we couldn't figure out the size before, then we can now.
					if size < 0 {
						size = offset
					}
					break Transfer
				}

				// Anything other than an EOF is unexpected, submit it to the error stream
				err_s <- fmt.Errorf("[%d/%d] Unexpected error : %#v", ro + offset, ro + size, err)

				// We should be able to leave now..
				break Transfer
			}

			// Figure out if we're still going or not
			select {
			case <-ctx.Done():
				err_s <- fmt.Errorf("[%d/%d] Cancellation was requested during transfer", ro + offset, ro + size)
				this.Done()
				return

			// Submit what we read to our output stream
			case out <- data[:n]:
				offset += int64(n)
			}
		}

		// Figure out what state we terminated in.
		// FIXME: There's likely a race here
		if offset < size {
			err_s <- fmt.Errorf("[%d/%d] Expected %d more bytes before terminating", ro + offset, ro + size, size - offset)
		} else if offset > size {
			err_s <- fmt.Errorf("[%d/%d] Received %d more bytes than expected", ro + offset, ro + size, offset - size)
		}
		this.Done()
	}(ctx, *response, rs, ss)

	return out, nil
}
