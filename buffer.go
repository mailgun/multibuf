// package multibuf implements buffer optimized for streaming large chunks of data,
// multiple reads and  optional partial buffering to disk.
package multibuf

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os"
)

// MultiBuf provides Read, Close, Seek and Size methods. In addition to that it supports WriterTo interface
// to provide efficient writing schemes, as functions like io.Copy use WriterTo when it's available.
type MultiBuf interface {
	io.Reader
	io.Seeker
	io.Closer
	io.WriterTo

	// Size calculates and returns the total size of the reader and not the length remaining.
	Size() (int64, error)
}

// MaxBytes, ignored if set to value >=, if request exceeds the specified limit, the reader will return error,
// by default buffer is not limited
func MaxBytes(m int64) optionSetter {
	return func(o *options) error {
		if m <= 0 {
			return fmt.Errorf("MaxSizeBytes should be > 0")
		}
		o.maxSizeBytes = m
		return nil
	}
}

// MemBytes specifies the largest buffer to hold in RAM before writing to disk, default is 1MB
func MemBytes(m int64) optionSetter {
	return func(o *options) error {
		if m < 0 {
			return fmt.Errorf("MemBytes should be >= 0")
		}
		o.maxSizeBytes = m
		return nil
	}
}

// New returns MultiBuf that can limit the size of the buffer and persist large buffers to disk.
// By default New returns unbound buffer that will read up to 1MB in RAM and will start buffering to disk
// It supports multiple functional optional arguments:
//
//    // Buffer up to 1MB in RAM and limit max buffer size to 20MB
//    multibuf.New(r, multibuf.MemBytes(1024 * 1024), multibuf.MaxBytes(1024 * 1024 * 20))
//
//
func New(input io.Reader, setters ...optionSetter) (MultiBuf, error) {
	o := options{
		memBytes:     DefaultMemBytes,
		maxSizeBytes: DefaultMaxSizeBytes,
	}

	for _, s := range setters {
		if err := s(&o); err != nil {
			return nil, err
		}
	}

	memReader := &io.LimitedReader{
		R: input,      // Read from this reader
		N: o.memBytes, // Maximum amount of data to read
	}
	readers := make([]io.ReadSeeker, 0, 2)

	buffer, err := ioutil.ReadAll(memReader)
	if err != nil {
		return nil, err
	}
	readers = append(readers, bytes.NewReader(buffer))

	var file *os.File
	// This means that we have exceeded all the memory capacity and we will start buffering the body to disk.
	totalBytes := int64(len(buffer))
	if memReader.N <= 0 {
		file, err = ioutil.TempFile("", "multibuf-")
		if err != nil {
			return nil, err
		}
		os.Remove(file.Name())

		readSrc := input
		if o.maxSizeBytes > 0 {
			readSrc = &maxReader{R: input, Max: o.maxSizeBytes - o.memBytes}
		}

		writtenBytes, err := io.Copy(file, readSrc)
		if err != nil {
			return nil, err
		}
		totalBytes += writtenBytes
		file.Seek(0, 0)
		readers = append(readers, file)
	}

	var cleanupFn cleanupFunc
	if file != nil {
		cleanupFn = func() error {
			file.Close()
			return nil
		}
	}
	return newBuf(totalBytes, cleanupFn, readers...), nil
}

// MaxSizeReachedError is returned when the maximum allowed buffer size is reached when reading
type MaxSizeReachedError struct {
	MaxSize int64
}

func (e *MaxSizeReachedError) Error() string {
	return fmt.Sprintf("Maximum size %d was reached", e)
}

const (
	DefaultMemBytes     = 1048576
	DefaultMaxSizeBytes = -1
	// Equivalent of bytes.MinRead used in ioutil.ReadAll
	DefaultBufferBytes = 512
)

// Constraints:
//  - Implements io.Reader
//  - Implements Seek(0, 0)
//	- Designed for Write once, Read many times.
type multiReaderSeek struct {
	length  int64
	readers []io.ReadSeeker
	mr      io.Reader
	cleanup cleanupFunc
}

type cleanupFunc func() error

func newBuf(length int64, cleanup cleanupFunc, readers ...io.ReadSeeker) *multiReaderSeek {
	converted := make([]io.Reader, len(readers))
	for i, r := range readers {
		// This conversion is safe as ReadSeeker includes Reader
		converted[i] = r.(io.Reader)
	}

	return &multiReaderSeek{
		length:  length,
		readers: readers,
		mr:      io.MultiReader(converted...),
		cleanup: cleanup,
	}
}

func (mr *multiReaderSeek) Close() (err error) {
	if mr.cleanup != nil {
		return mr.cleanup()
	}
	return nil
}

func (mr *multiReaderSeek) WriteTo(w io.Writer) (int64, error) {
	b := make([]byte, DefaultBufferBytes)
	var total int64
	for {
		n, err := mr.mr.Read(b)
		// Recommended way is to always handle non 0 reads despite the errors
		if n > 0 {
			nw, errw := w.Write(b[:n])
			total += int64(nw)
			// Write must return a non-nil error if it returns nw < n
			if nw != n || errw != nil {
				return total, errw
			}
		}
		if err != nil {
			if err == io.EOF {
				return total, nil
			}
			return total, err
		}
	}
}

func (mr *multiReaderSeek) Read(p []byte) (n int, err error) {
	return mr.mr.Read(p)
}

func (mr *multiReaderSeek) Size() (int64, error) {
	return mr.length, nil
}

func (mr *multiReaderSeek) Seek(offset int64, whence int) (int64, error) {
	// TODO: implement other whence
	// TODO: implement real offsets

	if whence != 0 {
		return 0, fmt.Errorf("multiReaderSeek: unsupported whence")
	}

	if offset != 0 {
		return 0, fmt.Errorf("multiReaderSeek: unsupported offset")
	}

	for _, seeker := range mr.readers {
		seeker.Seek(0, 0)
	}

	ior := make([]io.Reader, len(mr.readers))
	for i, arg := range mr.readers {
		ior[i] = arg.(io.Reader)
	}
	mr.mr = io.MultiReader(ior...)

	return 0, nil
}

type options struct {
	// MemBufferBytes sets up the size of the memory buffer for this request.
	// If the data size exceeds the limit, the remaining request part will be saved on the file system.
	memBytes int64

	maxSizeBytes int64
}

type optionSetter func(o *options) error

// MaxReader does not allow to read more than Max bytes and returns error if this limit has been exceeded.
type maxReader struct {
	R   io.Reader // underlying reader
	N   int64     // bytes read
	Max int64     // max bytes to read
}

func (r *maxReader) Read(p []byte) (int, error) {
	readBytes, err := r.R.Read(p)
	if err != nil && err != io.EOF {
		return readBytes, err
	}

	r.N += int64(readBytes)
	if r.N > r.Max {
		return readBytes, &MaxSizeReachedError{MaxSize: r.Max}
	}
	return readBytes, err
}
