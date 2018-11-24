package dsvreader

import (
	"bytes"
	"fmt"
	"io"
	"unsafe"
)

// NewCSV returns new Reader that reads CSV data from r.
func NewCSV(r io.Reader) *Reader {
	var tr Reader
	tr.sep = ','
	tr.Reset(r)
	return &tr
}

// NewTSV returns new Reader that reads TSV data from r.
func NewTSV(r io.Reader) *Reader {
	var tr Reader
	tr.sep = '\t'
	tr.Reset(r)
	return &tr
}

// NewPSV returns new Reader that reads PSV data from r.
func NewPSV(r io.Reader) *Reader {
	var tr Reader
	tr.sep = '|'
	tr.Reset(r)
	return &tr
}

// NewCustom returns new Reader that reads arbitrary delimiter-separated data from r.
func NewCustom(sep byte, r io.Reader) *Reader {
	var tr Reader
	tr.sep = sep
	tr.Reset(r)
	return &tr
}

// Reader reads delimiter-separated data.
//
// Call NewCSV, NewTSV, NewPSV for creating new reader.
// Call Next before reading the next row.
//
// It is expected that columns are separated by delimiter while rows
// are separated by newlines.
type Reader struct {
	r    io.Reader
	rb   []byte
	rErr error
	rBuf [4 << 10]byte

	col int
	row int

	rowBuf  []byte
	b       []byte
	scratch []byte

	err          error
	sep          byte
	needUnescape bool
}

// Reset resets the reader for reading from r.
func (tr *Reader) Reset(r io.Reader) {
	tr.r = r
	tr.rb = nil
	tr.rErr = nil

	tr.col = 0
	tr.row = 0

	tr.rowBuf = nil
	tr.b = nil
	tr.scratch = tr.scratch[:0]

	tr.err = nil
	tr.needUnescape = false
}

// Error returns the last error.
func (tr *Reader) Error() error {
	if tr.err == io.EOF {
		return nil
	}
	return tr.err
}

// ResetError resets the current error, so the reader could proceed further.
func (tr *Reader) ResetError() {
	tr.err = nil
}

// HasCols returns true if the current row contains unread columns.
//
// An empty row doesn't contain columns.
//
// This function may be used if stream contains rows with different
// number of colums.
func (tr *Reader) HasCols() bool {
	return len(tr.rowBuf) > 0 && tr.b != nil
}

// Next advances to the next row.
//
// Returns true if the next row does exist.
//
// Next must be called after reading all the columns on the previous row.
// Check Error after Next returns false.
//
// HasCols may be used for reading rows with variable number of columns.
func (tr *Reader) Next() bool {
	if tr.err != nil {
		return false
	}
	if tr.HasCols() {
		tr.err = fmt.Errorf("row #%d %q contains unread columns: %q", tr.row, tr.rowBuf, tr.b)
		return false
	}

	tr.row++
	tr.col = 0
	tr.rowBuf = nil

	for {
		if len(tr.rb) == 0 {
			// Read buffer is empty. Attempt to fill it.
			if tr.rErr != nil {
				tr.err = tr.rErr
				if tr.err != io.EOF {
					tr.err = fmt.Errorf("cannot read row #%d: %s", tr.row, tr.err)
				} else if len(tr.scratch) > 0 {
					tr.err = fmt.Errorf("cannot find newline at the end of row #%d; row: %q", tr.row, tr.scratch)
				}
				return false
			}
			n, err := tr.r.Read(tr.rBuf[:])
			tr.rb = tr.rBuf[:n]
			tr.needUnescape = (bytes.IndexByte(tr.rb, '\\') >= 0)
			tr.rErr = err
		}

		// Search for the end of the current row.
		n := bytes.IndexByte(tr.rb, '\n')
		if n >= 0 {
			// Fast path: the row has been found.
			b := tr.rb[:n]
			tr.rb = tr.rb[n+1:]
			if len(tr.scratch) > 0 {
				tr.scratch = append(tr.scratch, b...)
				b = tr.scratch
				tr.scratch = tr.scratch[:0]
			}
			tr.rowBuf = b
			tr.b = tr.rowBuf
			return true
		}

		// Slow path: cannot find the end of row.
		// Append tr.rb to tr.scratch and repeat.
		tr.scratch = append(tr.scratch, tr.rb...)
		tr.rb = nil
	}
}

// SkipCol skips the next column from the current row.
func (tr *Reader) SkipCol() {
	if tr.err != nil {
		return
	}
	_, err := tr.nextCol()
	if err != nil {
		tr.setColError("cannot skip column", err)
	}
}

// Bytes returns the next bytes column value from the current row.
//
// The returned value is valid until the next call to Reader.
func (tr *Reader) Bytes() []byte {
	if tr.err != nil {
		return nil
	}
	b, err := tr.nextCol()
	if err != nil {
		tr.setColError("cannot read `bytes`", err)
		return nil
	}

	if !tr.needUnescape {
		// Fast path - nothing to unescape.
		return b
	}

	// Unescape b
	n := bytes.IndexByte(b, '\\')
	if n < 0 {
		// Nothing to unescape in the current column.
		return b
	}

	// Slow path - in-place unescaping compatible with ClickHouse.
	n++
	d := b[:n]
	b = b[n:]
	for len(b) > 0 {
		switch b[0] {
		case 'b':
			d[len(d)-1] = '\b'
		case 'f':
			d[len(d)-1] = '\f'
		case 'r':
			d[len(d)-1] = '\r'
		case 'n':
			d[len(d)-1] = '\n'
		case 't':
			d[len(d)-1] = '\t'
		case '0':
			d[len(d)-1] = 0
		case '\'':
			d[len(d)-1] = '\''
		case '\\':
			d[len(d)-1] = '\\'
		default:
			d[len(d)-1] = b[0]
		}

		b = b[1:]
		n = bytes.IndexByte(b, '\\')
		if n < 0 {
			d = append(d, b...)
			break
		}
		n++
		d = append(d, b[:n]...)
		b = b[n:]
	}
	return d
}

// String returns the next string column value from the current row.
//
// String allocates memory. Use Bytes to avoid memory allocations.
func (tr *Reader) String() string {
	return string(tr.Bytes())
}

func (tr *Reader) nextCol() ([]byte, error) {
	if tr.row == 0 {
		return nil, fmt.Errorf("missing Next call")
	}

	tr.col++
	if tr.b == nil {
		return nil, fmt.Errorf("no more columns")
	}

	n := bytes.IndexByte(tr.b, tr.sep)
	if n < 0 {
		// last column
		b := tr.b
		tr.b = nil
		return b, nil
	}

	b := tr.b[:n]
	tr.b = tr.b[n+1:]
	return b, nil
}

func (tr *Reader) setColError(msg string, err error) {
	tr.err = fmt.Errorf("%s at row #%d, col #%d %q: %s", msg, tr.row, tr.col, tr.rowBuf, err)
}

func b2s(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}
