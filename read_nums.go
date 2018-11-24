package dsvreader

import (
	"fmt"
	"math"
	"strconv"
)

// Int returns the next int column value from the current row.
func (tr *Reader) Int() int {
	if tr.err != nil {
		return 0
	}
	b, err := tr.nextCol()
	if err != nil {
		tr.setColError("cannot read `int`", err)
		return 0
	}

	n, err := strconv.Atoi(b2s(b))
	if err != nil {
		tr.setColError("cannot parse `int`", err)
		return 0
	}
	return n
}

// Uint returns the next uint column value from the current row.
func (tr *Reader) Uint() uint {
	if tr.err != nil {
		return 0
	}
	b, err := tr.nextCol()
	if err != nil {
		tr.setColError("cannot read `uint`", err)
		return 0
	}
	s := b2s(b)

	// Fast path - attempt to use Atoi
	n, err := strconv.Atoi(s)
	if err == nil && n >= 0 {
		return uint(n)
	}

	// Slow path - use ParseUint
	nu, err := strconv.ParseUint(s, 10, strconv.IntSize)
	if err != nil {
		tr.setColError("cannot parse `uint`", err)
		return 0
	}
	return uint(nu)
}

// Int32 returns the next int32 column value from the current row.
func (tr *Reader) Int32() int32 {
	if tr.err != nil {
		return 0
	}
	b, err := tr.nextCol()
	if err != nil {
		tr.setColError("cannot read `int32`", err)
		return 0
	}
	s := b2s(b)

	// Fast path - attempt to use Atoi
	n, err := strconv.Atoi(s)
	if err == nil && n >= math.MinInt32 && n <= math.MaxInt32 {
		return int32(n)
	}

	// Slow path - use ParseInt
	n32, err := strconv.ParseInt(s, 10, 32)
	if err != nil {
		tr.setColError("cannot parse `int32`", err)
		return 0
	}
	return int32(n32)
}

// Uint32 returns the next uint32 column value from the current row.
func (tr *Reader) Uint32() uint32 {
	if tr.err != nil {
		return 0
	}
	b, err := tr.nextCol()
	if err != nil {
		tr.setColError("cannot read `uint32`", err)
		return 0
	}
	s := b2s(b)

	// Fast path - attempt to use Atoi
	n, err := strconv.Atoi(s)
	if err == nil && n >= 0 && n <= math.MaxUint32 {
		return uint32(n)
	}

	// Slow path - use ParseUint
	n32, err := strconv.ParseUint(s, 10, 32)
	if err != nil {
		tr.setColError("cannot parse `uint32`", err)
		return 0
	}
	return uint32(n32)
}

// Int16 returns the next int16 column value from the current row.
func (tr *Reader) Int16() int16 {
	if tr.err != nil {
		return 0
	}
	b, err := tr.nextCol()
	if err != nil {
		tr.setColError("cannot read `int16`", err)
		return 0
	}
	n, err := strconv.Atoi(b2s(b))
	if err != nil {
		tr.setColError("cannot parse `int16`", err)
		return 0
	}
	if n < math.MinInt16 || n > math.MaxInt16 {
		tr.setColError("cannot parse `int16`", fmt.Errorf("out of range"))
		return 0
	}
	return int16(n)
}

// Uint16 returns the next uint16 column value from the current row.
func (tr *Reader) Uint16() uint16 {
	if tr.err != nil {
		return 0
	}
	b, err := tr.nextCol()
	if err != nil {
		tr.setColError("cannot read `uint16`", err)
		return 0
	}
	n, err := strconv.Atoi(b2s(b))
	if err != nil {
		tr.setColError("cannot parse `uint16`", err)
		return 0
	}
	if n < 0 {
		tr.setColError("cannot parse `uint16`", fmt.Errorf("invalid syntax"))
		return 0
	}
	if n > math.MaxUint16 {
		tr.setColError("cannot parse `uint16`", fmt.Errorf("out of range"))
		return 0
	}
	return uint16(n)
}

// Int8 returns the next int8 column value from the current row.
func (tr *Reader) Int8() int8 {
	if tr.err != nil {
		return 0
	}
	b, err := tr.nextCol()
	if err != nil {
		tr.setColError("cannot read `int8`", err)
		return 0
	}
	n, err := strconv.Atoi(b2s(b))
	if err != nil {
		tr.setColError("cannot parse `int8`", err)
		return 0
	}
	if n < math.MinInt8 || n > math.MaxInt8 {
		tr.setColError("cannot parse `int8`", fmt.Errorf("out of range"))
		return 0
	}
	return int8(n)
}

// Uint8 returns the next uint8 column value from the current row.
func (tr *Reader) Uint8() uint8 {
	if tr.err != nil {
		return 0
	}
	b, err := tr.nextCol()
	if err != nil {
		tr.setColError("cannot read `uint8`", err)
		return 0
	}
	n, err := strconv.Atoi(b2s(b))
	if err != nil {
		tr.setColError("cannot parse `uint8`", err)
		return 0
	}
	if n < 0 {
		tr.setColError("cannot parse `uint8`", fmt.Errorf("invalid syntax"))
		return 0
	}
	if n > math.MaxUint8 {
		tr.setColError("cannot parse `uint8`", fmt.Errorf("out of range"))
		return 0
	}
	return uint8(n)
}

// Int64 returns the next int64 column value from the current row.
func (tr *Reader) Int64() int64 {
	if tr.err != nil {
		return 0
	}
	b, err := tr.nextCol()
	if err != nil {
		tr.setColError("cannot read `int64`", err)
		return 0
	}
	s := b2s(b)

	// Fast path - attempt to use Atoi
	n, err := strconv.Atoi(s)
	if err == nil && int64(n) >= math.MinInt64 && int64(n) <= math.MaxInt64 {
		return int64(n)
	}

	// Slow path - use ParseInt
	n64, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		tr.setColError("cannot parse `int64`", err)
		return 0
	}
	return n64
}

// Uint64 returns the next uint64 column value from the current row.
func (tr *Reader) Uint64() uint64 {
	if tr.err != nil {
		return 0
	}
	b, err := tr.nextCol()
	if err != nil {
		tr.setColError("cannot read `uint64`", err)
		return 0
	}
	s := b2s(b)

	// Fast path - attempt to use Atoi
	n, err := strconv.Atoi(s)
	if err == nil && n >= 0 && uint64(n) <= math.MaxUint64 {
		return uint64(n)
	}

	// Slow path - use ParseUint
	n64, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		tr.setColError("cannot parse `uint64`", err)
		return 0
	}
	return n64
}

// Float32 returns the next float32 column value from the current row.
func (tr *Reader) Float32() float32 {
	if tr.err != nil {
		return 0
	}
	b, err := tr.nextCol()
	if err != nil {
		tr.setColError("cannot read `float32`", err)
		return 0
	}
	s := b2s(b)

	f32, err := strconv.ParseFloat(s, 32)
	if err != nil {
		tr.setColError("cannot parse `float32`", err)
		return 0
	}
	return float32(f32)
}

// Float64 returns the next float64 column value from the current row.
func (tr *Reader) Float64() float64 {
	if tr.err != nil {
		return 0
	}
	b, err := tr.nextCol()
	if err != nil {
		tr.setColError("cannot read `float64`", err)
		return 0
	}
	s := b2s(b)

	f64, err := strconv.ParseFloat(s, 64)
	if err != nil {
		tr.setColError("cannot parse `float64`", err)
		return 0
	}
	return f64
}
