package tron

import (
	"encoding/binary"
	"fmt"
	"math"

	"github.com/delaneyj/toolbelt/bytebufferpool"
)

// Value represents a decoded TRON value record.
type Value struct {
	Type   ValueType
	Bool   bool
	I64    int64
	F64    float64
	Bytes  []byte
	Offset uint32
}

// DecodeValue decodes a scalar node from b and returns the value and bytes read.
func DecodeValue(b []byte) (Value, int, error) {
	if len(b) < 1 {
		return Value{}, 0, fmt.Errorf("value tag missing")
	}
	tag := b[0]
	typ := TypeFromTag(tag)

	switch typ {
	case TypeNil:
		if tag != TagNil {
			return Value{}, 0, fmt.Errorf("nil tag has non-zero low bits")
		}
		return Value{Type: TypeNil}, 1, nil
	case TypeBit:
		if (tag & 0xF6) != 0 {
			return Value{}, 0, fmt.Errorf("bit tag has invalid low bits")
		}
		return Value{Type: TypeBit, Bool: (tag & 0x08) != 0}, 1, nil
	case TypeI64:
		v, n, err := DecodeI64Value(b)
		if err != nil {
			return Value{}, 0, err
		}
		return Value{Type: TypeI64, I64: v}, n, nil
	case TypeF64:
		v, n, err := DecodeF64Value(b)
		if err != nil {
			return Value{}, 0, err
		}
		return Value{Type: TypeF64, F64: v}, n, nil
	case TypeTxt, TypeBin:
		length, n, err := decodeLength(tag, b[1:])
		if err != nil {
			return Value{}, 0, err
		}
		if len(b) < 1+n+int(length) {
			return Value{}, 0, fmt.Errorf("payload too short: need %d bytes", length)
		}
		payload := b[1+n : 1+n+int(length)]

		return Value{Type: typ, Bytes: payload}, 1 + n + int(length), nil
	case TypeArr, TypeMap:
		return Value{}, 0, fmt.Errorf("arr/map nodes must be decoded by address")
	}

	return Value{}, 0, fmt.Errorf("unknown value type %d", typ)
}

// DecodeValueAt decodes a value node at an absolute address in doc.
func DecodeValueAt(doc []byte, addr uint32) (Value, error) {
	if int(addr) < 0 || int(addr) >= len(doc) {
		return Value{}, fmt.Errorf("value address out of range: %d", addr)
	}
	tag := doc[addr]
	switch TypeFromTag(tag) {
	case TypeArr:
		return Value{Type: TypeArr, Offset: addr}, nil
	case TypeMap:
		return Value{Type: TypeMap, Offset: addr}, nil
	default:
		val, _, err := DecodeValue(doc[addr:])
		return val, err
	}
}

// DecodeI64Value decodes an i64 value record without copying payload bytes.
func DecodeI64Value(b []byte) (int64, int, error) {
	if len(b) < 1 {
		return 0, 0, fmt.Errorf("value tag missing")
	}
	tag := b[0]
	if TypeFromTag(tag) != TypeI64 {
		return 0, 0, fmt.Errorf("value is not i64")
	}
	if (tag & 0xF8) != 0 {
		return 0, 0, fmt.Errorf("i64 tag has non-zero high bits")
	}
	if len(b) < 1+8 {
		return 0, 0, fmt.Errorf("i64 payload too short")
	}
	v := int64(binary.LittleEndian.Uint64(b[1:9]))
	return v, 9, nil
}

// DecodeF64Value decodes an f64 value record without copying payload bytes.
func DecodeF64Value(b []byte) (float64, int, error) {
	if len(b) < 1 {
		return 0, 0, fmt.Errorf("value tag missing")
	}
	tag := b[0]
	if TypeFromTag(tag) != TypeF64 {
		return 0, 0, fmt.Errorf("value is not f64")
	}
	if (tag & 0xF8) != 0 {
		return 0, 0, fmt.Errorf("f64 tag has non-zero high bits")
	}
	if len(b) < 1+8 {
		return 0, 0, fmt.Errorf("f64 payload too short")
	}
	u := binary.LittleEndian.Uint64(b[1:9])
	return math.Float64frombits(u), 9, nil
}

// EncodeValue encodes v into a value record.
func EncodeValue(v Value) ([]byte, error) {
	buf := bytebufferpool.Get()
	defer bytebufferpool.Put(buf)
	if err := encodeValueToBuffer(buf, v); err != nil {
		return nil, err
	}
	out := append([]byte{}, buf.Bytes()...)
	return out, nil
}

func encodeBytesValue(typ ValueType, payload []byte) ([]byte, error) {
	if typ != TypeTxt && typ != TypeBin {
		return nil, fmt.Errorf("invalid type for bytes value: %d", typ)
	}
	buf := bytebufferpool.Get()
	defer bytebufferpool.Put(buf)
	if err := encodeBytesToBuffer(buf, typ, payload); err != nil {
		return nil, err
	}
	out := append([]byte{}, buf.Bytes()...)
	return out, nil
}

// decodeLength reads the length encoding for txt/bin/arr/map.
// It returns the payload length and number of length bytes read.
func decodeLength(tag byte, b []byte) (uint64, int, error) {
	if (tag & 0x08) != 0 {
		return uint64(tag>>4) & 0x0F, 0, nil
	}
	n := int(tag>>4) & 0x0F
	if n < 1 || n > 8 {
		return 0, 0, fmt.Errorf("invalid length-of-length: %d", n)
	}
	if len(b) < n {
		return 0, 0, fmt.Errorf("length-of-length bytes missing")
	}
	var length uint64
	for i := 0; i < n; i++ {
		length |= uint64(b[i]) << (8 * i)
	}
	return length, n, nil
}

func encodeValueToBuffer(buf *bytebufferpool.ByteBuffer, v Value) error {
	switch v.Type {
	case TypeNil:
		buf.WriteByte(TagNil)
		return nil
	case TypeBit:
		if v.Bool {
			buf.WriteByte(TagBitTrue)
		} else {
			buf.WriteByte(TagBitFalse)
		}
		return nil
	case TypeI64:
		buf.WriteByte(TagI64)
		var tmp [8]byte
		binary.LittleEndian.PutUint64(tmp[:], uint64(v.I64))
		buf.Write(tmp[:])
		return nil
	case TypeF64:
		buf.WriteByte(TagF64)
		var tmp [8]byte
		binary.LittleEndian.PutUint64(tmp[:], math.Float64bits(v.F64))
		buf.Write(tmp[:])
		return nil
	case TypeTxt, TypeBin:
		return encodeBytesToBuffer(buf, v.Type, v.Bytes)
	case TypeArr, TypeMap:
		return fmt.Errorf("arr/map nodes must be encoded as nodes")
	default:
		return fmt.Errorf("unknown value type %d", v.Type)
	}
}

func encodeTextStringToBuffer(buf *bytebufferpool.ByteBuffer, s string) error {
	if err := writeLength(buf, TypeTxt, len(s)); err != nil {
		return err
	}
	buf.WriteString(s)
	return nil
}

func encodeBytesToBuffer(buf *bytebufferpool.ByteBuffer, typ ValueType, payload []byte) error {
	if typ != TypeTxt && typ != TypeBin {
		return fmt.Errorf("invalid type for bytes value: %d", typ)
	}
	if err := writeLength(buf, typ, len(payload)); err != nil {
		return err
	}
	buf.Write(payload)
	return nil
}

func writeLength(buf *bytebufferpool.ByteBuffer, typ ValueType, length int) error {
	if length < 0 {
		return fmt.Errorf("negative length")
	}
	if length <= 15 {
		tag := byte(typ) | 0x08 | byte(length<<4)
		buf.WriteByte(tag)
		return nil
	}
	n := 1
	for max := 0xFF; length > max && n < 8; n++ {
		max = (max << 8) | 0xFF
	}
	if n > 8 {
		return fmt.Errorf("length too large")
	}
	tag := byte(typ) | byte(n<<4)
	buf.WriteByte(tag)
	var tmp [8]byte
	for i := 0; i < n; i++ {
		tmp[i] = byte(length >> (8 * i))
	}
	buf.Write(tmp[:n])
	return nil
}
