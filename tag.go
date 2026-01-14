package tron

// ValueType represents the high 3-bit tag type.
type ValueType uint8

const (
	TypeNil ValueType = iota
	TypeBit
	TypeI64
	TypeF64
	TypeTxt
	TypeBin
	TypeArr
	TypeMap
)

const (
	TagNil      byte = 0x00 // 00000000
	TagBitFalse byte = 0x01 // 00000001
	TagBitTrue  byte = 0x09 // 00001001 (bit set in bit3)
	TagI64      byte = 0x02 // 00000010
	TagF64      byte = 0x03 // 00000011
)

// TypeFromTag returns the ValueType encoded in the low 3 bits.
func TypeFromTag(tag byte) ValueType {
	return ValueType(tag & 0x07)
}

// IsPacked reports whether the tag uses inline packing (txt/bin/arr/map).
func IsPacked(tag byte) bool {
	return (tag & 0x08) != 0
}
