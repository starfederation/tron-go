package tron

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

// HeaderMagic is the 4-byte header for TRON documents.
var HeaderMagic = [4]byte{'T', 'R', 'O', 'N'}

const TrailerSize = 8

// Trailer describes the root record trailer at the end of a tree document.
type Trailer struct {
	RootOffset     uint32
	PrevRootOffset uint32
}

// ParseTrailer parses the last 8 bytes of a document footer.
func ParseTrailer(b []byte) (Trailer, error) {
	if len(b) < len(HeaderMagic)+TrailerSize {
		return Trailer{}, fmt.Errorf("document too short: %d", len(b))
	}
	if !bytes.Equal(b[:len(HeaderMagic)], HeaderMagic[:]) {
		return Trailer{}, fmt.Errorf("missing TRON header magic")
	}
	start := len(b) - TrailerSize
	return Trailer{
		RootOffset:     binary.LittleEndian.Uint32(b[start : start+4]),
		PrevRootOffset: binary.LittleEndian.Uint32(b[start+4 : start+8]),
	}, nil
}

// AppendTrailer appends a trailer to dst and returns the extended slice.
func AppendTrailer(dst []byte, t Trailer) []byte {
	buf := make([]byte, TrailerSize)
	binary.LittleEndian.PutUint32(buf[0:4], t.RootOffset)
	binary.LittleEndian.PutUint32(buf[4:8], t.PrevRootOffset)
	return append(dst, buf...)
}
