package tron

import (
	"encoding/binary"
	"fmt"
)

// NodeKind indicates whether a node is a branch or leaf.
type NodeKind uint8

const (
	NodeBranch NodeKind = 0
	NodeLeaf   NodeKind = 1
)

// KeyType indicates whether a node is for arrays or maps.
type KeyType uint8

const (
	KeyArr KeyType = 0
	KeyMap KeyType = 1
)

// NodeHeader is the parsed header metadata for a node.
type NodeHeader struct {
	Type     ValueType
	NodeLen  uint32
	Kind     NodeKind
	KeyType  KeyType
	IsRoot   bool
	LenBytes int
}

// ParseNodeHeader parses a node header from b and returns its metadata.
func ParseNodeHeader(b []byte) (NodeHeader, error) {
	if len(b) < 1 {
		return NodeHeader{}, fmt.Errorf("node tag missing")
	}
	tag := b[0]
	typ := TypeFromTag(tag)

	switch typ {
	case TypeNil:
		if tag != TagNil {
			return NodeHeader{}, fmt.Errorf("nil tag has non-zero bits")
		}
		return NodeHeader{Type: TypeNil, NodeLen: 1}, nil
	case TypeBit:
		if (tag & 0xF6) != 0 {
			return NodeHeader{}, fmt.Errorf("bit tag has invalid bits")
		}
		return NodeHeader{Type: TypeBit, NodeLen: 1}, nil
	case TypeI64:
		if (tag & 0xF8) != 0 {
			return NodeHeader{}, fmt.Errorf("i64 tag has invalid bits")
		}
		return NodeHeader{Type: TypeI64, NodeLen: 9}, nil
	case TypeF64:
		if (tag & 0xF8) != 0 {
			return NodeHeader{}, fmt.Errorf("f64 tag has invalid bits")
		}
		return NodeHeader{Type: TypeF64, NodeLen: 9}, nil
	case TypeTxt, TypeBin:
		length, n, err := decodeLength(tag, b[1:])
		if err != nil {
			return NodeHeader{}, err
		}
		nodeLen := uint32(1 + n + int(length))
		return NodeHeader{Type: typ, NodeLen: nodeLen}, nil
	case TypeArr:
		if (tag & 0x80) != 0 {
			return NodeHeader{}, fmt.Errorf("arr tag has invalid high bit")
		}
		lenBytes := int((tag>>4)&0x03) + 1
		if len(b) < 1+lenBytes {
			return NodeHeader{}, fmt.Errorf("arr node length truncated")
		}
		nodeLen := readUint32LE(b[1:], lenBytes)
		if nodeLen < uint32(1+lenBytes) {
			return NodeHeader{}, fmt.Errorf("invalid arr node length: %d", nodeLen)
		}
		return NodeHeader{
			Type:     TypeArr,
			NodeLen:  nodeLen,
			Kind:     NodeKind((tag >> 3) & 0x01),
			KeyType:  KeyArr,
			IsRoot:   (tag & 0x40) == 0,
			LenBytes: lenBytes,
		}, nil
	case TypeMap:
		if (tag & 0xC0) != 0 {
			return NodeHeader{}, fmt.Errorf("map tag has invalid high bits")
		}
		lenBytes := int((tag>>4)&0x03) + 1
		if len(b) < 1+lenBytes {
			return NodeHeader{}, fmt.Errorf("map node length truncated")
		}
		nodeLen := readUint32LE(b[1:], lenBytes)
		if nodeLen < uint32(1+lenBytes) {
			return NodeHeader{}, fmt.Errorf("invalid map node length: %d", nodeLen)
		}
		return NodeHeader{
			Type:     TypeMap,
			NodeLen:  nodeLen,
			Kind:     NodeKind((tag >> 3) & 0x01),
			KeyType:  KeyMap,
			LenBytes: lenBytes,
		}, nil
	default:
		return NodeHeader{}, fmt.Errorf("unknown node type %d", typ)
	}
}

// MapBranchNode represents a map branch node.
type MapBranchNode struct {
	Header   NodeHeader
	Bitmap   uint32
	Children []uint32
}

// MapLeafEntry is a single map leaf entry.
type MapLeafEntry struct {
	KeyAddr   uint32
	ValueAddr uint32
	Key       []byte
	Value     Value
}

// MapLeafNode represents a map leaf node.
type MapLeafNode struct {
	Header  NodeHeader
	Entries []MapLeafEntry
}

// ArrayBranchNode represents an array branch node.
type ArrayBranchNode struct {
	Header   NodeHeader
	Shift    uint8
	Bitmap   uint16
	Length   uint32
	Children []uint32
}

// ArrayLeafNode represents an array leaf node.
type ArrayLeafNode struct {
	Header     NodeHeader
	Shift      uint8
	Bitmap     uint16
	Length     uint32
	ValueAddrs []uint32
}

// ParseMapBranchNode parses a map branch node from b.
func ParseMapBranchNode(b []byte) (MapBranchNode, error) {
	h, err := ParseNodeHeader(b)
	if err != nil {
		return MapBranchNode{}, err
	}
	if h.KeyType != KeyMap || h.Kind != NodeBranch {
		return MapBranchNode{}, fmt.Errorf("node is not a map branch")
	}
	if len(b) < int(h.NodeLen) {
		return MapBranchNode{}, fmt.Errorf("node truncated: have %d need %d", len(b), h.NodeLen)
	}
	p := 1 + h.LenBytes
	if int(h.NodeLen) < p+4 {
		return MapBranchNode{}, fmt.Errorf("map branch node too small: %d", h.NodeLen)
	}
	bitmap := binary.LittleEndian.Uint32(b[p : p+4])
	if bitmap&0xFFFF0000 != 0 {
		return MapBranchNode{}, fmt.Errorf("map branch bitmap high bits must be zero")
	}
	p += 4
	entryCount := popcount16(uint16(bitmap))
	children := getUint32Slice(entryCount)
	for i := 0; i < entryCount; i++ {
		if p+4 > int(h.NodeLen) {
			putUint32Slice(children)
			return MapBranchNode{}, fmt.Errorf("child address truncated")
		}
		off := binary.LittleEndian.Uint32(b[p : p+4])
		p += 4
		children[i] = off
	}
	return MapBranchNode{
		Header:   h,
		Bitmap:   bitmap,
		Children: children,
	}, nil
}

// ParseMapLeafNode parses a map leaf node from b, decoding key/value nodes via doc.
func ParseMapLeafNode(doc []byte, b []byte) (MapLeafNode, error) {
	h, err := ParseNodeHeader(b)
	if err != nil {
		return MapLeafNode{}, err
	}
	if h.KeyType != KeyMap || h.Kind != NodeLeaf {
		return MapLeafNode{}, fmt.Errorf("node is not a map leaf")
	}
	if len(b) < int(h.NodeLen) {
		return MapLeafNode{}, fmt.Errorf("node truncated: have %d need %d", len(b), h.NodeLen)
	}
	p := 1 + h.LenBytes
	payloadLen := int(h.NodeLen) - p
	if payloadLen%8 != 0 {
		return MapLeafNode{}, fmt.Errorf("map leaf payload misaligned")
	}
	entryCount := payloadLen / 8
	entries := getEntrySlice(entryCount)
	var prevKey []byte
	for i := 0; i < entryCount; i++ {
		if p+8 > int(h.NodeLen) {
			putEntrySlice(entries)
			return MapLeafNode{}, fmt.Errorf("map leaf entry truncated")
		}
		keyAddr := binary.LittleEndian.Uint32(b[p : p+4])
		valAddr := binary.LittleEndian.Uint32(b[p+4 : p+8])
		p += 8

		keyVal, err := DecodeValueAt(doc, keyAddr)
		if err != nil {
			putEntrySlice(entries)
			return MapLeafNode{}, fmt.Errorf("key decode failed: %w", err)
		}
		if keyVal.Type != TypeTxt {
			putEntrySlice(entries)
			return MapLeafNode{}, fmt.Errorf("map leaf key must be txt")
		}
		if i > 0 {
			if cmp := bytesCompare(prevKey, keyVal.Bytes); cmp >= 0 {
				putEntrySlice(entries)
				return MapLeafNode{}, fmt.Errorf("map leaf keys must be sorted and unique")
			}
		}
		prevKey = keyVal.Bytes
		val, err := DecodeValueAt(doc, valAddr)
		if err != nil {
			putEntrySlice(entries)
			return MapLeafNode{}, fmt.Errorf("value decode failed: %w", err)
		}
		entries[i] = MapLeafEntry{
			KeyAddr:   keyAddr,
			ValueAddr: valAddr,
			Key:       keyVal.Bytes,
			Value:     val,
		}
	}
	return MapLeafNode{
		Header:  h,
		Entries: entries,
	}, nil
}

// ParseArrayBranchNode parses an array branch node from b.
func ParseArrayBranchNode(b []byte) (ArrayBranchNode, error) {
	h, err := ParseNodeHeader(b)
	if err != nil {
		return ArrayBranchNode{}, err
	}
	if h.KeyType != KeyArr || h.Kind != NodeBranch {
		return ArrayBranchNode{}, fmt.Errorf("node is not an array branch")
	}
	if len(b) < int(h.NodeLen) {
		return ArrayBranchNode{}, fmt.Errorf("node truncated: have %d need %d", len(b), h.NodeLen)
	}
	p := 1 + h.LenBytes
	if int(h.NodeLen) < p+3 {
		return ArrayBranchNode{}, fmt.Errorf("array branch node too small: %d", h.NodeLen)
	}
	shift := b[p]
	p++
	if shift%4 != 0 {
		return ArrayBranchNode{}, fmt.Errorf("array branch shift must be multiple of 4")
	}
	bitmap := binary.LittleEndian.Uint16(b[p : p+2])
	p += 2
	length := uint32(0)
	if h.IsRoot {
		if p+4 > int(h.NodeLen) {
			return ArrayBranchNode{}, fmt.Errorf("array branch root length truncated")
		}
		length = binary.LittleEndian.Uint32(b[p : p+4])
		p += 4
	}
	entryCount := popcount16(bitmap)
	children := getUint32Slice(entryCount)
	for i := 0; i < entryCount; i++ {
		if p+4 > int(h.NodeLen) {
			putUint32Slice(children)
			return ArrayBranchNode{}, fmt.Errorf("child address truncated")
		}
		off := binary.LittleEndian.Uint32(b[p : p+4])
		p += 4
		children[i] = off
	}
	return ArrayBranchNode{
		Header:   h,
		Shift:    shift,
		Bitmap:   bitmap,
		Length:   length,
		Children: children,
	}, nil
}

// ParseArrayLeafNode parses an array leaf node from b.
func ParseArrayLeafNode(b []byte) (ArrayLeafNode, error) {
	h, err := ParseNodeHeader(b)
	if err != nil {
		return ArrayLeafNode{}, err
	}
	if h.KeyType != KeyArr || h.Kind != NodeLeaf {
		return ArrayLeafNode{}, fmt.Errorf("node is not an array leaf")
	}
	if len(b) < int(h.NodeLen) {
		return ArrayLeafNode{}, fmt.Errorf("node truncated: have %d need %d", len(b), h.NodeLen)
	}
	p := 1 + h.LenBytes
	if int(h.NodeLen) < p+3 {
		return ArrayLeafNode{}, fmt.Errorf("array leaf node too small: %d", h.NodeLen)
	}
	shift := b[p]
	p++
	if shift != 0 {
		return ArrayLeafNode{}, fmt.Errorf("array leaf shift must be 0")
	}
	bitmap := binary.LittleEndian.Uint16(b[p : p+2])
	p += 2
	length := uint32(0)
	if h.IsRoot {
		if p+4 > int(h.NodeLen) {
			return ArrayLeafNode{}, fmt.Errorf("array leaf root length truncated")
		}
		length = binary.LittleEndian.Uint32(b[p : p+4])
		p += 4
	}
	entryCount := popcount16(bitmap)
	values := getUint32Slice(entryCount)
	for i := 0; i < entryCount; i++ {
		if p+4 > int(h.NodeLen) {
			putUint32Slice(values)
			return ArrayLeafNode{}, fmt.Errorf("value address truncated")
		}
		off := binary.LittleEndian.Uint32(b[p : p+4])
		p += 4
		values[i] = off
	}
	return ArrayLeafNode{
		Header:     h,
		Shift:      shift,
		Bitmap:     bitmap,
		Length:     length,
		ValueAddrs: values,
	}, nil
}

func popcount16(x uint16) int {
	// simple popcount for 16 bits
	x = x - ((x >> 1) & 0x5555)
	x = (x & 0x3333) + ((x >> 2) & 0x3333)
	x = (x + (x >> 4)) & 0x0F0F
	x = x + (x >> 8)
	return int(x & 0x1F)
}

func bytesCompare(a, b []byte) int {
	min := len(a)
	if len(b) < min {
		min = len(b)
	}
	for i := 0; i < min; i++ {
		if a[i] == b[i] {
			continue
		}
		if a[i] < b[i] {
			return -1
		}
		return 1
	}
	switch {
	case len(a) < len(b):
		return -1
	case len(a) > len(b):
		return 1
	default:
		return 0
	}
}

func readUint32LE(b []byte, n int) uint32 {
	var out uint32
	for i := 0; i < n; i++ {
		out |= uint32(b[i]) << (8 * i)
	}
	return out
}

func releaseMapBranchNode(n *MapBranchNode) {
	putUint32Slice(n.Children)
	n.Children = nil
}

func releaseMapLeafNode(n *MapLeafNode) {
	putEntrySlice(n.Entries)
	n.Entries = nil
}

func releaseArrayBranchNode(n *ArrayBranchNode) {
	putUint32Slice(n.Children)
	n.Children = nil
}

func releaseArrayLeafNode(n *ArrayLeafNode) {
	putUint32Slice(n.ValueAddrs)
	n.ValueAddrs = nil
}

// ReleaseMapBranchNode releases pooled slices held by a map branch node.
func ReleaseMapBranchNode(n *MapBranchNode) {
	releaseMapBranchNode(n)
}

// ReleaseMapLeafNode releases pooled slices held by a map leaf node.
func ReleaseMapLeafNode(n *MapLeafNode) {
	releaseMapLeafNode(n)
}

// ReleaseArrayBranchNode releases pooled slices held by an array branch node.
func ReleaseArrayBranchNode(n *ArrayBranchNode) {
	releaseArrayBranchNode(n)
}

// ReleaseArrayLeafNode releases pooled slices held by an array leaf node.
func ReleaseArrayLeafNode(n *ArrayLeafNode) {
	releaseArrayLeafNode(n)
}
