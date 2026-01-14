package tron

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sort"

	"github.com/delaneyj/toolbelt/bytebufferpool"
)

// EncodeMapBranchNode encodes a map branch node.
func EncodeMapBranchNode(n MapBranchNode) ([]byte, error) {
	if n.Bitmap&0xFFFF0000 != 0 {
		return nil, fmt.Errorf("map branch bitmap high bits must be zero")
	}
	if len(n.Children) != popcount16(uint16(n.Bitmap)) {
		return nil, fmt.Errorf("children length does not match bitmap")
	}
	payloadLen := 4 + 4*len(n.Children)
	nodeLen, lenBytes, err := encodeNodeLength(payloadLen)
	if err != nil {
		return nil, err
	}
	tag := byte(TypeMap) | byte((lenBytes-1)<<4)
	out := make([]byte, nodeLen)
	out[0] = tag
	writeUint32LE(out[1:], lenBytes, nodeLen)
	p := 1 + lenBytes
	binary.LittleEndian.PutUint32(out[p:p+4], n.Bitmap)
	p += 4
	for _, child := range n.Children {
		binary.LittleEndian.PutUint32(out[p:p+4], child)
		p += 4
	}
	return out, nil
}

// EncodeMapLeafNode encodes a map leaf node.
func EncodeMapLeafNode(n MapLeafNode) ([]byte, error) {
	entries := make([]MapLeafEntry, len(n.Entries))
	copy(entries, n.Entries)
	sort.Slice(entries, func(i, j int) bool {
		return bytes.Compare(entries[i].Key, entries[j].Key) < 0
	})
	for i := 1; i < len(entries); i++ {
		if bytes.Equal(entries[i-1].Key, entries[i].Key) {
			return nil, fmt.Errorf("duplicate map key: %q", entries[i].Key)
		}
	}
	payloadLen := 8 * len(entries)
	nodeLen, lenBytes, err := encodeNodeLength(payloadLen)
	if err != nil {
		return nil, err
	}
	tag := byte(TypeMap) | byte((lenBytes-1)<<4) | 0x08
	out := make([]byte, nodeLen)
	out[0] = tag
	writeUint32LE(out[1:], lenBytes, nodeLen)
	p := 1 + lenBytes
	for _, entry := range entries {
		binary.LittleEndian.PutUint32(out[p:p+4], entry.KeyAddr)
		binary.LittleEndian.PutUint32(out[p+4:p+8], entry.ValueAddr)
		p += 8
	}
	return out, nil
}

func appendMapBranchNode(builder *Builder, n MapBranchNode) (uint32, error) {
	node, err := EncodeMapBranchNode(n)
	if err != nil {
		return 0, err
	}
	return builder.AppendNode(node), nil
}

func appendMapLeafNodeSorted(builder *Builder, entries []MapLeafEntry) (uint32, error) {
	entriesCopy := make([]MapLeafEntry, len(entries))
	copy(entriesCopy, entries)
	sort.Slice(entriesCopy, func(i, j int) bool {
		return bytes.Compare(entriesCopy[i].Key, entriesCopy[j].Key) < 0
	})
	for i := 1; i < len(entriesCopy); i++ {
		if bytes.Equal(entriesCopy[i-1].Key, entriesCopy[i].Key) {
			return 0, fmt.Errorf("duplicate map key: %q", entriesCopy[i].Key)
		}
	}

	for i := range entriesCopy {
		keyAddr, err := appendValueNode(builder, Value{Type: TypeTxt, Bytes: entriesCopy[i].Key})
		if err != nil {
			return 0, err
		}
		valAddr, err := valueAddress(builder, entriesCopy[i].Value)
		if err != nil {
			return 0, err
		}
		entriesCopy[i].KeyAddr = keyAddr
		entriesCopy[i].ValueAddr = valAddr
	}

	node, err := EncodeMapLeafNode(MapLeafNode{Entries: entriesCopy})
	if err != nil {
		return 0, err
	}
	return builder.AppendNode(node), nil
}

// EncodeArrayBranchNode encodes an array branch node.
func EncodeArrayBranchNode(n ArrayBranchNode) ([]byte, error) {
	if n.Shift%4 != 0 {
		return nil, fmt.Errorf("array branch shift must be multiple of 4")
	}
	if len(n.Children) != popcount16(n.Bitmap) {
		return nil, fmt.Errorf("children length does not match bitmap")
	}
	payloadLen := 1 + 2 + 4*len(n.Children)
	if n.Header.IsRoot {
		payloadLen += 4
	}
	nodeLen, lenBytes, err := encodeNodeLength(payloadLen)
	if err != nil {
		return nil, err
	}
	tag := byte(TypeArr) | byte((lenBytes-1)<<4)
	if n.Header.IsRoot {
		// R=0 for root
	} else {
		tag |= 0x40
	}
	out := make([]byte, nodeLen)
	out[0] = tag
	writeUint32LE(out[1:], lenBytes, nodeLen)
	p := 1 + lenBytes
	out[p] = n.Shift
	p++
	binary.LittleEndian.PutUint16(out[p:p+2], n.Bitmap)
	p += 2
	if n.Header.IsRoot {
		binary.LittleEndian.PutUint32(out[p:p+4], n.Length)
		p += 4
	}
	for _, child := range n.Children {
		binary.LittleEndian.PutUint32(out[p:p+4], child)
		p += 4
	}
	return out, nil
}

// EncodeArrayLeafNode encodes an array leaf node.
func EncodeArrayLeafNode(n ArrayLeafNode) ([]byte, error) {
	if n.Shift != 0 {
		return nil, fmt.Errorf("array leaf shift must be 0")
	}
	if len(n.ValueAddrs) != popcount16(n.Bitmap) {
		return nil, fmt.Errorf("values length does not match bitmap")
	}
	payloadLen := 1 + 2 + 4*len(n.ValueAddrs)
	if n.Header.IsRoot {
		payloadLen += 4
	}
	nodeLen, lenBytes, err := encodeNodeLength(payloadLen)
	if err != nil {
		return nil, err
	}
	tag := byte(TypeArr) | byte((lenBytes-1)<<4) | 0x08
	if n.Header.IsRoot {
		// R=0 for root
	} else {
		tag |= 0x40
	}
	out := make([]byte, nodeLen)
	out[0] = tag
	writeUint32LE(out[1:], lenBytes, nodeLen)
	p := 1 + lenBytes
	out[p] = n.Shift
	p++
	binary.LittleEndian.PutUint16(out[p:p+2], n.Bitmap)
	p += 2
	if n.Header.IsRoot {
		binary.LittleEndian.PutUint32(out[p:p+4], n.Length)
		p += 4
	}
	for _, addr := range n.ValueAddrs {
		binary.LittleEndian.PutUint32(out[p:p+4], addr)
		p += 4
	}
	return out, nil
}

func appendArrayBranchNode(builder *Builder, n ArrayBranchNode) (uint32, error) {
	node, err := EncodeArrayBranchNode(n)
	if err != nil {
		return 0, err
	}
	return builder.AppendNode(node), nil
}

func appendArrayLeafNode(builder *Builder, n ArrayLeafNode) (uint32, error) {
	node, err := EncodeArrayLeafNode(n)
	if err != nil {
		return 0, err
	}
	return builder.AppendNode(node), nil
}

func appendValueNode(builder *Builder, v Value) (uint32, error) {
	if v.Type == TypeArr || v.Type == TypeMap {
		if v.Offset == 0 {
			return 0, fmt.Errorf("missing address for arr/map value")
		}
		return v.Offset, nil
	}
	buf := bytebufferpool.Get()
	defer bytebufferpool.Put(buf)
	if err := encodeValueToBuffer(buf, v); err != nil {
		return 0, err
	}
	return builder.AppendNode(buf.Bytes()), nil
}

func valueAddress(builder *Builder, v Value) (uint32, error) {
	if v.Type == TypeArr || v.Type == TypeMap {
		if v.Offset == 0 {
			return 0, fmt.Errorf("missing address for arr/map value")
		}
		return v.Offset, nil
	}
	return appendValueNode(builder, v)
}

func encodeNodeLength(payloadLen int) (uint32, int, error) {
	if payloadLen < 0 {
		return 0, 0, fmt.Errorf("negative payload length")
	}
	for lenBytes := 1; lenBytes <= 4; lenBytes++ {
		nodeLen := uint32(1 + lenBytes + payloadLen)
		if nodeLen <= maxLenForBytes(lenBytes) {
			return nodeLen, lenBytes, nil
		}
	}
	return 0, 0, fmt.Errorf("node length too large")
}

func maxLenForBytes(n int) uint32 {
	return uint32((1 << (8 * n)) - 1)
}

func writeUint32LE(dst []byte, n int, value uint32) {
	for i := 0; i < n; i++ {
		dst[i] = byte(value >> (8 * i))
	}
}
