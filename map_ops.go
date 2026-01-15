package tron

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
)

func mapGet(doc []byte, off uint32, key []byte, depth int) (Value, bool, error) {
	return mapGetHashed(doc, off, key, XXH32(key, 0), depth)
}

func mapGetHashed(doc []byte, off uint32, key []byte, hash uint32, depth int) (Value, bool, error) {
	for {
		h, node, err := NodeSliceAt(doc, off)
		if err != nil {
			return Value{}, false, err
		}
		if h.KeyType != KeyMap {
			return Value{}, false, fmt.Errorf("node is not a map")
		}
		if h.Kind == NodeLeaf {
			return mapLeafGetValue(doc, h, node, key)
		}

		p := 1 + h.LenBytes
		if int(h.NodeLen) < p+4 {
			return Value{}, false, fmt.Errorf("map branch node too small: %d", h.NodeLen)
		}
		bitmap := binary.LittleEndian.Uint32(node[p : p+4])
		if bitmap&0xFFFF0000 != 0 {
			return Value{}, false, fmt.Errorf("map branch bitmap high bits must be zero")
		}
		slot := uint8((hash >> (depth * 4)) & hamtMask)
		if ((bitmap >> slot) & 1) == 0 {
			return Value{}, false, nil
		}
		mask := uint32((uint32(1) << slot) - 1)
		idx := popcount16(uint16(bitmap & mask))
		childPos := p + 4 + (idx * 4)
		if childPos+4 > int(h.NodeLen) {
			return Value{}, false, fmt.Errorf("child address truncated")
		}
		off = binary.LittleEndian.Uint32(node[childPos : childPos+4])
		depth++
	}
}

func mapLeafGetValue(doc []byte, h NodeHeader, node []byte, key []byte) (Value, bool, error) {
	p := 1 + h.LenBytes
	payloadLen := int(h.NodeLen) - p
	if payloadLen%8 != 0 {
		return Value{}, false, fmt.Errorf("map leaf payload misaligned")
	}
	entryCount := payloadLen / 8
	for i := 0; i < entryCount; i++ {
		if p+8 > int(h.NodeLen) {
			return Value{}, false, fmt.Errorf("map leaf entry truncated")
		}
		keyAddr := binary.LittleEndian.Uint32(node[p : p+4])
		valAddr := binary.LittleEndian.Uint32(node[p+4 : p+8])
		p += 8

		keyVal, err := DecodeValueAt(doc, keyAddr)
		if err != nil {
			return Value{}, false, fmt.Errorf("key decode failed: %w", err)
		}
		if keyVal.Type != TypeTxt {
			return Value{}, false, fmt.Errorf("map leaf key must be txt")
		}
		cmp := bytesCompare(keyVal.Bytes, key)
		if cmp == 0 {
			val, err := DecodeValueAt(doc, valAddr)
			if err != nil {
				return Value{}, false, fmt.Errorf("value decode failed: %w", err)
			}
			return val, true, nil
		}
		if cmp > 0 {
			return Value{}, false, nil
		}
	}
	return Value{}, false, nil
}

// MapGet returns the value for key under the map node at rootOff.
func MapGet(doc []byte, rootOff uint32, key []byte) (Value, bool, error) {
	return mapGet(doc, rootOff, key, 0)
}

// MapGetHashed returns the value for key under the map node at rootOff using a precomputed hash.
func MapGetHashed(doc []byte, rootOff uint32, key []byte, hash uint32) (Value, bool, error) {
	return mapGetHashed(doc, rootOff, key, hash, 0)
}

// MapHas reports whether key exists under the map node at rootOff.
func MapHas(doc []byte, rootOff uint32, key []byte) (bool, error) {
	return mapHas(doc, rootOff, key, 0)
}

// MapSetNode updates a map node at rootOff and returns the new root offset.
func MapSetNode(builder *Builder, rootOff uint32, key []byte, val Value) (uint32, bool, error) {
	if builder == nil {
		return 0, false, fmt.Errorf("nil builder")
	}
	return mapSet(builder.buf, rootOff, key, val, 0, builder)
}

// MapSetNodeHashed updates a map node at rootOff using a precomputed hash.
func MapSetNodeHashed(builder *Builder, rootOff uint32, key []byte, hash uint32, val Value) (uint32, bool, error) {
	if builder == nil {
		return 0, false, fmt.Errorf("nil builder")
	}
	return mapSetHashed(builder.buf, rootOff, key, val, hash, 0, builder)
}

// MapDelNode deletes a key from a map node at rootOff and returns the new root offset.
func MapDelNode(builder *Builder, rootOff uint32, key []byte) (uint32, bool, error) {
	if builder == nil {
		return 0, false, fmt.Errorf("nil builder")
	}
	return mapDelete(builder.buf, rootOff, key, 0, builder)
}

// EmptyMapRoot returns a new empty map root node offset.
func EmptyMapRoot(builder *Builder) (uint32, error) {
	if builder == nil {
		return 0, fmt.Errorf("nil builder")
	}
	return appendMapLeafNodeSorted(builder, nil)
}

func mapHas(doc []byte, off uint32, key []byte, depth int) (bool, error) {
	_, ok, err := mapGet(doc, off, key, depth)
	return ok, err
}

func mapSet(doc []byte, off uint32, key []byte, val Value, depth int, builder *Builder) (uint32, bool, error) {
	return mapSetHashed(doc, off, key, val, XXH32(key, 0), depth, builder)
}

func mapSetHashed(doc []byte, off uint32, key []byte, val Value, hash uint32, depth int, builder *Builder) (uint32, bool, error) {
	if depth > maxDepth32 {
		return 0, false, fmt.Errorf("map depth exceeds max")
	}
	h, node, err := NodeSliceAt(doc, off)
	if err != nil {
		return 0, false, err
	}
	if h.KeyType != KeyMap {
		return 0, false, fmt.Errorf("node is not a map")
	}
	if h.Kind == NodeLeaf {
		return mapSetLeaf(doc, off, h, node, key, val, depth, builder)
	}
	return mapSetBranch(doc, off, h, node, key, val, hash, depth, builder)
}

func mapSetLeaf(doc []byte, off uint32, h NodeHeader, node []byte, key []byte, val Value, depth int, builder *Builder) (uint32, bool, error) {
	p := 1 + h.LenBytes
	payloadLen := int(h.NodeLen) - p
	if payloadLen%8 != 0 {
		return 0, false, fmt.Errorf("map leaf payload misaligned")
	}
	entryCount := payloadLen / 8
	entries := getEntrySlice(entryCount)
	var prevKey []byte
	foundIdx := -1
	for i := 0; i < entryCount; i++ {
		if p+8 > int(h.NodeLen) {
			putEntrySlice(entries)
			return 0, false, fmt.Errorf("map leaf entry truncated")
		}
		keyAddr := binary.LittleEndian.Uint32(node[p : p+4])
		valAddr := binary.LittleEndian.Uint32(node[p+4 : p+8])
		p += 8

		keyVal, err := DecodeValueAt(doc, keyAddr)
		if err != nil {
			putEntrySlice(entries)
			return 0, false, fmt.Errorf("key decode failed: %w", err)
		}
		if keyVal.Type != TypeTxt {
			putEntrySlice(entries)
			return 0, false, fmt.Errorf("map leaf key must be txt")
		}
		if i > 0 {
			if cmp := bytesCompare(prevKey, keyVal.Bytes); cmp >= 0 {
				putEntrySlice(entries)
				return 0, false, fmt.Errorf("map leaf keys must be sorted and unique")
			}
		}
		prevKey = keyVal.Bytes

		entryVal, err := DecodeValueAt(doc, valAddr)
		if err != nil {
			putEntrySlice(entries)
			return 0, false, fmt.Errorf("value decode failed: %w", err)
		}
		entries[i] = MapLeafEntry{
			KeyAddr:   keyAddr,
			ValueAddr: valAddr,
			Key:       keyVal.Bytes,
			Value:     entryVal,
		}
		if foundIdx == -1 && bytes.Equal(keyVal.Bytes, key) {
			foundIdx = i
		}
	}
	if foundIdx >= 0 && valueEqual(entries[foundIdx].Value, val) {
		putEntrySlice(entries)
		return off, false, nil
	}
	if foundIdx >= 0 {
		entries[foundIdx].Value = val
		newNode := buildMapNodeFromEntries(entries, depth, nil)
		newOff, err := encodeMapNode(builder, newNode, nil)
		putEntrySlice(entries)
		if err != nil {
			return 0, false, err
		}
		return newOff, true, nil
	}
	if cap(entries) > len(entries) {
		entries = append(entries, MapLeafEntry{Key: key, Value: val})
	} else {
		newEntries := getEntrySlice(entryCount + 1)
		copy(newEntries, entries)
		newEntries[entryCount] = MapLeafEntry{Key: key, Value: val}
		putEntrySlice(entries)
		entries = newEntries
	}
	newNode := buildMapNodeFromEntries(entries, depth, nil)
	newOff, err := encodeMapNode(builder, newNode, nil)
	putEntrySlice(entries)
	if err != nil {
		return 0, false, err
	}
	return newOff, true, nil
}

func mapSetBranch(doc []byte, off uint32, h NodeHeader, node []byte, key []byte, val Value, hash uint32, depth int, builder *Builder) (uint32, bool, error) {
	p := 1 + h.LenBytes
	if int(h.NodeLen) < p+4 {
		return 0, false, fmt.Errorf("map branch node too small: %d", h.NodeLen)
	}
	bitmap := binary.LittleEndian.Uint32(node[p : p+4])
	if bitmap&0xFFFF0000 != 0 {
		return 0, false, fmt.Errorf("map branch bitmap high bits must be zero")
	}
	p += 4
	entryCount := popcount16(uint16(bitmap))
	childrenBytes := entryCount * 4
	if int(h.NodeLen) < p+childrenBytes {
		return 0, false, fmt.Errorf("child address truncated")
	}

	slot := uint8((hash >> (depth * 4)) & hamtMask)
	mask := uint32((uint32(1) << slot) - 1)
	idx := popcount16(uint16(bitmap & mask))
	hasChild := ((bitmap >> slot) & 1) == 1

	if hasChild {
		childPos := p + idx*4
		child := binary.LittleEndian.Uint32(node[childPos : childPos+4])
		newChild, changed, err := mapSetHashed(doc, child, key, val, hash, depth+1, builder)
		if err != nil {
			return 0, false, err
		}
		if !changed {
			return off, false, nil
		}
		children := getUint32Slice(entryCount)
		pos := p
		for i := 0; i < entryCount; i++ {
			addr := binary.LittleEndian.Uint32(node[pos : pos+4])
			pos += 4
			if i == idx {
				addr = newChild
			}
			children[i] = addr
		}
		newBranch := MapBranchNode{
			Header:   NodeHeader{Kind: NodeBranch, KeyType: KeyMap},
			Bitmap:   bitmap,
			Children: children,
		}
		newOff, err := appendMapBranchNode(builder, newBranch)
		putUint32Slice(children)
		if err != nil {
			return 0, false, err
		}
		return newOff, true, nil
	}

	newChildNode := buildMapNodeFromEntries([]MapLeafEntry{{Key: key, Value: val}}, depth+1, nil)
	newChild, err := encodeMapNode(builder, newChildNode, nil)
	if err != nil {
		return 0, false, err
	}
	newBitmap := bitmap | (uint32(1) << slot)
	children := getUint32Slice(entryCount + 1)
	pos := p
	for i := 0; i < idx; i++ {
		children[i] = binary.LittleEndian.Uint32(node[pos : pos+4])
		pos += 4
	}
	children[idx] = newChild
	for i := idx; i < entryCount; i++ {
		children[i+1] = binary.LittleEndian.Uint32(node[pos : pos+4])
		pos += 4
	}
	newBranch := MapBranchNode{
		Header:   NodeHeader{Kind: NodeBranch, KeyType: KeyMap},
		Bitmap:   newBitmap,
		Children: children,
	}
	newOff, err := appendMapBranchNode(builder, newBranch)
	putUint32Slice(children)
	if err != nil {
		return 0, false, err
	}
	return newOff, true, nil
}

func mapDelete(doc []byte, off uint32, key []byte, depth int, builder *Builder) (uint32, bool, error) {
	if depth > maxDepth32 {
		return 0, false, fmt.Errorf("map depth exceeds max")
	}
	h, node, err := NodeSliceAt(doc, off)
	if err != nil {
		return 0, false, err
	}
	if h.KeyType != KeyMap {
		return 0, false, fmt.Errorf("node is not a map")
	}
	if h.Kind == NodeLeaf {
		leaf, err := ParseMapLeafNode(doc, node)
		if err != nil {
			return 0, false, err
		}
		defer releaseMapLeafNode(&leaf)
		found := false
		entries := make([]MapLeafEntry, 0, len(leaf.Entries))
		for _, entry := range leaf.Entries {
			if bytes.Equal(entry.Key, key) {
				found = true
				continue
			}
			entries = append(entries, entry)
		}
		if !found {
			return off, false, nil
		}
		if len(entries) == 0 {
			off, err := appendMapLeafNodeSorted(builder, nil)
			if err != nil {
				return 0, false, err
			}
			return off, true, nil
		}
		newNode := buildMapNodeFromEntries(entries, depth, nil)
		newOff, err := encodeMapNode(builder, newNode, nil)
		if err != nil {
			return 0, false, err
		}
		return newOff, true, nil
	}

	branch, err := ParseMapBranchNode(node)
	if err != nil {
		return 0, false, err
	}
	defer releaseMapBranchNode(&branch)
	slot := uint8((XXH32(key, 0) >> (depth * 4)) & hamtMask)
	mask := uint32((uint32(1) << slot) - 1)
	idx := popcount16(uint16(branch.Bitmap & mask))
	hasChild := ((branch.Bitmap >> slot) & 1) == 1
	if !hasChild {
		return off, false, nil
	}
	child := branch.Children[idx]
	newChild, changed, err := mapDelete(doc, child, key, depth+1, builder)
	if err != nil {
		return 0, false, err
	}
	if !changed {
		return off, false, nil
	}
	emptyChild, err := mapNodeEmpty(builder.buf, newChild)
	if err != nil {
		return 0, false, err
	}
	if emptyChild {
		newBitmap := branch.Bitmap &^ (uint32(1) << slot)
		children := getUint32Slice(len(branch.Children) - 1)
		copy(children, branch.Children[:idx])
		copy(children[idx:], branch.Children[idx+1:])
		if len(children) == 0 {
			putUint32Slice(children)
			off, err := appendMapLeafNodeSorted(builder, nil)
			if err != nil {
				return 0, false, err
			}
			return off, true, nil
		}
		newBranch := MapBranchNode{
			Header:   NodeHeader{Kind: NodeBranch, KeyType: KeyMap},
			Bitmap:   newBitmap,
			Children: children,
		}
		newOff, err := appendMapBranchNode(builder, newBranch)
		putUint32Slice(children)
		if err != nil {
			return 0, false, err
		}
		return newOff, true, nil
	}

	children := branch.Children
	children[idx] = newChild
	newBranch := MapBranchNode{
		Header:   NodeHeader{Kind: NodeBranch, KeyType: KeyMap},
		Bitmap:   branch.Bitmap,
		Children: children,
	}
	newOff, err := appendMapBranchNode(builder, newBranch)
	if err != nil {
		return 0, false, err
	}
	return newOff, true, nil
}

func valueEqual(a, b Value) bool {
	if a.Type != b.Type {
		return false
	}
	switch a.Type {
	case TypeNil:
		return true
	case TypeBit:
		return a.Bool == b.Bool
	case TypeI64:
		return a.I64 == b.I64
	case TypeF64:
		return math.Float64bits(a.F64) == math.Float64bits(b.F64)
	case TypeTxt, TypeBin:
		return bytes.Equal(a.Bytes, b.Bytes)
	case TypeArr, TypeMap:
		return a.Offset == b.Offset
	default:
		return false
	}
}

func mapNodeEmpty(doc []byte, off uint32) (bool, error) {
	h, node, err := NodeSliceAt(doc, off)
	if err != nil {
		return false, err
	}
	if h.KeyType != KeyMap {
		return false, fmt.Errorf("node is not a map")
	}
	if h.Kind != NodeLeaf {
		return false, nil
	}
	leaf, err := ParseMapLeafNode(doc, node)
	if err != nil {
		return false, err
	}
	defer releaseMapLeafNode(&leaf)
	return len(leaf.Entries) == 0, nil
}
