package path

import (
	"fmt"

	tron "github.com/starfederation/tron-go"
)

const hamtMaxDepth = 7

func mapGetBytes(doc []byte, off uint32, key []byte, depth int) (tron.Value, bool, error) {
	return mapGetBytesHashed(doc, off, key, tron.XXH32(key, 0), depth)
}

func mapGetBytesHashed(doc []byte, off uint32, key []byte, hash uint32, depth int) (tron.Value, bool, error) {
	if depth > hamtMaxDepth {
		return tron.Value{}, false, fmt.Errorf("map depth exceeds max")
	}
	h, node, err := tron.NodeSliceAt(doc, off)
	if err != nil {
		return tron.Value{}, false, err
	}
	if h.KeyType != tron.KeyMap {
		return tron.Value{}, false, fmt.Errorf("node is not a map")
	}
	if h.Kind == tron.NodeLeaf {
		leaf, err := tron.ParseMapLeafNode(doc, node)
		if err != nil {
			return tron.Value{}, false, err
		}
		defer tron.ReleaseMapLeafNode(&leaf)
		for _, entry := range leaf.Entries {
			if bytesEqual(entry.Key, key) {
				return entry.Value, true, nil
			}
		}
		return tron.Value{}, false, nil
	}

	branch, err := tron.ParseMapBranchNode(node)
	if err != nil {
		return tron.Value{}, false, err
	}
	defer tron.ReleaseMapBranchNode(&branch)
	slot := uint8((hash >> (depth * 4)) & 0xF)
	if ((branch.Bitmap >> slot) & 1) == 0 {
		return tron.Value{}, false, nil
	}
	mask := uint32((uint32(1) << slot) - 1)
	idx := popcount16(uint16(branch.Bitmap & mask))
	child := branch.Children[idx]
	return mapGetBytesHashed(doc, child, key, hash, depth+1)
}

func mapIterValues(doc []byte, off uint32, fn func(tron.Value) error) error {
	h, node, err := tron.NodeSliceAt(doc, off)
	if err != nil {
		return err
	}
	if h.KeyType != tron.KeyMap {
		return fmt.Errorf("node is not a map")
	}
	if h.Kind == tron.NodeLeaf {
		leaf, err := tron.ParseMapLeafNode(doc, node)
		if err != nil {
			return err
		}
		defer tron.ReleaseMapLeafNode(&leaf)
		for _, entry := range leaf.Entries {
			if err := fn(entry.Value); err != nil {
				return err
			}
		}
		return nil
	}
	branch, err := tron.ParseMapBranchNode(node)
	if err != nil {
		return err
	}
	defer tron.ReleaseMapBranchNode(&branch)
	for _, child := range branch.Children {
		if err := mapIterValues(doc, child, fn); err != nil {
			return err
		}
	}
	return nil
}

func mapIterEntries(doc []byte, off uint32, fn func(key []byte, val tron.Value) error) error {
	h, node, err := tron.NodeSliceAt(doc, off)
	if err != nil {
		return err
	}
	if h.KeyType != tron.KeyMap {
		return fmt.Errorf("node is not a map")
	}
	if h.Kind == tron.NodeLeaf {
		leaf, err := tron.ParseMapLeafNode(doc, node)
		if err != nil {
			return err
		}
		defer tron.ReleaseMapLeafNode(&leaf)
		for _, entry := range leaf.Entries {
			if err := fn(entry.Key, entry.Value); err != nil {
				return err
			}
		}
		return nil
	}
	branch, err := tron.ParseMapBranchNode(node)
	if err != nil {
		return err
	}
	defer tron.ReleaseMapBranchNode(&branch)
	for _, child := range branch.Children {
		if err := mapIterEntries(doc, child, fn); err != nil {
			return err
		}
	}
	return nil
}

func arrGetRaw(doc []byte, off uint32, index uint32) (tron.Value, bool, error) {
	h, node, err := tron.NodeSliceAt(doc, off)
	if err != nil {
		return tron.Value{}, false, err
	}
	if h.KeyType != tron.KeyArr {
		return tron.Value{}, false, fmt.Errorf("node is not an array")
	}
	if h.Kind == tron.NodeLeaf {
		leaf, err := tron.ParseArrayLeafNode(node)
		if err != nil {
			return tron.Value{}, false, err
		}
		defer tron.ReleaseArrayLeafNode(&leaf)
		slot := uint8(index & 0xF)
		if ((leaf.Bitmap >> slot) & 1) == 0 {
			return tron.Value{}, false, nil
		}
		mask := uint16((uint32(1) << slot) - 1)
		idx := popcount16(leaf.Bitmap & mask)
		addr := leaf.ValueAddrs[idx]
		val, err := tron.DecodeValueAt(doc, addr)
		if err != nil {
			return tron.Value{}, false, err
		}
		return val, true, nil
	}

	branch, err := tron.ParseArrayBranchNode(node)
	if err != nil {
		return tron.Value{}, false, err
	}
	defer tron.ReleaseArrayBranchNode(&branch)
	slot := uint8((index >> branch.Shift) & 0xF)
	if ((branch.Bitmap >> slot) & 1) == 0 {
		return tron.Value{}, false, nil
	}
	mask := uint16((uint32(1) << slot) - 1)
	idx := popcount16(branch.Bitmap & mask)
	child := branch.Children[idx]
	return arrGetRaw(doc, child, index)
}

func arrayLength(doc []byte, off uint32) (uint32, error) {
	return tron.ArrayRootLength(doc, off)
}

func arrIterValues(doc []byte, off uint32, fn func(tron.Value) error) error {
	h, node, err := tron.NodeSliceAt(doc, off)
	if err != nil {
		return err
	}
	if h.KeyType != tron.KeyArr {
		return fmt.Errorf("node is not an array")
	}
	if h.Kind == tron.NodeLeaf {
		leaf, err := tron.ParseArrayLeafNode(node)
		if err != nil {
			return err
		}
		defer tron.ReleaseArrayLeafNode(&leaf)
		for _, addr := range leaf.ValueAddrs {
			val, err := tron.DecodeValueAt(doc, addr)
			if err != nil {
				return err
			}
			if err := fn(val); err != nil {
				return err
			}
		}
		return nil
	}
	branch, err := tron.ParseArrayBranchNode(node)
	if err != nil {
		return err
	}
	defer tron.ReleaseArrayBranchNode(&branch)
	for _, child := range branch.Children {
		if err := arrIterValues(doc, child, fn); err != nil {
			return err
		}
	}
	return nil
}

func arrCollectValues(doc []byte, off uint32, base uint32, values []tron.Value, present []bool) error {
	h, node, err := tron.NodeSliceAt(doc, off)
	if err != nil {
		return err
	}
	if h.KeyType != tron.KeyArr {
		return fmt.Errorf("node is not an array")
	}
	if h.Kind == tron.NodeLeaf {
		leaf, err := tron.ParseArrayLeafNode(node)
		if err != nil {
			return err
		}
		defer tron.ReleaseArrayLeafNode(&leaf)
		if leaf.Shift != 0 {
			return fmt.Errorf("array leaf shift must be 0")
		}
		idx := 0
		for slot := 0; slot < 16; slot++ {
			if ((leaf.Bitmap >> uint(slot)) & 1) == 0 {
				continue
			}
			val, err := tron.DecodeValueAt(doc, leaf.ValueAddrs[idx])
			if err != nil {
				return err
			}
			index := base + uint32(slot)
			if index >= uint32(len(values)) {
				return fmt.Errorf("array index out of range: %d", index)
			}
			values[index] = val
			present[index] = true
			idx++
		}
		return nil
	}
	branch, err := tron.ParseArrayBranchNode(node)
	if err != nil {
		return err
	}
	defer tron.ReleaseArrayBranchNode(&branch)
	childIdx := 0
	for slot := 0; slot < 16; slot++ {
		if ((branch.Bitmap >> uint(slot)) & 1) == 0 {
			continue
		}
		child := branch.Children[childIdx]
		childBase := base + (uint32(slot) << branch.Shift)
		if err := arrCollectValues(doc, child, childBase, values, present); err != nil {
			return err
		}
		childIdx++
	}
	return nil
}

func popcount16(x uint16) int {
	x = x - ((x >> 1) & 0x5555)
	x = (x & 0x3333) + ((x >> 2) & 0x3333)
	x = (x + (x >> 4)) & 0x0F0F
	x = x + (x >> 8)
	return int(x & 0x1F)
}

func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
