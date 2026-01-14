package tron

import "fmt"

// ArrGet returns the value at index, if present.
func ArrGet(doc []byte, rootOff uint32, index uint32) (Value, bool, error) {
	length, err := arrayRootLength(doc, rootOff)
	if err != nil {
		return Value{}, false, err
	}
	if index >= length {
		return Value{}, false, fmt.Errorf("array index %d out of range", index)
	}
	return arrGet(doc, rootOff, index, true)
}

// ArrayRootLength returns the length stored on the array root node.
func ArrayRootLength(doc []byte, rootOff uint32) (uint32, error) {
	return arrayRootLength(doc, rootOff)
}

// ArraySetNode updates an array node at rootOff and returns the new root offset.
func ArraySetNode(builder *Builder, rootOff uint32, index uint32, value Value, length uint32) (uint32, error) {
	if builder == nil {
		return 0, fmt.Errorf("nil builder")
	}
	return arrSet(builder, rootOff, index, value, length)
}

// ArrSetDocument replaces the value at index for a top-level array document.
func ArrSetDocument(doc []byte, index uint32, value Value) ([]byte, error) {
	rootOff, length, builder, err := arrayDocumentBase(doc)
	if err != nil {
		return nil, err
	}
	if index >= length {
		return nil, fmt.Errorf("array index %d out of range", index)
	}
	newRoot, err := arrSet(builder, rootOff, index, value, length)
	if err != nil {
		return nil, err
	}
	return builder.BytesWithTrailer(newRoot, rootOff), nil
}

// ArrAppendDocument appends values at the end.
func ArrAppendDocument(doc []byte, values ...Value) ([]byte, error) {
	if len(values) == 0 {
		return doc, nil
	}
	rootOff, length, builder, err := arrayDocumentBase(doc)
	if err != nil {
		return nil, err
	}
	prevRoot := rootOff
	newRoot := rootOff
	for _, v := range values {
		updated, err := arrSet(builder, newRoot, length, v, length+1)
		if err != nil {
			return nil, err
		}
		newRoot = updated
		length++
	}
	return builder.BytesWithTrailer(newRoot, prevRoot), nil
}

// ArrSliceDocument returns a new array document containing values[start:end].
func ArrSliceDocument(doc []byte, start, end uint32) ([]byte, error) {
	rootOff, length, builder, err := arrayDocumentBase(doc)
	if err != nil {
		return nil, err
	}
	if start > end || end > length {
		return nil, fmt.Errorf("array slice [%d:%d] out of range", start, end)
	}
	values, err := arrayDenseValues(doc, rootOff, length)
	if err != nil {
		return nil, err
	}
	sliced := values[start:end]
	newRoot, err := buildArrayFromValues(builder, sliced)
	if err != nil {
		return nil, err
	}
	return builder.BytesWithTrailer(newRoot, rootOff), nil
}

func arrayDocumentBase(doc []byte) (uint32, uint32, *Builder, error) {
	if _, err := DetectDocType(doc); err != nil {
		return 0, 0, nil, err
	}
	tr, err := ParseTrailer(doc)
	if err != nil {
		return 0, 0, nil, err
	}
	root, err := DecodeValueAt(doc, tr.RootOffset)
	if err != nil {
		return 0, 0, nil, err
	}
	if root.Type != TypeArr {
		return 0, 0, nil, fmt.Errorf("root is not an array")
	}
	length, err := arrayRootLength(doc, root.Offset)
	if err != nil {
		return 0, 0, nil, err
	}
	builder, _, err := NewBuilderFromDocument(doc)
	if err != nil {
		return 0, 0, nil, err
	}
	return tr.RootOffset, length, builder, nil
}

func arrayDenseValues(doc []byte, rootOff uint32, length uint32) ([]Value, error) {
	values := make([]Value, length)
	for i := uint32(0); i < length; i++ {
		val, ok, err := arrGet(doc, rootOff, i, true)
		if err != nil {
			return nil, err
		}
		if ok {
			values[i] = val
		} else {
			values[i] = Value{Type: TypeNil}
		}
	}
	return values, nil
}

func buildArrayFromValues(builder *Builder, values []Value) (uint32, error) {
	if builder == nil {
		return 0, fmt.Errorf("nil builder")
	}
	if len(values) > int(^uint32(0)) {
		return 0, fmt.Errorf("array length exceeds u32")
	}
	length := uint32(len(values))
	entries := getArrayEntrySlice(len(values))
	for i, v := range values {
		entries[i] = arrayEntry{index: uint32(i), value: v}
	}
	shift := arrayRootShift(length)
	root, err := buildArrayNode(entries, shift, length, true, nil)
	putArrayEntrySlice(entries)
	if err != nil {
		return 0, err
	}
	return encodeArrayNode(builder, root, nil)
}

func arrayRootLength(doc []byte, rootOff uint32) (uint32, error) {
	h, node, err := NodeSliceAt(doc, rootOff)
	if err != nil {
		return 0, err
	}
	if h.KeyType != KeyArr {
		return 0, fmt.Errorf("root is not array")
	}
	if !h.IsRoot {
		return 0, fmt.Errorf("array root missing root flag")
	}
	switch h.Kind {
	case NodeLeaf:
		leaf, err := ParseArrayLeafNode(node)
		if err != nil {
			return 0, err
		}
		defer releaseArrayLeafNode(&leaf)
		return leaf.Length, nil
	case NodeBranch:
		branch, err := ParseArrayBranchNode(node)
		if err != nil {
			return 0, err
		}
		defer releaseArrayBranchNode(&branch)
		return branch.Length, nil
	default:
		return 0, fmt.Errorf("unknown array node kind")
	}
}

func arrGet(doc []byte, off uint32, index uint32, isRoot bool) (Value, bool, error) {
	h, node, err := NodeSliceAt(doc, off)
	if err != nil {
		return Value{}, false, err
	}
	if h.KeyType != KeyArr {
		return Value{}, false, fmt.Errorf("node is not an array")
	}
	if h.Kind == NodeLeaf {
		leaf, err := ParseArrayLeafNode(node)
		if err != nil {
			return Value{}, false, err
		}
		defer releaseArrayLeafNode(&leaf)
		if !isRoot && leaf.Header.IsRoot {
			return Value{}, false, fmt.Errorf("array non-root leaf marked as root")
		}
		slot := uint8(index & 0xF)
		if ((leaf.Bitmap >> slot) & 1) == 0 {
			return Value{}, false, nil
		}
		mask := uint16((uint32(1) << slot) - 1)
		idx := popcount16(leaf.Bitmap & mask)
		addr := leaf.ValueAddrs[idx]
		val, err := DecodeValueAt(doc, addr)
		if err != nil {
			return Value{}, false, err
		}
		return val, true, nil
	}
	branch, err := ParseArrayBranchNode(node)
	if err != nil {
		return Value{}, false, err
	}
	defer releaseArrayBranchNode(&branch)
	if !isRoot && branch.Header.IsRoot {
		return Value{}, false, fmt.Errorf("array non-root branch marked as root")
	}
	slot := uint8((index >> branch.Shift) & 0xF)
	if ((branch.Bitmap >> slot) & 1) == 0 {
		return Value{}, false, nil
	}
	mask := uint16((uint32(1) << slot) - 1)
	idx := popcount16(branch.Bitmap & mask)
	child := branch.Children[idx]
	return arrGet(doc, child, index, false)
}

func arrSet(builder *Builder, rootOff uint32, index uint32, value Value, length uint32) (uint32, error) {
	rootOff, err := ensureArrayRoot(builder, rootOff, index, length)
	if err != nil {
		return 0, err
	}
	doc := builder.buf
	newRoot, _, err := arrSetNode(doc, rootOff, index, value, builder, true, length)
	if err != nil {
		return 0, err
	}
	return newRoot, nil
}

func ensureArrayRoot(builder *Builder, rootOff uint32, index uint32, length uint32) (uint32, error) {
	h, node, err := NodeSliceAt(builder.buf, rootOff)
	if err != nil {
		return 0, err
	}
	if h.KeyType != KeyArr || !h.IsRoot {
		return 0, fmt.Errorf("array root missing root flag")
	}
	var shift uint8
	switch h.Kind {
	case NodeLeaf:
		shift = 0
	case NodeBranch:
		branch, err := ParseArrayBranchNode(node)
		if err != nil {
			return 0, err
		}
		defer releaseArrayBranchNode(&branch)
		shift = branch.Shift
	default:
		return 0, fmt.Errorf("unknown array node kind")
	}
	off := rootOff
	for (index >> shift) > 0xF {
		childOff, err := cloneArrayNodeAsChild(builder.buf, off, builder)
		if err != nil {
			return 0, err
		}
		shift += 4
		branch := ArrayBranchNode{
			Header:   NodeHeader{Kind: NodeBranch, KeyType: KeyArr, IsRoot: true},
			Shift:    shift,
			Bitmap:   1,
			Length:   length,
			Children: []uint32{childOff},
		}
		newOff, err := appendArrayBranchNode(builder, branch)
		if err != nil {
			return 0, err
		}
		off = newOff
	}
	return off, nil
}

func arrSetNode(doc []byte, off uint32, index uint32, value Value, builder *Builder, isRoot bool, rootLength uint32) (uint32, bool, error) {
	h, node, err := NodeSliceAt(doc, off)
	if err != nil {
		return 0, false, err
	}
	if h.KeyType != KeyArr {
		return 0, false, fmt.Errorf("node is not an array")
	}
	if h.Kind == NodeLeaf {
		leaf, err := ParseArrayLeafNode(node)
		if err != nil {
			return 0, false, err
		}
		defer releaseArrayLeafNode(&leaf)
		if !isRoot && leaf.Header.IsRoot {
			return 0, false, fmt.Errorf("array non-root leaf marked as root")
		}
		slot := uint8(index & 0xF)
		mask := uint16((uint32(1) << slot) - 1)
		idx := popcount16(leaf.Bitmap & mask)
		has := ((leaf.Bitmap >> slot) & 1) == 1
		if has {
			cur, err := DecodeValueAt(doc, leaf.ValueAddrs[idx])
			if err != nil {
				return 0, false, err
			}
			if valueEqual(cur, value) {
				if !isRoot || leaf.Length == rootLength {
					return off, false, nil
				}
			}
		}
		newAddr, err := valueAddress(builder, value)
		if err != nil {
			return 0, false, err
		}
		values := leaf.ValueAddrs
		var newValues []uint32
		if has {
			newValues = getUint32Slice(len(values))
			copy(newValues, values)
			newValues[idx] = newAddr
		} else {
			newValues = getUint32Slice(len(values) + 1)
			copy(newValues, values[:idx])
			newValues[idx] = newAddr
			copy(newValues[idx+1:], values[idx:])
		}
		bitmap := leaf.Bitmap
		if !has {
			bitmap |= 1 << slot
		}
		length := uint32(0)
		if isRoot {
			length = rootLength
		}
		newLeaf := ArrayLeafNode{
			Header: NodeHeader{Kind: NodeLeaf, KeyType: KeyArr, IsRoot: isRoot},
			Shift:  0,
			Bitmap: bitmap,
			Length: length,
			ValueAddrs: newValues,
		}
		newOff, err := appendArrayLeafNode(builder, newLeaf)
		putUint32Slice(newValues)
		if err != nil {
			return 0, false, err
		}
		return newOff, true, nil
	}

	branch, err := ParseArrayBranchNode(node)
	if err != nil {
		return 0, false, err
	}
	defer releaseArrayBranchNode(&branch)
	if !isRoot && branch.Header.IsRoot {
		return 0, false, fmt.Errorf("array non-root branch marked as root")
	}
	slot := uint8((index >> branch.Shift) & 0xF)
	mask := uint16((uint32(1) << slot) - 1)
	idx := popcount16(branch.Bitmap & mask)
	has := ((branch.Bitmap >> slot) & 1) == 1

	var child uint32
	if has {
		oldChild := branch.Children[idx]
		newChild, childChanged, err := arrSetNode(doc, oldChild, index, value, builder, false, 0)
		if err != nil {
			return 0, false, err
		}
		if !childChanged && (!isRoot || branch.Length == rootLength) {
			return off, false, nil
		}
		child = newChild
	} else {
		newChild, err := buildArrayPath(index, branch.Shift-4, value, builder)
		if err != nil {
			return 0, false, err
		}
		child = newChild
	}

	bitmap := branch.Bitmap
	children := branch.Children
	if has {
		children[idx] = child
	} else {
		bitmap |= 1 << slot
		newChildren := getUint32Slice(len(children) + 1)
		copy(newChildren, children[:idx])
		newChildren[idx] = child
		copy(newChildren[idx+1:], children[idx:])
		children = newChildren
	}
	length := uint32(0)
	if isRoot {
		length = rootLength
	}
	newBranch := ArrayBranchNode{
		Header:   NodeHeader{Kind: NodeBranch, KeyType: KeyArr, IsRoot: isRoot},
		Shift:    branch.Shift,
		Bitmap:   bitmap,
		Length:   length,
		Children: children,
	}
	newOff, err := appendArrayBranchNode(builder, newBranch)
	if !has {
		putUint32Slice(children)
	}
	if err != nil {
		return 0, false, err
	}
	return newOff, true, nil
}

func buildArrayPath(index uint32, shift uint8, value Value, builder *Builder) (uint32, error) {
	if shift%4 != 0 {
		return 0, fmt.Errorf("array path shift must be multiple of 4")
	}
	if shift == 0 {
		slot := uint8(index & 0xF)
		addr, err := valueAddress(builder, value)
		if err != nil {
			return 0, err
		}
		leaf := ArrayLeafNode{
			Header: NodeHeader{Kind: NodeLeaf, KeyType: KeyArr, IsRoot: false},
			Shift:  0,
			Bitmap: 1 << slot,
			Length: 0,
			ValueAddrs: []uint32{addr},
		}
		return appendArrayLeafNode(builder, leaf)
	}
	child, err := buildArrayPath(index, shift-4, value, builder)
	if err != nil {
		return 0, err
	}
	slot := uint8((index >> shift) & 0xF)
	branch := ArrayBranchNode{
		Header:   NodeHeader{Kind: NodeBranch, KeyType: KeyArr, IsRoot: false},
		Shift:    shift,
		Bitmap:   1 << slot,
		Length:   0,
		Children: []uint32{child},
	}
	return appendArrayBranchNode(builder, branch)
}

func cloneArrayNodeAsChild(doc []byte, off uint32, builder *Builder) (uint32, error) {
	h, node, err := NodeSliceAt(doc, off)
	if err != nil {
		return 0, err
	}
	if h.KeyType != KeyArr {
		return 0, fmt.Errorf("node is not an array")
	}
	if !h.IsRoot {
		return off, nil
	}
	switch h.Kind {
	case NodeLeaf:
		leaf, err := ParseArrayLeafNode(node)
		if err != nil {
			return 0, err
		}
		defer releaseArrayLeafNode(&leaf)
		child := ArrayLeafNode{
			Header:     NodeHeader{Kind: NodeLeaf, KeyType: KeyArr, IsRoot: false},
			Shift:      leaf.Shift,
			Bitmap:     leaf.Bitmap,
			Length:     0,
			ValueAddrs: leaf.ValueAddrs,
		}
		return appendArrayLeafNode(builder, child)
	case NodeBranch:
		branch, err := ParseArrayBranchNode(node)
		if err != nil {
			return 0, err
		}
		defer releaseArrayBranchNode(&branch)
		child := ArrayBranchNode{
			Header:   NodeHeader{Kind: NodeBranch, KeyType: KeyArr, IsRoot: false},
			Shift:    branch.Shift,
			Bitmap:   branch.Bitmap,
			Length:   0,
			Children: branch.Children,
		}
		return appendArrayBranchNode(builder, child)
	default:
		return 0, fmt.Errorf("unknown array node kind")
	}
}
