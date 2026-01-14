package patch

import (
	"bytes"
	"errors"
	"fmt"
	"math"

	tron "github.com/starfederation/tron-go"
)

const (
	opAdd int64 = iota
	opRemove
	opReplace
	opMove
	opCopy
	opTest
)

const maxUint32 = ^uint32(0)

var (
	keyOp       = []byte("op")
	keyPath     = []byte("path")
	keyValue    = []byte("value")
	keyFrom     = []byte("from")
	appendToken = []byte("-")
)

var (
	errMapKeyMissing    = errors.New("map key missing")
	errMapValueMismatch = errors.New("map value mismatch")
)

type tokenType uint8

const (
	tokenKey tokenType = iota
	tokenIndex
)

type pathToken struct {
	typ   tokenType
	key   []byte
	index uint32
}

type opRecord struct {
	op       int64
	path     []pathToken
	value    tron.Value
	hasValue bool
	from     []pathToken
	hasFrom  bool
}

// ApplyPatch applies TRON Patch (RFC 6902 JSON Patch semantics) to target.
func ApplyPatch(target, patch []byte) ([]byte, error) {
	patchType, err := tron.DetectDocType(patch)
	if err != nil {
		return nil, err
	}
	if patchType != tron.DocTree {
		return nil, fmt.Errorf("patch must be a tree document")
	}
	patchTrailer, err := tron.ParseTrailer(patch)
	if err != nil {
		return nil, err
	}
	patchHeader, _, err := tron.NodeSliceAt(patch, patchTrailer.RootOffset)
	if err != nil {
		return nil, err
	}
	if patchHeader.KeyType != tron.KeyArr {
		return nil, fmt.Errorf("patch root must be an array")
	}

	state, err := newPatchState(target)
	if err != nil {
		return nil, err
	}

	length, err := tron.ArrayRootLength(patch, patchTrailer.RootOffset)
	if err != nil {
		return nil, err
	}
	for i := uint32(0); i < length; i++ {
		entry, ok, err := tron.ArrGet(patch, patchTrailer.RootOffset, i)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, fmt.Errorf("patch op %d missing", i)
		}
		if entry.Type != tron.TypeMap {
			return nil, fmt.Errorf("patch op %d must be a map", i)
		}
		op, err := parseOperation(patch, entry.Offset)
		if err != nil {
			return nil, fmt.Errorf("patch op %d: %w", i, err)
		}
		if err := state.applyOperation(patch, op); err != nil {
			return nil, fmt.Errorf("patch op %d: %w", i, err)
		}
	}
	return state.finish()
}

type patchState struct {
	builder  *tron.Builder
	rootVal  tron.Value
	prevRoot uint32
}

func newPatchState(target []byte) (*patchState, error) {
	docType, err := tron.DetectDocType(target)
	if err != nil {
		return nil, err
	}
	switch docType {
	case tron.DocScalar:
		val, err := tron.DecodeScalarDocument(target)
		if err != nil {
			return nil, err
		}
		return &patchState{rootVal: val}, nil
	case tron.DocTree:
		tr, err := tron.ParseTrailer(target)
		if err != nil {
			return nil, err
		}
		header, _, err := tron.NodeSliceAt(target, tr.RootOffset)
		if err != nil {
			return nil, err
		}
		if header.KeyType != tron.KeyArr && header.KeyType != tron.KeyMap {
			return nil, fmt.Errorf("unsupported root node type")
		}
		builder, _, err := tron.NewBuilderFromDocument(target)
		if err != nil {
			return nil, err
		}
		rootType := tron.TypeMap
		if header.KeyType == tron.KeyArr {
			rootType = tron.TypeArr
		}
		return &patchState{
			builder:  builder,
			rootVal:  tron.Value{Type: rootType, Offset: tr.RootOffset},
			prevRoot: tr.RootOffset,
		}, nil
	default:
		return nil, fmt.Errorf("unknown document type")
	}
}

func (s *patchState) docBytes() []byte {
	if s.builder == nil {
		return nil
	}
	return s.builder.Buffer()
}

func (s *patchState) ensureBuilder() {
	if s.builder == nil {
		s.builder = tron.NewBuilder()
		s.prevRoot = 0
	}
}

func (s *patchState) finish() ([]byte, error) {
	switch s.rootVal.Type {
	case tron.TypeArr, tron.TypeMap:
		if s.builder == nil {
			return nil, fmt.Errorf("missing builder for tree document")
		}
		return s.builder.BytesWithTrailer(s.rootVal.Offset, s.prevRoot), nil
	default:
		return tron.EncodeScalarDocument(s.rootVal)
	}
}

func parseOperation(doc []byte, off uint32) (opRecord, error) {
	opVal, ok, err := tron.MapGet(doc, off, keyOp)
	if err != nil {
		return opRecord{}, err
	}
	if !ok {
		return opRecord{}, fmt.Errorf("missing op")
	}
	if opVal.Type != tron.TypeI64 {
		return opRecord{}, fmt.Errorf("op must be i64")
	}
	if opVal.I64 < opAdd || opVal.I64 > opTest {
		return opRecord{}, fmt.Errorf("invalid op %d", opVal.I64)
	}

	pathVal, ok, err := tron.MapGet(doc, off, keyPath)
	if err != nil {
		return opRecord{}, err
	}
	if !ok {
		return opRecord{}, fmt.Errorf("missing path")
	}
	if pathVal.Type != tron.TypeArr {
		return opRecord{}, fmt.Errorf("path must be array")
	}
	path, err := parsePath(doc, pathVal.Offset)
	if err != nil {
		return opRecord{}, err
	}

	rec := opRecord{op: opVal.I64, path: path}
	if val, ok, err := tron.MapGet(doc, off, keyValue); err != nil {
		return opRecord{}, err
	} else if ok {
		rec.value = val
		rec.hasValue = true
	}
	if fromVal, ok, err := tron.MapGet(doc, off, keyFrom); err != nil {
		return opRecord{}, err
	} else if ok {
		if fromVal.Type != tron.TypeArr {
			return opRecord{}, fmt.Errorf("from must be array")
		}
		fromPath, err := parsePath(doc, fromVal.Offset)
		if err != nil {
			return opRecord{}, err
		}
		rec.from = fromPath
		rec.hasFrom = true
	}
	return rec, nil
}

func parsePath(doc []byte, off uint32) ([]pathToken, error) {
	length, err := tron.ArrayRootLength(doc, off)
	if err != nil {
		return nil, err
	}
	tokens := make([]pathToken, 0, length)
	for i := uint32(0); i < length; i++ {
		val, ok, err := tron.ArrGet(doc, off, i)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, fmt.Errorf("path token %d missing", i)
		}
		switch val.Type {
		case tron.TypeTxt:
			tokens = append(tokens, pathToken{typ: tokenKey, key: val.Bytes})
		case tron.TypeI64:
			if val.I64 < 0 || val.I64 > int64(maxUint32) {
				return nil, fmt.Errorf("array index out of range: %d", val.I64)
			}
			tokens = append(tokens, pathToken{typ: tokenIndex, index: uint32(val.I64)})
		default:
			return nil, fmt.Errorf("invalid path token type %d", val.Type)
		}
	}
	return tokens, nil
}

func (s *patchState) applyOperation(patchDoc []byte, op opRecord) error {
	switch op.op {
	case opAdd:
		if !op.hasValue {
			return fmt.Errorf("add requires value")
		}
		val, err := s.cloneForTarget(patchDoc, op.value)
		if err != nil {
			return err
		}
		return s.applyAdd(op.path, val)
	case opRemove:
		return s.applyRemove(op.path)
	case opReplace:
		if !op.hasValue {
			return fmt.Errorf("replace requires value")
		}
		val, err := s.cloneForTarget(patchDoc, op.value)
		if err != nil {
			return err
		}
		return s.applyReplace(op.path, val)
	case opMove:
		if !op.hasFrom {
			return fmt.Errorf("move requires from")
		}
		return s.applyMove(op.from, op.path)
	case opCopy:
		if !op.hasFrom {
			return fmt.Errorf("copy requires from")
		}
		return s.applyCopy(op.from, op.path)
	case opTest:
		if !op.hasValue {
			return fmt.Errorf("test requires value")
		}
		return s.applyTest(patchDoc, op.path, op.value)
	default:
		return fmt.Errorf("unknown op %d", op.op)
	}
}

func (s *patchState) cloneForTarget(doc []byte, v tron.Value) (tron.Value, error) {
	if v.Type == tron.TypeArr || v.Type == tron.TypeMap {
		s.ensureBuilder()
	}
	return tron.CloneValueFromDoc(doc, v, s.builder)
}

type pathStep struct {
	container tron.Value
	token     pathToken
}

func (s *patchState) resolveParent(tokens []pathToken) (tron.Value, pathToken, []pathStep, error) {
	if len(tokens) == 0 {
		return tron.Value{}, pathToken{}, nil, fmt.Errorf("empty path")
	}
	cur := s.rootVal
	steps := make([]pathStep, 0, len(tokens)-1)
	for i := 0; i < len(tokens)-1; i++ {
		if cur.Type != tron.TypeMap && cur.Type != tron.TypeArr {
			return tron.Value{}, pathToken{}, nil, fmt.Errorf("path traverses non-container")
		}
		tok := tokens[i]
		switch cur.Type {
		case tron.TypeMap:
			if tok.typ != tokenKey {
				return tron.Value{}, pathToken{}, nil, fmt.Errorf("expected map key")
			}
			child, ok, err := tron.MapGet(s.docBytes(), cur.Offset, tok.key)
			if err != nil {
				return tron.Value{}, pathToken{}, nil, err
			}
			if !ok {
				return tron.Value{}, pathToken{}, nil, fmt.Errorf("path not found")
			}
			steps = append(steps, pathStep{container: cur, token: tok})
			cur = child
		case tron.TypeArr:
			if tok.typ != tokenIndex {
				return tron.Value{}, pathToken{}, nil, fmt.Errorf("expected array index")
			}
			length, err := tron.ArrayRootLength(s.docBytes(), cur.Offset)
			if err != nil {
				return tron.Value{}, pathToken{}, nil, err
			}
			if tok.index >= length {
				return tron.Value{}, pathToken{}, nil, fmt.Errorf("array index %d out of range", tok.index)
			}
			child, ok, err := tron.ArrGet(s.docBytes(), cur.Offset, tok.index)
			if err != nil {
				return tron.Value{}, pathToken{}, nil, err
			}
			if !ok {
				return tron.Value{}, pathToken{}, nil, fmt.Errorf("path not found")
			}
			steps = append(steps, pathStep{container: cur, token: tok})
			cur = child
		}
	}
	return cur, tokens[len(tokens)-1], steps, nil
}

func (s *patchState) rebuildPath(steps []pathStep, updated tron.Value) (tron.Value, error) {
	cur := updated
	for i := len(steps) - 1; i >= 0; i-- {
		step := steps[i]
		var err error
		cur, err = s.setChild(step.container, step.token, cur)
		if err != nil {
			return tron.Value{}, err
		}
	}
	return cur, nil
}

func (s *patchState) setChild(container tron.Value, tok pathToken, child tron.Value) (tron.Value, error) {
	switch container.Type {
	case tron.TypeMap:
		if tok.typ != tokenKey {
			return tron.Value{}, fmt.Errorf("expected map key")
		}
		s.ensureBuilder()
		off, _, err := tron.MapSetNode(s.builder, container.Offset, tok.key, child)
		if err != nil {
			return tron.Value{}, err
		}
		return tron.Value{Type: tron.TypeMap, Offset: off}, nil
	case tron.TypeArr:
		if tok.typ != tokenIndex {
			return tron.Value{}, fmt.Errorf("expected array index")
		}
		length, err := tron.ArrayRootLength(s.docBytes(), container.Offset)
		if err != nil {
			return tron.Value{}, err
		}
		if tok.index >= length {
			return tron.Value{}, fmt.Errorf("array index %d out of range", tok.index)
		}
		s.ensureBuilder()
		off, err := tron.ArraySetNode(s.builder, container.Offset, tok.index, child, length)
		if err != nil {
			return tron.Value{}, err
		}
		return tron.Value{Type: tron.TypeArr, Offset: off}, nil
	default:
		return tron.Value{}, fmt.Errorf("path traverses non-container")
	}
}

func (s *patchState) applyAdd(path []pathToken, value tron.Value) error {
	if len(path) == 0 {
		s.rootVal = value
		return nil
	}
	parent, last, steps, err := s.resolveParent(path)
	if err != nil {
		return err
	}
	updated, err := s.addToContainer(parent, last, value)
	if err != nil {
		return err
	}
	root, err := s.rebuildPath(steps, updated)
	if err != nil {
		return err
	}
	s.rootVal = root
	return nil
}

func (s *patchState) applyReplace(path []pathToken, value tron.Value) error {
	if len(path) == 0 {
		s.rootVal = value
		return nil
	}
	parent, last, steps, err := s.resolveParent(path)
	if err != nil {
		return err
	}
	updated, err := s.replaceInContainer(parent, last, value)
	if err != nil {
		return err
	}
	root, err := s.rebuildPath(steps, updated)
	if err != nil {
		return err
	}
	s.rootVal = root
	return nil
}

func (s *patchState) applyRemove(path []pathToken) error {
	if len(path) == 0 {
		return fmt.Errorf("remove cannot target document root")
	}
	parent, last, steps, err := s.resolveParent(path)
	if err != nil {
		return err
	}
	updated, err := s.removeFromContainer(parent, last)
	if err != nil {
		return err
	}
	root, err := s.rebuildPath(steps, updated)
	if err != nil {
		return err
	}
	s.rootVal = root
	return nil
}

func (s *patchState) applyCopy(from, path []pathToken) error {
	val, err := s.getValueAtPath(from)
	if err != nil {
		return err
	}
	return s.applyAdd(path, val)
}

func (s *patchState) applyMove(from, path []pathToken) error {
	val, err := s.getValueAtPath(from)
	if err != nil {
		return err
	}
	if err := s.applyRemove(from); err != nil {
		return err
	}
	return s.applyAdd(path, val)
}

func (s *patchState) applyTest(patchDoc []byte, path []pathToken, value tron.Value) error {
	got, err := s.getValueAtPath(path)
	if err != nil {
		return err
	}
	eq, err := valuesDeepEqual(s.docBytes(), got, patchDoc, value)
	if err != nil {
		return err
	}
	if !eq {
		return fmt.Errorf("test operation failed")
	}
	return nil
}

func (s *patchState) getValueAtPath(tokens []pathToken) (tron.Value, error) {
	if len(tokens) == 0 {
		return s.rootVal, nil
	}
	cur := s.rootVal
	for _, tok := range tokens {
		switch cur.Type {
		case tron.TypeMap:
			if tok.typ != tokenKey {
				return tron.Value{}, fmt.Errorf("expected map key")
			}
			child, ok, err := tron.MapGet(s.docBytes(), cur.Offset, tok.key)
			if err != nil {
				return tron.Value{}, err
			}
			if !ok {
				return tron.Value{}, fmt.Errorf("path not found")
			}
			cur = child
		case tron.TypeArr:
			if tok.typ != tokenIndex {
				return tron.Value{}, fmt.Errorf("expected array index")
			}
			length, err := tron.ArrayRootLength(s.docBytes(), cur.Offset)
			if err != nil {
				return tron.Value{}, err
			}
			if tok.index >= length {
				return tron.Value{}, fmt.Errorf("array index %d out of range", tok.index)
			}
			child, ok, err := tron.ArrGet(s.docBytes(), cur.Offset, tok.index)
			if err != nil {
				return tron.Value{}, err
			}
			if !ok {
				cur = tron.Value{Type: tron.TypeNil}
			} else {
				cur = child
			}
		default:
			return tron.Value{}, fmt.Errorf("path traverses non-container")
		}
	}
	return cur, nil
}

func (s *patchState) addToContainer(parent tron.Value, tok pathToken, value tron.Value) (tron.Value, error) {
	switch parent.Type {
	case tron.TypeMap:
		if tok.typ != tokenKey {
			return tron.Value{}, fmt.Errorf("expected map key")
		}
		s.ensureBuilder()
		off, _, err := tron.MapSetNode(s.builder, parent.Offset, tok.key, value)
		if err != nil {
			return tron.Value{}, err
		}
		return tron.Value{Type: tron.TypeMap, Offset: off}, nil
	case tron.TypeArr:
		if tok.typ == tokenKey {
			if !bytes.Equal(tok.key, appendToken) {
				return tron.Value{}, fmt.Errorf("expected array index")
			}
			tok = pathToken{typ: tokenIndex, index: maxUint32}
		}
		if tok.typ != tokenIndex {
			return tron.Value{}, fmt.Errorf("expected array index")
		}
		s.ensureBuilder()
		return s.arrayInsert(parent.Offset, tok.index, value)
	default:
		return tron.Value{}, fmt.Errorf("path traverses non-container")
	}
}

func (s *patchState) replaceInContainer(parent tron.Value, tok pathToken, value tron.Value) (tron.Value, error) {
	switch parent.Type {
	case tron.TypeMap:
		if tok.typ != tokenKey {
			return tron.Value{}, fmt.Errorf("expected map key")
		}
		_, ok, err := tron.MapGet(s.docBytes(), parent.Offset, tok.key)
		if err != nil {
			return tron.Value{}, err
		}
		if !ok {
			return tron.Value{}, fmt.Errorf("path not found")
		}
		s.ensureBuilder()
		off, _, err := tron.MapSetNode(s.builder, parent.Offset, tok.key, value)
		if err != nil {
			return tron.Value{}, err
		}
		return tron.Value{Type: tron.TypeMap, Offset: off}, nil
	case tron.TypeArr:
		if tok.typ != tokenIndex {
			return tron.Value{}, fmt.Errorf("expected array index")
		}
		length, err := tron.ArrayRootLength(s.docBytes(), parent.Offset)
		if err != nil {
			return tron.Value{}, err
		}
		if tok.index >= length {
			return tron.Value{}, fmt.Errorf("array index %d out of range", tok.index)
		}
		s.ensureBuilder()
		off, err := tron.ArraySetNode(s.builder, parent.Offset, tok.index, value, length)
		if err != nil {
			return tron.Value{}, err
		}
		return tron.Value{Type: tron.TypeArr, Offset: off}, nil
	default:
		return tron.Value{}, fmt.Errorf("path traverses non-container")
	}
}

func (s *patchState) removeFromContainer(parent tron.Value, tok pathToken) (tron.Value, error) {
	switch parent.Type {
	case tron.TypeMap:
		if tok.typ != tokenKey {
			return tron.Value{}, fmt.Errorf("expected map key")
		}
		_, ok, err := tron.MapGet(s.docBytes(), parent.Offset, tok.key)
		if err != nil {
			return tron.Value{}, err
		}
		if !ok {
			return tron.Value{}, fmt.Errorf("path not found")
		}
		s.ensureBuilder()
		off, _, err := tron.MapDelNode(s.builder, parent.Offset, tok.key)
		if err != nil {
			return tron.Value{}, err
		}
		return tron.Value{Type: tron.TypeMap, Offset: off}, nil
	case tron.TypeArr:
		if tok.typ != tokenIndex {
			return tron.Value{}, fmt.Errorf("expected array index")
		}
		s.ensureBuilder()
		return s.arrayRemove(parent.Offset, tok.index)
	default:
		return tron.Value{}, fmt.Errorf("path traverses non-container")
	}
}

func (s *patchState) arrayInsert(off uint32, index uint32, value tron.Value) (tron.Value, error) {
	values, length, err := s.arrayValues(off)
	if err != nil {
		return tron.Value{}, err
	}
	if index == maxUint32 {
		index = length
	}
	if index > length {
		return tron.Value{}, fmt.Errorf("array index %d out of range", index)
	}
	insertAt := int(index)
	values = append(values, tron.Value{})
	copy(values[insertAt+1:], values[insertAt:])
	values[insertAt] = value
	newOff, err := s.buildArray(values)
	if err != nil {
		return tron.Value{}, err
	}
	return tron.Value{Type: tron.TypeArr, Offset: newOff}, nil
}

func (s *patchState) arrayRemove(off uint32, index uint32) (tron.Value, error) {
	values, length, err := s.arrayValues(off)
	if err != nil {
		return tron.Value{}, err
	}
	if index >= length {
		return tron.Value{}, fmt.Errorf("array index %d out of range", index)
	}
	removeAt := int(index)
	copy(values[removeAt:], values[removeAt+1:])
	values = values[:len(values)-1]
	newOff, err := s.buildArray(values)
	if err != nil {
		return tron.Value{}, err
	}
	return tron.Value{Type: tron.TypeArr, Offset: newOff}, nil
}

func (s *patchState) arrayValues(off uint32) ([]tron.Value, uint32, error) {
	length, err := tron.ArrayRootLength(s.docBytes(), off)
	if err != nil {
		return nil, 0, err
	}
	values := make([]tron.Value, int(length))
	for i := uint32(0); i < length; i++ {
		val, ok, err := tron.ArrGet(s.docBytes(), off, i)
		if err != nil {
			return nil, 0, err
		}
		if ok {
			values[int(i)] = val
		} else {
			values[int(i)] = tron.Value{Type: tron.TypeNil}
		}
	}
	return values, length, nil
}

func (s *patchState) buildArray(values []tron.Value) (uint32, error) {
	builder := tron.NewArrayBuilder()
	for _, v := range values {
		builder.Append(v)
	}
	return builder.Build(s.builder)
}

func valuesDeepEqual(docA []byte, a tron.Value, docB []byte, b tron.Value) (bool, error) {
	if a.Type != b.Type {
		return false, nil
	}
	switch a.Type {
	case tron.TypeNil:
		return true, nil
	case tron.TypeBit:
		return a.Bool == b.Bool, nil
	case tron.TypeI64:
		return a.I64 == b.I64, nil
	case tron.TypeF64:
		return math.Float64bits(a.F64) == math.Float64bits(b.F64), nil
	case tron.TypeTxt, tron.TypeBin:
		return bytes.Equal(a.Bytes, b.Bytes), nil
	case tron.TypeArr:
		return arraysDeepEqual(docA, a.Offset, docB, b.Offset)
	case tron.TypeMap:
		return mapsDeepEqual(docA, a.Offset, docB, b.Offset)
	default:
		return false, fmt.Errorf("unknown value type %d", a.Type)
	}
}

func arraysDeepEqual(docA []byte, offA uint32, docB []byte, offB uint32) (bool, error) {
	lenA, err := tron.ArrayRootLength(docA, offA)
	if err != nil {
		return false, err
	}
	lenB, err := tron.ArrayRootLength(docB, offB)
	if err != nil {
		return false, err
	}
	if lenA != lenB {
		return false, nil
	}
	for i := uint32(0); i < lenA; i++ {
		valA, err := arrayValueAt(docA, offA, i)
		if err != nil {
			return false, err
		}
		valB, err := arrayValueAt(docB, offB, i)
		if err != nil {
			return false, err
		}
		eq, err := valuesDeepEqual(docA, valA, docB, valB)
		if err != nil {
			return false, err
		}
		if !eq {
			return false, nil
		}
	}
	return true, nil
}

func arrayValueAt(doc []byte, off uint32, index uint32) (tron.Value, error) {
	length, err := tron.ArrayRootLength(doc, off)
	if err != nil {
		return tron.Value{}, err
	}
	if index >= length {
		return tron.Value{}, fmt.Errorf("array index %d out of range", index)
	}
	val, ok, err := tron.ArrGet(doc, off, index)
	if err != nil {
		return tron.Value{}, err
	}
	if !ok {
		return tron.Value{Type: tron.TypeNil}, nil
	}
	return val, nil
}

func mapsDeepEqual(docA []byte, offA uint32, docB []byte, offB uint32) (bool, error) {
	entries := make(map[string]tron.Value)
	if err := forEachMapEntry(docA, offA, func(key []byte, val tron.Value) error {
		entries[string(key)] = val
		return nil
	}); err != nil {
		return false, err
	}
	if err := forEachMapEntry(docB, offB, func(key []byte, val tron.Value) error {
		strKey := string(key)
		other, ok := entries[strKey]
		if !ok {
			return errMapKeyMissing
		}
		eq, err := valuesDeepEqual(docA, other, docB, val)
		if err != nil {
			return err
		}
		if !eq {
			return errMapValueMismatch
		}
		delete(entries, strKey)
		return nil
	}); err != nil {
		if errors.Is(err, errMapKeyMissing) || errors.Is(err, errMapValueMismatch) {
			return false, nil
		}
		return false, err
	}
	return len(entries) == 0, nil
}

func forEachMapEntry(doc []byte, off uint32, fn func(key []byte, val tron.Value) error) error {
	header, node, err := tron.NodeSliceAt(doc, off)
	if err != nil {
		return err
	}
	if header.KeyType != tron.KeyMap {
		return fmt.Errorf("node is not a map")
	}
	if header.Kind == tron.NodeLeaf {
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
		if err := forEachMapEntry(doc, child, fn); err != nil {
			return err
		}
	}
	return nil
}
