package tron

import "errors"

var (
	ErrTronProxyNoData      = errors.New("trongen: no data")
	ErrTronProxyNotTree     = errors.New("trongen: document is not a tree")
	ErrTronProxyRootNotMap  = errors.New("trongen: root is not a map")
	ErrTronProxyFieldNotMap = errors.New("trongen: field is not a map")

	ErrNoData      = ErrTronProxyNoData
	ErrNotTree     = ErrTronProxyNotTree
	ErrRootNotMap  = ErrTronProxyRootNotMap
	ErrFieldNotMap = ErrTronProxyFieldNotMap
)

// MapRoot returns the root offset for a TRON document with a map root.
func MapRoot(doc []byte) (uint32, error) {
	if _, err := DetectDocType(doc); err != nil {
		return 0, err
	}
	tr, err := ParseTrailer(doc)
	if err != nil {
		return 0, err
	}
	root, err := DecodeValueAt(doc, tr.RootOffset)
	if err != nil {
		return 0, err
	}
	if root.Type != TypeMap {
		return 0, ErrTronProxyRootNotMap
	}
	return tr.RootOffset, nil
}

// DocForRoot returns a TRON document with root as the document root.
func DocForRoot(doc []byte, root uint32) ([]byte, error) {
	if len(doc) == 0 {
		return nil, ErrTronProxyNoData
	}
	tr, err := ParseTrailer(doc)
	if err == nil && tr.RootOffset == root {
		return doc, nil
	}
	builder, _, err := NewBuilderFromDocument(doc)
	if err != nil {
		return nil, err
	}
	return builder.BytesWithTrailer(root, root), nil
}

// GetFieldValue returns the value for a map key using a precomputed hash.
func GetFieldValue(doc []byte, root uint32, key string, hash uint32) (Value, bool, error) {
	header, _, err := NodeSliceAt(doc, root)
	if err != nil {
		return Value{}, false, err
	}
	if header.KeyType != KeyMap {
		return Value{}, false, ErrTronProxyRootNotMap
	}
	return MapGetHashed(doc, root, []byte(key), hash)
}

// SetFieldValue updates a map key using a precomputed hash and returns the updated doc and root offset.
func SetFieldValue(doc []byte, root uint32, key string, hash uint32, value any) ([]byte, uint32, error) {
	if len(doc) == 0 {
		return nil, 0, ErrTronProxyNoData
	}
	builder, _, err := NewBuilderFromDocument(doc)
	if err != nil {
		return nil, 0, err
	}
	header, _, err := NodeSliceAt(doc, root)
	if err != nil {
		return nil, 0, err
	}
	if header.KeyType != KeyMap {
		return nil, 0, ErrTronProxyRootNotMap
	}
	val, err := ValueFromGo(builder, value)
	if err != nil {
		return nil, 0, err
	}
	newRoot, _, err := MapSetNodeHashed(builder, root, []byte(key), hash, val)
	if err != nil {
		return nil, 0, err
	}
	return builder.BytesWithTrailer(newRoot, root), newRoot, nil
}
