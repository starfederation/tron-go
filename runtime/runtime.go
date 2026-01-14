package runtime

import (
	"errors"

	"github.com/starfederation/tron-go"
	tronjson "github.com/starfederation/tron-go/runtime/json"
)

var ErrTronProxyNoData = errors.New("trongen: no data")
var ErrTronProxyNotTree = errors.New("trongen: document is not a tree")
var ErrTronProxyRootNotMap = errors.New("trongen: root is not a map")
var ErrTronProxyFieldNotMap = errors.New("trongen: field is not a map")

var ErrNoData = ErrTronProxyNoData
var ErrNotTree = ErrTronProxyNotTree
var ErrRootNotMap = ErrTronProxyRootNotMap
var ErrFieldNotMap = ErrTronProxyFieldNotMap

// MapRoot returns the root offset for a TRON document with a map root.
func MapRoot(doc []byte) (uint32, error) {
	if _, err := tron.DetectDocType(doc); err != nil {
		return 0, err
	}
	tr, err := tron.ParseTrailer(doc)
	if err != nil {
		return 0, err
	}
	root, err := tron.DecodeValueAt(doc, tr.RootOffset)
	if err != nil {
		return 0, err
	}
	if root.Type != tron.TypeMap {
		return 0, ErrTronProxyRootNotMap
	}
	return tr.RootOffset, nil
}

// DocForRoot returns a TRON document with root as the document root.
func DocForRoot(doc []byte, root uint32) ([]byte, error) {
	if len(doc) == 0 {
		return nil, ErrTronProxyNoData
	}
	tr, err := tron.ParseTrailer(doc)
	if err == nil && tr.RootOffset == root {
		return doc, nil
	}
	builder, _, err := tron.NewBuilderFromDocument(doc)
	if err != nil {
		return nil, err
	}
	return builder.BytesWithTrailer(root, root), nil
}

// GetFieldValue returns the value for a map key using a precomputed hash.
func GetFieldValue(doc []byte, root uint32, key string, hash uint32) (tron.Value, bool, error) {
	header, _, err := tron.NodeSliceAt(doc, root)
	if err != nil {
		return tron.Value{}, false, err
	}
	if header.KeyType != tron.KeyMap {
		return tron.Value{}, false, ErrTronProxyRootNotMap
	}
	return tron.MapGetHashed(doc, root, []byte(key), hash)
}

// SetFieldValue updates a map key using a precomputed hash and returns the updated doc and root offset.
func SetFieldValue(doc []byte, root uint32, key string, hash uint32, value any) ([]byte, uint32, error) {
	if len(doc) == 0 {
		return nil, 0, ErrTronProxyNoData
	}
	builder, _, err := tron.NewBuilderFromDocument(doc)
	if err != nil {
		return nil, 0, err
	}
	header, _, err := tron.NodeSliceAt(doc, root)
	if err != nil {
		return nil, 0, err
	}
	if header.KeyType != tron.KeyMap {
		return nil, 0, ErrTronProxyRootNotMap
	}
	val, err := tronjson.ValueFromGo(builder, value)
	if err != nil {
		return nil, 0, err
	}
	newRoot, _, err := tron.MapSetNodeHashed(builder, root, []byte(key), hash, val)
	if err != nil {
		return nil, 0, err
	}
	return builder.BytesWithTrailer(newRoot, root), newRoot, nil
}
