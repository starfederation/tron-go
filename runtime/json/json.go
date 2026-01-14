package json

import (
	stdjson "encoding/json"
	"fmt"

	tron "github.com/starfederation/tron-go"
)

// Marshal encodes a Go value into a TRON document using JSON semantics.
func Marshal(v any) ([]byte, error) {
	data, err := stdjson.Marshal(v)
	if err != nil {
		return nil, err
	}
	return tron.FromJSON(data)
}

// Unmarshal decodes a TRON document into a Go value using JSON semantics.
func Unmarshal(doc []byte, v any) error {
	if v == nil {
		return fmt.Errorf("nil target")
	}
	value, err := documentToAny(doc)
	if err != nil {
		return err
	}
	data, err := stdjson.Marshal(value)
	if err != nil {
		return err
	}
	return stdjson.Unmarshal(data, v)
}

// UnmarshalValue decodes a TRON value into a Go value using JSON semantics.
func UnmarshalValue(doc []byte, v tron.Value, out any) error {
	if out == nil {
		return fmt.Errorf("nil target")
	}
	value, err := valueToAny(doc, v)
	if err != nil {
		return err
	}
	data, err := stdjson.Marshal(value)
	if err != nil {
		return err
	}
	return stdjson.Unmarshal(data, out)
}

// ValueFromGo encodes a Go value into a TRON value stored in builder.
func ValueFromGo(builder *tron.Builder, v any) (tron.Value, error) {
	if builder == nil {
		return tron.Value{}, fmt.Errorf("nil builder")
	}
	doc, err := Marshal(v)
	if err != nil {
		return tron.Value{}, err
	}
	docType, err := tron.DetectDocType(doc)
	if err != nil {
		return tron.Value{}, err
	}
	switch docType {
	case tron.DocScalar:
		return tron.DecodeScalarDocument(doc)
	case tron.DocTree:
		tr, err := tron.ParseTrailer(doc)
		if err != nil {
			return tron.Value{}, err
		}
		header, _, err := tron.NodeSliceAt(doc, tr.RootOffset)
		if err != nil {
			return tron.Value{}, err
		}
		var val tron.Value
		switch header.KeyType {
		case tron.KeyMap:
			val = tron.Value{Type: tron.TypeMap, Offset: tr.RootOffset}
		case tron.KeyArr:
			val = tron.Value{Type: tron.TypeArr, Offset: tr.RootOffset}
		default:
			return tron.Value{}, fmt.Errorf("unknown root node type")
		}
		return tron.CloneValueFromDoc(doc, val, builder)
	default:
		return tron.Value{}, fmt.Errorf("unknown document type")
	}
}

func documentToAny(doc []byte) (any, error) {
	docType, err := tron.DetectDocType(doc)
	if err != nil {
		return nil, err
	}
	switch docType {
	case tron.DocScalar:
		val, err := tron.DecodeScalarDocument(doc)
		if err != nil {
			return nil, err
		}
		return valueToAny(doc, val)
	case tron.DocTree:
		tr, err := tron.ParseTrailer(doc)
		if err != nil {
			return nil, err
		}
		header, _, err := tron.NodeSliceAt(doc, tr.RootOffset)
		if err != nil {
			return nil, err
		}
		switch header.KeyType {
		case tron.KeyMap:
			return mapToAny(doc, tr.RootOffset)
		case tron.KeyArr:
			return arrayToAny(doc, tr.RootOffset)
		default:
			return nil, fmt.Errorf("unknown root node type")
		}
	default:
		return nil, fmt.Errorf("unknown document type")
	}
}

func valueToAny(doc []byte, v tron.Value) (any, error) {
	switch v.Type {
	case tron.TypeNil:
		return nil, nil
	case tron.TypeBit:
		return v.Bool, nil
	case tron.TypeI64:
		return v.I64, nil
	case tron.TypeF64:
		return v.F64, nil
	case tron.TypeTxt:
		return string(v.Bytes), nil
	case tron.TypeBin:
		return v.Bytes, nil
	case tron.TypeArr:
		return arrayToAny(doc, v.Offset)
	case tron.TypeMap:
		return mapToAny(doc, v.Offset)
	default:
		return nil, fmt.Errorf("unknown value type %d", v.Type)
	}
}

func mapToAny(doc []byte, off uint32) (map[string]any, error) {
	out := make(map[string]any)
	if err := mapFill(doc, off, out); err != nil {
		return nil, err
	}
	return out, nil
}

func mapFill(doc []byte, off uint32, out map[string]any) error {
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
			val, err := valueToAny(doc, entry.Value)
			if err != nil {
				return err
			}
			out[string(entry.Key)] = val
		}
		return nil
	}
	branch, err := tron.ParseMapBranchNode(node)
	if err != nil {
		return err
	}
	defer tron.ReleaseMapBranchNode(&branch)
	for _, child := range branch.Children {
		if err := mapFill(doc, child, out); err != nil {
			return err
		}
	}
	return nil
}

func arrayToAny(doc []byte, off uint32) ([]any, error) {
	length, err := tron.ArrayRootLength(doc, off)
	if err != nil {
		return nil, err
	}
	out := make([]any, length)
	for i := uint32(0); i < length; i++ {
		val, ok, err := tron.ArrGet(doc, off, i)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, fmt.Errorf("array index missing: %d", i)
		}
		conv, err := valueToAny(doc, val)
		if err != nil {
			return nil, err
		}
		out[i] = conv
	}
	return out, nil
}
