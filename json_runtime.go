package tron

import (
	stdjson "encoding/json"
	"fmt"
)

// Marshal encodes a Go value into a TRON document using JSON semantics.
func Marshal(v any) ([]byte, error) {
	data, err := stdjson.Marshal(v)
	if err != nil {
		return nil, err
	}
	return FromJSON(data)
}

// Unmarshal decodes a TRON document into a Go value using JSON semantics.
func Unmarshal(doc []byte, out any) error {
	if out == nil {
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
	return stdjson.Unmarshal(data, out)
}

// UnmarshalValue decodes a TRON value into a Go value using JSON semantics.
func UnmarshalValue(doc []byte, v Value, out any) error {
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
func ValueFromGo(builder *Builder, v any) (Value, error) {
	if builder == nil {
		return Value{}, fmt.Errorf("nil builder")
	}
	doc, err := Marshal(v)
	if err != nil {
		return Value{}, err
	}
	if _, err := DetectDocType(doc); err != nil {
		return Value{}, err
	}
	tr, err := ParseTrailer(doc)
	if err != nil {
		return Value{}, err
	}
	root, err := DecodeValueAt(doc, tr.RootOffset)
	if err != nil {
		return Value{}, err
	}
	return CloneValueFromDoc(doc, root, builder)
}

func documentToAny(doc []byte) (any, error) {
	if _, err := DetectDocType(doc); err != nil {
		return nil, err
	}
	tr, err := ParseTrailer(doc)
	if err != nil {
		return nil, err
	}
	root, err := DecodeValueAt(doc, tr.RootOffset)
	if err != nil {
		return nil, err
	}
	return valueToAny(doc, root)
}
