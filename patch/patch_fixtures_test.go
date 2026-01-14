package patch

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"

	tron "github.com/starfederation/tron-go"
)

type patchCase struct {
	Name   string          `json:"name"`
	Target json.RawMessage `json:"target"`
	Patch  json.RawMessage `json:"patch"`
	Expect json.RawMessage `json:"expect"`
	Error  string          `json:"error"`
}

func findPatchCasesPath() (string, error) {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		return "", errors.New("unable to locate test file path")
	}
	dir := filepath.Dir(file)
	for i := 0; i < 10; i++ {
		candidates := []string{
			filepath.Join(dir, "shared", "testdata", "patch", "patch_cases.json"),
			filepath.Join(dir, "tron-shared", "shared", "testdata", "patch", "patch_cases.json"),
		}
		for _, candidate := range candidates {
			if _, err := os.Stat(candidate); err == nil {
				return candidate, nil
			}
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	return "", errors.New("patch_cases.json not found")
}

func docToAny(doc []byte) (any, error) {
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
		switch val.Type {
		case tron.TypeNil:
			return nil, nil
		case tron.TypeBit:
			return val.Bool, nil
		case tron.TypeI64:
			return val.I64, nil
		case tron.TypeF64:
			return val.F64, nil
		case tron.TypeTxt:
			return string(val.Bytes), nil
		case tron.TypeBin:
			return val.Bytes, nil
		default:
			return nil, fmt.Errorf("unsupported scalar type %d", val.Type)
		}
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
		case tron.KeyArr:
			return tron.Value{Type: tron.TypeArr, Offset: tr.RootOffset}.AsArray(doc)
		case tron.KeyMap:
			return tron.Value{Type: tron.TypeMap, Offset: tr.RootOffset}.AsObject(doc)
		default:
			return nil, fmt.Errorf("unknown root key type")
		}
	default:
		return nil, fmt.Errorf("unknown document type")
	}
}

func TestPatchFixtures(t *testing.T) {
	path, err := findPatchCasesPath()
	if err != nil {
		t.Fatalf("locate fixtures: %v", err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read fixtures: %v", err)
	}

	var cases []patchCase
	if err := json.Unmarshal(raw, &cases); err != nil {
		t.Fatalf("parse fixtures: %v", err)
	}
	if len(cases) == 0 {
		t.Fatalf("no patch cases found")
	}

	for _, tc := range cases {
		t.Run(tc.Name, func(t *testing.T) {
			target, err := tron.FromJSON(tc.Target)
			if err != nil {
				t.Fatalf("FromJSON target: %v", err)
			}
			patchDoc, err := tron.FromJSON(tc.Patch)
			if err != nil {
				t.Fatalf("FromJSON patch: %v", err)
			}
			out, err := ApplyPatch(target, patchDoc)
			if tc.Error != "" {
				if err == nil {
					t.Fatalf("expected error %q", tc.Error)
				}
				if !strings.Contains(err.Error(), tc.Error) {
					t.Fatalf("error mismatch: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("ApplyPatch: %v", err)
			}
			if len(tc.Expect) == 0 {
				t.Fatalf("missing expected output")
			}
			expectedDoc, err := tron.FromJSON(tc.Expect)
			if err != nil {
				t.Fatalf("FromJSON expect: %v", err)
			}
			gotAny, err := docToAny(out)
			if err != nil {
				t.Fatalf("decode output: %v", err)
			}
			wantAny, err := docToAny(expectedDoc)
			if err != nil {
				t.Fatalf("decode expect: %v", err)
			}
			if !reflect.DeepEqual(gotAny, wantAny) {
				t.Fatalf("mismatch: got=%v want=%v", gotAny, wantAny)
			}
		})
	}
}
