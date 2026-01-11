package path

import (
	"encoding/json"
	"fmt"
	"sync"
	"testing"

	tron "github.com/starfederation/tron-go"
)

var (
	benchTransformOnce      sync.Once
	benchTransformDeepDoc   []byte
	benchTransformLargeDoc  []byte
	benchTransformDeepExpr  *Expr
	benchTransformLargeExpr *Expr
	benchTransformSink      []byte
)

func BenchmarkTransformDeepPath(b *testing.B) {
	benchTransformInit()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		out, err := benchTransformDeepExpr.Transform(benchTransformDeepDoc, benchTransformIncrement)
		if err != nil {
			b.Fatal(err)
		}
		benchTransformSink = out
	}
}

func BenchmarkTransformLargeMatches(b *testing.B) {
	benchTransformInit()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		out, err := benchTransformLargeExpr.Transform(benchTransformLargeDoc, benchTransformIncrement)
		if err != nil {
			b.Fatal(err)
		}
		benchTransformSink = out
	}
}

func benchTransformInit() {
	benchTransformOnce.Do(func() {
		benchTransformDeepDoc = mustTransformDoc(buildDeepObject([]string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"}))
		benchTransformLargeDoc = mustTransformDoc(buildLargeObject(1000))
		benchTransformDeepExpr = mustTransformExpr("a.b.c.d.e.f.g.h.i.j")
		benchTransformLargeExpr = mustTransformExpr("items[*].value")
	})
}

func benchTransformIncrement(val tron.Value) (tron.Value, error) {
	switch val.Type {
	case tron.TypeI64:
		return tron.Value{Type: tron.TypeI64, I64: val.I64 + 1}, nil
	case tron.TypeF64:
		return tron.Value{Type: tron.TypeF64, F64: val.F64 + 1}, nil
	default:
		return val, nil
	}
}

func mustTransformExpr(expression string) *Expr {
	expr, err := Compile(expression)
	if err != nil {
		panic(err)
	}
	return expr
}

func mustTransformDoc(root any) []byte {
	raw, err := json.Marshal(root)
	if err != nil {
		panic(err)
	}
	doc, err := tron.FromJSON(raw)
	if err != nil {
		panic(err)
	}
	return doc
}

func buildDeepObject(keys []string) map[string]any {
	if len(keys) == 0 {
		panic("no keys")
	}
	var cur any = int64(1)
	for i := len(keys) - 1; i >= 0; i-- {
		cur = map[string]any{keys[i]: cur}
	}
	out, ok := cur.(map[string]any)
	if !ok {
		panic("deep object build failed")
	}
	return out
}

func buildLargeObject(count int) map[string]any {
	if count <= 0 {
		panic(fmt.Sprintf("invalid item count %d", count))
	}
	items := make([]map[string]any, count)
	for i := 0; i < count; i++ {
		items[i] = map[string]any{"value": int64(i)}
	}
	return map[string]any{"items": items}
}
