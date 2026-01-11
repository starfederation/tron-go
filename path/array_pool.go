package path

import (
	"github.com/delaneyj/toolbelt"

	tron "github.com/starfederation/tron-go"
)

var (
	valueSlicePool = toolbelt.New(func() []tron.Value { return make([]tron.Value, 0, 16) })
	boolSlicePool  = toolbelt.New(func() []bool { return make([]bool, 0, 16) })
)

func getValueSlice(n int) []tron.Value {
	if n <= 0 {
		return nil
	}
	s := valueSlicePool.Get()
	if cap(s) < n {
		return make([]tron.Value, n)
	}
	return s[:n]
}

func putValueSlice(s []tron.Value) {
	if s == nil {
		return
	}
	for i := range s {
		s[i] = tron.Value{}
	}
	s = s[:0]
	valueSlicePool.Put(s)
}

func getBoolSlice(n int) []bool {
	if n <= 0 {
		return nil
	}
	s := boolSlicePool.Get()
	if cap(s) < n {
		return make([]bool, n)
	}
	return s[:n]
}

func putBoolSlice(s []bool) {
	if s == nil {
		return
	}
	clear(s)
	s = s[:0]
	boolSlicePool.Put(s)
}
