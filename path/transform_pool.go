package path

import "github.com/delaneyj/toolbelt"

var (
	matchSlicePool    = toolbelt.New(func() []match { return make([]match, 0, 16) })
	pathStepSlicePool = toolbelt.New(func() []pathStep { return make([]pathStep, 0, 8) })
)

func getMatchSlice(n int) []match {
	s := matchSlicePool.Get()
	if cap(s) < n {
		return make([]match, n)
	}
	return s[:n]
}

func putMatchSlice(s []match) {
	if s == nil {
		return
	}
	for i := range s {
		s[i] = match{}
	}
	s = s[:0]
	matchSlicePool.Put(s)
}

func releaseMatches(matches []match) {
	if matches == nil {
		return
	}
	for i := range matches {
		putPathStepSlice(matches[i].path)
		matches[i] = match{}
	}
	matches = matches[:0]
	matchSlicePool.Put(matches)
}

func getPathStepSlice(n int) []pathStep {
	s := pathStepSlicePool.Get()
	if cap(s) < n {
		return make([]pathStep, n)
	}
	return s[:n]
}

func putPathStepSlice(s []pathStep) {
	if s == nil {
		return
	}
	for i := range s {
		s[i] = pathStep{}
	}
	s = s[:0]
	pathStepSlicePool.Put(s)
}
