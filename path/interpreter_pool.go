package path

import "github.com/delaneyj/toolbelt"

var interpreterPool = toolbelt.New(func() *interpreter {
	return &interpreter{}
})

func getInterpreter() *interpreter {
	return interpreterPool.Get()
}

func putInterpreter(intr *interpreter) {
	if intr == nil {
		return
	}
	interpreterPool.Put(intr)
}
