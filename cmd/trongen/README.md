# trongen

Generate lazy TRON proxy types for Go structs with `json` tags. The proxies convert
between TRON documents and Go values on demand.

## Install

- `task trongen:build` (writes `./bin/trongen`)
- `task trongen:install` (installs to `$GOBIN`)
- `go install ./cmd/trongen`

## Usage

```sh
trongen --dir .
```

`--dir` defaults to the current working directory. The generator scans packages
recursively, writes `tron_gen.go` next to matching structs, and removes the
generated file if no eligible structs remain.

## Example

Input structs (see `examples/trongen/basic/types.go`):

```go
package basic

type Address struct {
    Line1 string `json:"line1"`
    City  string `json:"city"`
    State string `json:"state"`
    Zip   string `json:"zip"`
}

type User struct {
    ID      string   `json:"id"`
    Name    string   `json:"name"`
    Email   string   `json:"email,omitempty"`
    Address Address  `json:"address"`
    Tags    []string `json:"tags,omitempty"`
}
```

Generate proxies:

```sh
go run ./cmd/trongen --dir ./examples/trongen/basic
```

Use the generated type:

```go
proxy, err := basic.UserFromTRON(tronDoc)
if err != nil {
    // handle error
}

id, ok, err := proxy.ID()
if err != nil {
    // handle error
}
if ok {
    _ = id
}

address, ok, err := proxy.Address()
if err != nil {
    // handle error
}
if ok {
    _ = address
}

if err := proxy.SetID("user-1"); err != nil {
    // handle error
}

if err := proxy.SetTags("math", "research"); err != nil {
    // handle error
}

doc, err := proxy.TRON()
if err != nil {
    // handle error
}
```

Or encode directly from a struct:

```go
user := &basic.User{Name: "Ada"}
proxy, err := user.FullCopyToTRON()
if err != nil {
    // handle error
}
doc, err := proxy.TRON()
if err != nil {
    // handle error
}
```
