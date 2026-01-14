package main

import (
	"bufio"
	"bytes"
	_ "embed"
	"errors"
	"fmt"
	"go/ast"
	"go/format"
	"go/token"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"text/template"

	tron "github.com/starfederation/tron-go"
	"golang.org/x/tools/go/packages"
)

type packageInfo struct {
	Dir     string
	Name    string
	Structs []structInfo
}

type structInfo struct {
	Name   string
	Fields []fieldInfo
}

type fieldInfo struct {
	Name           string
	Type           string
	JSONName       string
	Ident          string
	IsPointer      bool
	IsSlice        bool
	IsRuneSlice    bool
	SliceElem      string
	IsProxy        bool
	ProxyType      string
	ReturnType     string
	SetterType     string
	SetterVariadic bool
	HashHex        string
}

//go:embed templates/tron_gen.gotemplate
var tronGenTemplate string

func findModuleRoot(start string) (string, string, error) {
	dir := start
	for {
		modPath := filepath.Join(dir, "go.mod")
		data, err := os.ReadFile(modPath)
		if err == nil {
			modulePath, err := parseModulePath(data)
			if err != nil {
				return "", "", err
			}
			return dir, modulePath, nil
		}
		if !errors.Is(err, os.ErrNotExist) {
			return "", "", err
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", "", fmt.Errorf("go.mod not found starting from %s", start)
		}
		dir = parent
	}
}

func parseModulePath(data []byte) (string, error) {
	scanner := bufio.NewScanner(bytes.NewReader(data))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "//") {
			continue
		}
		if strings.HasPrefix(line, "module ") {
			fields := strings.Fields(line)
			if len(fields) >= 2 {
				return fields[1], nil
			}
			return "", fmt.Errorf("module declaration malformed")
		}
	}
	if err := scanner.Err(); err != nil {
		return "", err
	}
	return "", fmt.Errorf("module path not found in go.mod")
}

func collectPackageInfos(root string) ([]*packageInfo, error) {
	dirs := make(map[string]struct{})
	walkErr := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if shouldSkipDir(d.Name()) {
				return fs.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(d.Name(), ".go") {
			return nil
		}
		if strings.HasSuffix(d.Name(), "_test.go") {
			return nil
		}
		dirs[filepath.Dir(path)] = struct{}{}
		return nil
	})
	if walkErr != nil {
		return nil, walkErr
	}

	var infos []*packageInfo
	for dir := range dirs {
		pkgInfos, err := parsePackageDir(dir)
		if err != nil {
			return nil, err
		}
		infos = append(infos, pkgInfos...)
	}

	sort.Slice(infos, func(i, j int) bool {
		if infos[i].Dir == infos[j].Dir {
			return infos[i].Name < infos[j].Name
		}
		return infos[i].Dir < infos[j].Dir
	})
	return infos, nil
}

func parsePackageDir(dir string) ([]*packageInfo, error) {
	cfg := &packages.Config{
		Mode: packages.NeedName | packages.NeedSyntax | packages.NeedFiles,
		Dir:  dir,
	}
	pkgs, err := packages.Load(cfg, ".")
	if err != nil {
		return nil, err
	}

	var infos []*packageInfo
	for _, pkg := range pkgs {
		if len(pkg.Errors) > 0 {
			if isSkippablePackageErrors(pkg.Errors) {
				log.Printf("trongen: skipping %s (no buildable Go files for current tags)", dir)
				continue
			}
			return nil, fmt.Errorf("package load error in %s: %v", dir, pkg.Errors[0])
		}
		if pkg.Name == "" {
			continue
		}
		if strings.HasSuffix(pkg.Name, "_test") {
			continue
		}
		info := &packageInfo{Dir: dir, Name: pkg.Name}
		typeNames := make(map[string]struct{})
		var candidates []structInfo
		for _, file := range pkg.Syntax {
			if pkg.Fset != nil {
				filename := pkg.Fset.Position(file.Pos()).Filename
				if filename != "" {
					base := filepath.Base(filename)
					switch {
					case strings.HasSuffix(base, "_test.go"):
						continue
					case strings.HasSuffix(base, "_tron_gen.go"):
						continue
					case strings.HasSuffix(base, "tron_gen.go"):
						continue
					}
				}
			}
			ast.Inspect(file, func(n ast.Node) bool {
				ts, ok := n.(*ast.TypeSpec)
				if !ok {
					return true
				}
				typeNames[ts.Name.Name] = struct{}{}
				st, ok := ts.Type.(*ast.StructType)
				if !ok {
					return false
				}
				if ts.TypeParams != nil && len(ts.TypeParams.List) > 0 {
					log.Printf("trongen: skipping %s in %s (generic structs not supported)", ts.Name.Name, dir)
					return false
				}
				fields, err := collectTaggedFields(pkg.Fset, st)
				if err != nil {
					log.Printf("trongen: skipping %s in %s (field parse error: %v)", ts.Name.Name, dir, err)
					return false
				}
				fields = filterReservedFieldNames(ts.Name.Name, fields, dir)
				if len(fields) == 0 {
					return false
				}
				candidates = append(candidates, structInfo{Name: ts.Name.Name, Fields: fields})
				return false
			})
		}

		for _, candidate := range candidates {
			tronName := candidate.Name + "TRON"
			if _, exists := typeNames[tronName]; exists {
				log.Printf("trongen: skipping %s in %s (type %s already exists)", candidate.Name, dir, tronName)
				continue
			}
			if strings.HasSuffix(candidate.Name, "TRON") {
				log.Printf("trongen: skipping %s in %s (name already ends with TRON)", candidate.Name, dir)
				continue
			}
			info.Structs = append(info.Structs, candidate)
		}

		sort.Slice(info.Structs, func(i, j int) bool {
			return info.Structs[i].Name < info.Structs[j].Name
		})
		applyProxyTypes(info)
		infos = append(infos, info)
	}

	return infos, nil
}

func isSkippablePackageErrors(errs []packages.Error) bool {
	if len(errs) == 0 {
		return false
	}
	for _, err := range errs {
		msg := strings.ToLower(err.Msg)
		if strings.Contains(msg, "build constraints exclude all go files") {
			continue
		}
		if strings.Contains(msg, "no go files") {
			continue
		}
		return false
	}
	return true
}

func collectTaggedFields(fset *token.FileSet, st *ast.StructType) ([]fieldInfo, error) {
	var fields []fieldInfo
	for _, field := range st.Fields.List {
		if field.Tag == nil || len(field.Names) == 0 {
			continue
		}
		tagValue, err := strconv.Unquote(field.Tag.Value)
		if err != nil {
			continue
		}
		tag := reflect.StructTag(tagValue)
		jsonTag := tag.Get("json")
		if jsonTag == "" {
			continue
		}
		jsonName := strings.Split(jsonTag, ",")[0]
		if jsonName == "-" {
			continue
		}
		typ, err := formatNode(fset, field.Type)
		if err != nil {
			return nil, err
		}
		ident, isPtr := fieldIdent(field.Type)
		elemType, isSlice := sliceElemType(fset, field.Type)
		isRuneSlice := isSlice && elemType == "rune"
		for _, name := range field.Names {
			key := jsonName
			if key == "" {
				key = name.Name
			}
			fields = append(fields, fieldInfo{
				Name:        name.Name,
				Type:        typ,
				JSONName:    key,
				Ident:       ident,
				IsPointer:   isPtr,
				IsSlice:     isSlice,
				IsRuneSlice: isRuneSlice,
				SliceElem:   elemType,
				HashHex:     fmt.Sprintf("0x%08x", tron.XXH32([]byte(key), 0)),
			})
		}
	}
	return fields, nil
}

func filterReservedFieldNames(structName string, fields []fieldInfo, dir string) []fieldInfo {
	reserved := map[string]struct{}{
		"FromTRON":       {},
		"TRON":           {},
		"Raw":            {},
		"FullCopyToTRON": {},
	}
	var out []fieldInfo
	for _, field := range fields {
		if _, ok := reserved[field.Name]; ok {
			log.Printf("trongen: skipping field %s.%s in %s (reserved method name)", structName, field.Name, dir)
			continue
		}
		out = append(out, field)
	}
	return out
}

func applyProxyTypes(info *packageInfo) {
	available := make(map[string]struct{}, len(info.Structs))
	for _, st := range info.Structs {
		available[st.Name] = struct{}{}
	}
	for i := range info.Structs {
		for j := range info.Structs[i].Fields {
			field := &info.Structs[i].Fields[j]
			field.ReturnType = field.Type
			field.SetterType = field.Type
			field.SetterVariadic = false
			if field.IsSlice {
				field.SetterType = field.SliceElem
				field.SetterVariadic = true
			}
			if field.IsRuneSlice {
				field.ReturnType = "string"
				field.SetterType = "string"
				field.SetterVariadic = false
			}
			if field.Ident == "" || field.IsPointer {
				continue
			}
			if _, ok := available[field.Ident]; !ok {
				continue
			}
			field.IsProxy = true
			field.ProxyType = field.Ident + "TRON"
			field.ReturnType = "*" + field.ProxyType
		}
	}
}

func fieldIdent(expr ast.Expr) (string, bool) {
	switch t := expr.(type) {
	case *ast.Ident:
		return t.Name, false
	case *ast.StarExpr:
		if ident, ok := t.X.(*ast.Ident); ok {
			return ident.Name, true
		}
	}
	return "", false
}

func sliceElemType(fset *token.FileSet, expr ast.Expr) (string, bool) {
	arr, ok := expr.(*ast.ArrayType)
	if !ok || arr.Len != nil {
		return "", false
	}
	elem, err := formatNode(fset, arr.Elt)
	if err != nil {
		return "", false
	}
	if elem == "byte" || elem == "uint8" {
		return "", false
	}
	return elem, true
}

func formatNode(fset *token.FileSet, node ast.Node) (string, error) {
	var buf bytes.Buffer
	if fset == nil {
		fset = token.NewFileSet()
	}
	if err := format.Node(&buf, fset, node); err != nil {
		return "", err
	}
	return buf.String(), nil
}

func shouldSkipDir(name string) bool {
	if strings.HasPrefix(name, ".") || strings.HasPrefix(name, "_") {
		return true
	}
	switch name {
	case "vendor", "node_modules", "tron-shared", "testdata":
		return true
	default:
		return false
	}
}

func generatePackage(info *packageInfo, moduleRoot, modulePath string) ([]byte, error) {
	moduleRoot = filepath.Clean(moduleRoot)
	info.Dir = filepath.Clean(info.Dir)

	isRootPackage := info.Dir == moduleRoot
	tronPrefix := "tron."
	var imports []string
	if isRootPackage {
		tronPrefix = ""
		imports = append(imports, "errors", `stdjson "encoding/json"`)
	} else {
		imports = append(imports, fmt.Sprintf("%q", modulePath))
	}
	sort.Strings(imports)

	var buf bytes.Buffer
	tmpl, err := template.New("tron_gen").Parse(tronGenTemplate)
	if err != nil {
		return nil, err
	}
	if err := tmpl.Execute(&buf, templateData{
		PackageName:   info.Name,
		Imports:       imports,
		Structs:       info.Structs,
		TronPrefix:    tronPrefix,
		IsRootPackage: isRootPackage,
	}); err != nil {
		return nil, err
	}

	formatted, err := format.Source(buf.Bytes())
	if err != nil {
		return nil, err
	}
	return formatted, nil
}

type templateData struct {
	PackageName   string
	Imports       []string
	Structs       []structInfo
	TronPrefix    string
	IsRootPackage bool
}

func writeFileIfChanged(filePath string, data []byte) (bool, error) {
	existing, err := os.ReadFile(filePath)
	if err == nil && bytes.Equal(existing, data) {
		return false, nil
	}
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return false, err
	}
	if err := os.WriteFile(filePath, data, 0o644); err != nil {
		return false, err
	}
	return true, nil
}

func removeGeneratedFile(dir string) (bool, error) {
	filePath := filepath.Join(dir, "tron_gen.go")
	data, err := os.ReadFile(filePath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		return false, err
	}
	if !bytes.HasPrefix(data, []byte("// Code generated by trongen; DO NOT EDIT.")) {
		return false, nil
	}
	if err := os.Remove(filePath); err != nil {
		return false, err
	}
	return true, nil
}
