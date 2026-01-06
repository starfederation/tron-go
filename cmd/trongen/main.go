package main

import (
	"log"
	"path/filepath"

	"github.com/alecthomas/kong"
)

type cli struct {
	Dir string `help:"Root directory to scan for Go packages." default:"."`
}

func main() {
	log.SetFlags(0)

	var args cli
	kong.Parse(&args,
		kong.Name("trongen"),
		kong.Description("Generate lazy TRON proxy structs for JSON-tagged Go structs."),
		kong.UsageOnError(),
	)

	absDir, err := filepath.Abs(args.Dir)
	if err != nil {
		log.Fatal(err)
	}

	moduleRoot, modulePath, err := findModuleRoot(absDir)
	if err != nil {
		log.Fatal(err)
	}

	infos, err := collectPackageInfos(absDir)
	if err != nil {
		log.Fatal(err)
	}

	wrote := 0
	removed := 0
	for _, info := range infos {
		if len(info.Structs) == 0 {
			wasRemoved, err := removeGeneratedFile(info.Dir)
			if err != nil {
				log.Fatal(err)
			}
			if wasRemoved {
				removed++
			}
			continue
		}

		src, err := generatePackage(info, moduleRoot, modulePath)
		if err != nil {
			log.Fatal(err)
		}

		outPath := filepath.Join(info.Dir, "tron_gen.go")
		changed, err := writeFileIfChanged(outPath, src)
		if err != nil {
			log.Fatal(err)
		}
		if changed {
			wrote++
		}
	}

	switch {
	case wrote == 0 && removed == 0:
		log.Printf("trongen: no changes")
	case wrote > 0 && removed > 0:
		log.Printf("trongen: wrote %d package(s), removed %d", wrote, removed)
	case wrote > 0:
		log.Printf("trongen: wrote %d package(s)", wrote)
	case removed > 0:
		log.Printf("trongen: removed %d package(s)", removed)
	}
}
