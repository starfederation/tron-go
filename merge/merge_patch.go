package merge

import (
	"fmt"

	tron "github.com/starfederation/tron-go"
)

// ApplyMergePatch applies JSON Merge Patch semantics to a target document.
// If the patch is not a map, the patch replaces the target.
func ApplyMergePatch(target, patch []byte) ([]byte, error) {
	if _, err := tron.DetectDocType(patch); err != nil {
		return nil, err
	}
	patchTrailer, err := tron.ParseTrailer(patch)
	if err != nil {
		return nil, err
	}
	patchRoot, err := tron.DecodeValueAt(patch, patchTrailer.RootOffset)
	if err != nil {
		return nil, err
	}
	if patchRoot.Type != tron.TypeMap {
		return patch, nil
	}

	if _, err := tron.DetectDocType(target); err != nil {
		return nil, err
	}

	var builder *tron.Builder
	var baseRoot uint32
	var prevRoot uint32

	targetTrailer, err := tron.ParseTrailer(target)
	if err != nil {
		return nil, err
	}
	targetRoot, err := tron.DecodeValueAt(target, targetTrailer.RootOffset)
	if err != nil {
		return nil, err
	}
	if targetRoot.Type == tron.TypeMap {
		builder, _, err = tron.NewBuilderFromDocument(target)
		if err != nil {
			return nil, err
		}
		baseRoot = targetRoot.Offset
		prevRoot = targetTrailer.RootOffset
	}

	if builder == nil {
		builder = tron.NewBuilder()
		empty, err := tron.EmptyMapRoot(builder)
		if err != nil {
			return nil, err
		}
		baseRoot = empty
		prevRoot = 0
	}

	applier := mergePatchApplier{
		builder: builder,
		patch:   patch,
	}
	root, err := applier.applyMapPatch(baseRoot, patchRoot.Offset)
	if err != nil {
		return nil, err
	}
	return builder.BytesWithTrailer(root, prevRoot), nil
}

type mergePatchApplier struct {
	builder *tron.Builder
	patch   []byte
}

func (a *mergePatchApplier) applyMapPatch(targetOff, patchOff uint32) (uint32, error) {
	header, node, err := tron.NodeSliceAt(a.patch, patchOff)
	if err != nil {
		return 0, err
	}
	if header.KeyType != tron.KeyMap {
		return 0, fmt.Errorf("merge patch expects map nodes")
	}
	if header.Kind == tron.NodeLeaf {
		leaf, err := tron.ParseMapLeafNode(a.patch, node)
		if err != nil {
			return 0, err
		}
		defer tron.ReleaseMapLeafNode(&leaf)
		off := targetOff
		for _, entry := range leaf.Entries {
			switch entry.Value.Type {
			case tron.TypeNil:
				newOff, _, err := tron.MapDelNode(a.builder, off, entry.Key)
				if err != nil {
					return 0, err
				}
				off = newOff
			case tron.TypeMap:
				targetVal, ok, err := tron.MapGet(a.builder.Buffer(), off, entry.Key)
				if err != nil {
					return 0, err
				}
				var merged uint32
				if ok && targetVal.Type == tron.TypeMap {
					merged, err = a.applyMapPatch(targetVal.Offset, entry.Value.Offset)
					if err != nil {
						return 0, err
					}
				} else {
					merged, err = tron.CloneMapNode(a.patch, entry.Value.Offset, a.builder)
					if err != nil {
						return 0, err
					}
				}
				newOff, _, err := tron.MapSetNode(a.builder, off, entry.Key, tron.Value{Type: tron.TypeMap, Offset: merged})
				if err != nil {
					return 0, err
				}
				off = newOff
			case tron.TypeArr:
				cloned, err := tron.CloneArrayNode(a.patch, entry.Value.Offset, a.builder)
				if err != nil {
					return 0, err
				}
				newOff, _, err := tron.MapSetNode(a.builder, off, entry.Key, tron.Value{Type: tron.TypeArr, Offset: cloned})
				if err != nil {
					return 0, err
				}
				off = newOff
			default:
				val, err := tron.CloneValueFromDoc(a.patch, entry.Value, a.builder)
				if err != nil {
					return 0, err
				}
				newOff, _, err := tron.MapSetNode(a.builder, off, entry.Key, val)
				if err != nil {
					return 0, err
				}
				off = newOff
			}
		}
		return off, nil
	}

	branch, err := tron.ParseMapBranchNode(node)
	if err != nil {
		return 0, err
	}
	defer tron.ReleaseMapBranchNode(&branch)
	off := targetOff
	for _, child := range branch.Children {
		newOff, err := a.applyMapPatch(off, child)
		if err != nil {
			return 0, err
		}
		off = newOff
	}
	return off, nil
}
