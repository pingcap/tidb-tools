package schemacmp

import (
	"strings"

	tidbcharset "github.com/pingcap/tidb/pkg/parser/charset"
)

type charsetLattice struct {
	value string
	kind  charsetKind
}

type charsetKind int

const (
	charsetKindEmpty charsetKind = iota
	charsetKindUTF8
	charsetKindUTF8MB4
	charsetKindOther
)

// Charset is a lattice for comparing/joining character sets.
// It supports the ordering: "" < utf8(utf8mb3) < utf8mb4.
// Other charsets are only comparable when identical.
func Charset(cs string) Lattice {
	value := strings.ToLower(cs)
	if value == tidbcharset.CharsetUTF8MB3 {
		value = tidbcharset.CharsetUTF8
	}

	switch value {
	case "":
		return charsetLattice{value: value, kind: charsetKindEmpty}
	case tidbcharset.CharsetUTF8:
		return charsetLattice{value: value, kind: charsetKindUTF8}
	case tidbcharset.CharsetUTF8MB4:
		return charsetLattice{value: value, kind: charsetKindUTF8MB4}
	default:
		return charsetLattice{value: value, kind: charsetKindOther}
	}
}

func (a charsetLattice) Unwrap() interface{} {
	return a.value
}

func (a charsetLattice) Compare(other Lattice) (int, error) {
	b, ok := other.(charsetLattice)
	if !ok {
		return 0, typeMismatchError(a, other)
	}

	if a.value == b.value {
		return 0, nil
	}

	// Only utf8/utf8mb4 (plus empty) are ordered.
	switch {
	case a.kind == charsetKindEmpty && (b.kind == charsetKindUTF8 || b.kind == charsetKindUTF8MB4):
		return -1, nil
	case b.kind == charsetKindEmpty && (a.kind == charsetKindUTF8 || a.kind == charsetKindUTF8MB4):
		return 1, nil
	case a.kind == charsetKindUTF8 && b.kind == charsetKindUTF8MB4:
		return -1, nil
	case a.kind == charsetKindUTF8MB4 && b.kind == charsetKindUTF8:
		return 1, nil
	default:
		return 0, distinctSingletonsErrors(a.value, b.value)
	}
}

func (a charsetLattice) Join(other Lattice) (Lattice, error) {
	b, ok := other.(charsetLattice)
	if !ok {
		return nil, typeMismatchError(a, other)
	}

	cmp, err := a.Compare(b)
	if err != nil {
		return nil, err
	}
	if cmp >= 0 {
		return a, nil
	}
	return b, nil
}

type collationLattice struct {
	value  string
	kind   collationKind
	suffix string
}

type collationKind int

const (
	collationKindEmpty collationKind = iota
	collationKindUTF8
	collationKindUTF8MB4
	collationKindOther
)

// Collation is a lattice for comparing/joining collations.
// It supports the ordering: utf8_<suffix> < utf8mb4_<suffix> (same suffix only).
// Other collations are only comparable when identical.
func Collation(co string) Lattice {
	value := strings.ToLower(co)
	if strings.HasPrefix(value, "utf8mb3_") {
		value = "utf8_" + strings.TrimPrefix(value, "utf8mb3_")
	}

	switch {
	case value == "":
		return collationLattice{value: value, kind: collationKindEmpty}
	case strings.HasPrefix(value, "utf8mb4_"):
		return collationLattice{
			value:  value,
			kind:   collationKindUTF8MB4,
			suffix: strings.TrimPrefix(value, "utf8mb4_"),
		}
	case strings.HasPrefix(value, "utf8_"):
		return collationLattice{
			value:  value,
			kind:   collationKindUTF8,
			suffix: strings.TrimPrefix(value, "utf8_"),
		}
	default:
		return collationLattice{value: value, kind: collationKindOther}
	}
}

func (a collationLattice) Unwrap() interface{} {
	return a.value
}

func (a collationLattice) Compare(other Lattice) (int, error) {
	b, ok := other.(collationLattice)
	if !ok {
		return 0, typeMismatchError(a, other)
	}

	if a.value == b.value {
		return 0, nil
	}

	// Collation without explicit value is not ordered (it depends on charset).
	if a.kind == collationKindEmpty || b.kind == collationKindEmpty {
		return 0, distinctSingletonsErrors(a.value, b.value)
	}

	// Only utf8/utf8mb4 with the same suffix are ordered.
	if (a.kind == collationKindUTF8 || a.kind == collationKindUTF8MB4) &&
		(b.kind == collationKindUTF8 || b.kind == collationKindUTF8MB4) {
		if a.suffix != b.suffix {
			return 0, distinctSingletonsErrors(a.value, b.value)
		}
		switch {
		case a.kind == collationKindUTF8 && b.kind == collationKindUTF8MB4:
			return -1, nil
		case a.kind == collationKindUTF8MB4 && b.kind == collationKindUTF8:
			return 1, nil
		default:
			// Same kind but different value cannot happen because suffix differs is handled above.
			return 0, distinctSingletonsErrors(a.value, b.value)
		}
	}

	return 0, distinctSingletonsErrors(a.value, b.value)
}

func (a collationLattice) Join(other Lattice) (Lattice, error) {
	b, ok := other.(collationLattice)
	if !ok {
		return nil, typeMismatchError(a, other)
	}

	cmp, err := a.Compare(b)
	if err != nil {
		return nil, err
	}
	if cmp >= 0 {
		return a, nil
	}
	return b, nil
}
