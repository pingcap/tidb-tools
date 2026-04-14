package schemacmp

import (
	"strings"

	tidbcharset "github.com/pingcap/tidb/pkg/parser/charset"
)

type charsetLattice struct {
	value string
	kind  charsetKind
}

type charsetKind struct {
	family charsetFamily
	key    string
}

const (
	charsetKeyLatin1  = tidbcharset.CharsetLatin1
	charsetKeyUTF8    = tidbcharset.CharsetUTF8
	charsetKeyUTF8MB4 = tidbcharset.CharsetUTF8MB4
)

type charsetFamily int

const (
	charsetFamilyOther charsetFamily = iota
	charsetFamilyLatin1
	charsetFamilyUTF8
	charsetFamilyUTF8MB4
)

// Charset is a lattice for comparing/joining character sets.
// It supports the ordering: latin1 < utf8mb4 and utf8(utf8mb3) < utf8mb4.
// Other charsets are only comparable when identical.
func Charset(cs string) Lattice {
	normalized := strings.ToLower(cs)
	if normalized == tidbcharset.CharsetUTF8MB3 {
		normalized = tidbcharset.CharsetUTF8
	}

	switch normalized {
	case tidbcharset.CharsetLatin1:
		return charsetLattice{value: cs, kind: charsetKind{family: charsetFamilyLatin1, key: charsetKeyLatin1}}
	case tidbcharset.CharsetUTF8:
		return charsetLattice{value: cs, kind: charsetKind{family: charsetFamilyUTF8, key: charsetKeyUTF8}}
	case tidbcharset.CharsetUTF8MB4:
		return charsetLattice{value: cs, kind: charsetKind{family: charsetFamilyUTF8MB4, key: charsetKeyUTF8MB4}}
	default:
		// Caller should always pass an explicit charset. Unrecognized values are treated as "other".
		return charsetLattice{value: cs, kind: charsetKind{family: charsetFamilyOther, key: normalized}}
	}
}

func (a charsetLattice) Unwrap() interface{} {
	return a.kind.key
}

func (a charsetLattice) Compare(other Lattice) (int, error) {
	b, ok := other.(charsetLattice)
	if !ok {
		return 0, typeMismatchError(a, other)
	}

	if a.kind == b.kind {
		return 0, nil
	}

	switch {
	case a.kind.family == charsetFamilyUTF8 && b.kind.family == charsetFamilyUTF8MB4:
		return -1, nil
	case a.kind.family == charsetFamilyUTF8MB4 && b.kind.family == charsetFamilyUTF8:
		return 1, nil
	case a.kind.family == charsetFamilyLatin1 && b.kind.family == charsetFamilyUTF8MB4:
		return -1, nil
	case a.kind.family == charsetFamilyUTF8MB4 && b.kind.family == charsetFamilyLatin1:
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
	value string
	kind  collationKind
}

type collationKind struct {
	family collationFamily
	suffix string
	key    string
}

type collationFamily int

const (
	collationFamilyOther collationFamily = iota
	collationFamilyLatin1
	collationFamilyUTF8
	collationFamilyUTF8MB4
)

// Collation is a lattice for comparing/joining collations.
// It supports the ordering:
//   - latin1_<suffix> < utf8mb4_<suffix>
//   - utf8_<suffix> < utf8mb4_<suffix>
//
// (same suffix only).
// Other collations are only comparable when identical.
func Collation(co string) Lattice {
	normalized := strings.ToLower(co)
	if strings.HasPrefix(normalized, "utf8mb3_") {
		normalized = "utf8_" + strings.TrimPrefix(normalized, "utf8mb3_")
	}

	switch {
	case strings.HasPrefix(normalized, "utf8mb4_"):
		suffix := strings.TrimPrefix(normalized, "utf8mb4_")
		return collationLattice{value: co, kind: collationKind{family: collationFamilyUTF8MB4, suffix: suffix, key: "utf8mb4_" + suffix}}
	case strings.HasPrefix(normalized, "utf8_"):
		suffix := strings.TrimPrefix(normalized, "utf8_")
		return collationLattice{value: co, kind: collationKind{family: collationFamilyUTF8, suffix: suffix, key: "utf8_" + suffix}}
	case strings.HasPrefix(normalized, "latin1_"):
		suffix := strings.TrimPrefix(normalized, "latin1_")
		return collationLattice{value: co, kind: collationKind{family: collationFamilyLatin1, suffix: suffix, key: "latin1_" + suffix}}
	default:
		// Caller should always pass an explicit collation. Unrecognized values are treated as "other".
		return collationLattice{value: co, kind: collationKind{family: collationFamilyOther, key: normalized}}
	}
}

func (a collationLattice) Unwrap() interface{} {
	return a.kind.key
}

func (a collationLattice) Compare(other Lattice) (int, error) {
	b, ok := other.(collationLattice)
	if !ok {
		return 0, typeMismatchError(a, other)
	}

	if a.kind == b.kind {
		return 0, nil
	}

	// If caller passed an unrecognized collation, treat it as "other" and make it incomparable with valid ones.
	if a.kind.family == collationFamilyOther || b.kind.family == collationFamilyOther {
		return 0, distinctSingletonsErrors(a.value, b.value)
	}

	// Only latin1/utf8/utf8mb4 with the same suffix are ordered.
	if a.kind.suffix != b.kind.suffix {
		return 0, distinctSingletonsErrors(a.value, b.value)
	}

	switch {
	case a.kind.family == collationFamilyUTF8 && b.kind.family == collationFamilyUTF8MB4:
		return -1, nil
	case a.kind.family == collationFamilyUTF8MB4 && b.kind.family == collationFamilyUTF8:
		return 1, nil
	case a.kind.family == collationFamilyLatin1 && b.kind.family == collationFamilyUTF8MB4:
		return -1, nil
	case a.kind.family == collationFamilyUTF8MB4 && b.kind.family == collationFamilyLatin1:
		return 1, nil
	default:
		return 0, distinctSingletonsErrors(a.value, b.value)
	}
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
