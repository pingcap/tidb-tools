// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package sqlgen

import (
	"bufio"
	"fmt"
	"os"
	"regexp"
	"strings"
	"unicode"
)

// BuildProdMap convert an array of production into a map of production, extracting the name of prodcution as key.
func BuildProdMap(prods []*Production) map[string]*Production {
	ret := make(map[string]*Production)
	for _, v := range prods {
		ret[v.head] = v
	}
	checkProductionMap(ret)
	return ret
}

func checkProductionMap(productionMap map[string]*Production) {
	for _, production := range productionMap {
		for _, seqs := range production.bodyList {
			for _, seq := range seqs.seq {
				if isLiteral(seq) {
					continue
				}
				if _, exist := productionMap[seq]; !exist {
					panic(fmt.Sprintf("Production '%s' not found", seq))
				}
			}
		}
	}
}

func breadthFirstSearch(prodName string, prodMap map[string]*Production, visitors ...func(*Production)) (map[string]struct{}, error) {
	resultSet := map[string]struct{}{}
	pendingSet := []string{prodName}

	for len(pendingSet) != 0 {
		name := pendingSet[0]
		pendingSet = pendingSet[1:]
		prod, ok := prodMap[name]
		if !ok {
			return nil, fmt.Errorf("%v not found", name)
		}

		if _, contains := resultSet[name]; !contains {
			resultSet[name] = struct{}{}
			for _, body := range prod.bodyList {
				for _, s := range body.seq {
					if !isLiteral(s) {
						pendingSet = append(pendingSet, s)
					}
				}
			}
			if len(visitors) != 0 {
				for _, v := range visitors {
					v(prod)
				}
			}
		}
	}
	return resultSet, nil
}

// ParseYacc parse the bnf file as an array of Production.
func ParseYacc(yaccFilePath string) ([]*Production, error) {
	file, err := os.Open(yaccFilePath)
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }()

	prodStrs := splitProdStr(bufio.NewReader(file))
	return parseProdStr(prodStrs)
}

func parseProdStr(prodStrs []string) ([]*Production, error) {
	bnfParser := NewParser()
	var ret []*Production
	for _, p := range prodStrs {
		r, _, err := bnfParser.Parse(p)
		if err != nil {
			return nil, err
		}
		ret = append(ret, r)
	}
	return ret, nil
}

func splitProdStr(prodReader *bufio.Reader) []string {
	var ret []string
	var sb strings.Builder
	time2Exit := false
	for !time2Exit {
		for {
			str, err := prodReader.ReadString('\n')
			if err != nil {
				time2Exit = true
				if !isWhitespace(str) {
					sb.WriteString(str)
				}
				break
			}
			if isWhitespace(str) && sb.Len() != 0 {
				ret = append(ret, sb.String())
				sb.Reset()
			} else {
				sb.WriteString(str)
			}
		}
	}
	if sb.Len() != 0 {
		ret = append(ret, sb.String())
	}
	return ret
}

func isWhitespace(str string) bool {
	for _, c := range str {
		if !unicode.IsSpace(c) {
			return false
		}
	}
	return true
}

func literal(token string) (string, bool) {
	if isLiteral(token) {
		return strings.Trim(token, "'"), true
	}
	return "", false
}

func isLiteral(token string) bool {
	return strings.HasPrefix(token, "'") && strings.HasSuffix(token, "'")
}

var idRegex = regexp.MustCompile("^[a-zA-Z][0-9a-zA-Z_]*")

func isIdentifier(str string) bool {
	rs := idRegex.ReplaceAllLiteralString(str, "")
	return len(rs) == 0
}

var reservedKeyword = map[string]struct{}{
	"break": {}, "case": {}, "chan": {}, "const": {},
	"continue": {}, "default": {}, "defer": {}, "else": {},
	"fallthrough": {}, "for": {}, "func": {}, "go": {},
	"goto": {}, "if": {}, "import": {}, "interface": {},
	"map": {}, "package": {}, "range": {}, "return": {},
	"select": {}, "struct": {}, "switch": {}, "type": {}, "var": {},
}

func isReservedKeyword(str string) bool {
	_, ok := reservedKeyword[str]
	return ok
}