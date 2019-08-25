package sqlgen

import (
	"fmt"
	"io"
	"strconv"
	"strings"
	"unicode"
)

// Production is a representation production in bnf file.
type Production struct {
	head     string
	maxLoop  int
	bodyList BodyList
}

func (p *Production) String() string {
	var sb strings.Builder
	sb.WriteString(p.head)
	writeOptNum(&sb, p.maxLoop)
	sb.WriteString(": ")
	firstBody := p.bodyList[0]
	sb.WriteString(strings.Join(firstBody.seq, " "))

	if isLiteral(firstBody.seq[0]) {
		writeOptNum(&sb, firstBody.randomFactor)
	}

	for i := 1; i < len(p.bodyList); i++ {
		body := p.bodyList[i]
		sb.WriteString("\n")
		sb.WriteString("| ")
		sb.WriteString(strings.Join(body.seq, " "))
		if isLiteral(firstBody.seq[0]) {
			writeOptNum(&sb, firstBody.randomFactor)
		}
	}

	sb.WriteString("\n")
	return sb.String()
}

func writeOptNum(sb *strings.Builder, i int) {
	sb.WriteString(" [")
	sb.WriteString(strconv.Itoa(i))
	sb.WriteString("]")
}

// Body is a branch production.
type Body = struct {
	seq          []string
	randomFactor int
}

// BodyList is a list of body.
type BodyList = []Body

// Scanner implements the yyLexer interface.
type Scanner struct {
	s        string
	curPos   int
	startPos int

	q quote

	errs  []error
	warns []error
}

// Errors returns the errors and warns during a scan.
func (s *Scanner) Errors() (warns []error, errs []error) {
	return s.warns, s.errs
}

// Errorf tells scanner something is wrong.
// Scanner satisfies yyLexer interface which need this function.
func (s *Scanner) Errorf(format string, a ...interface{}) (err error) {
	str := fmt.Sprintf(format, a...)
	val := s.s[s.startPos:]
	err = fmt.Errorf("column %d near \"%s\"%s",
		s.curPos, val, str)
	return
}

// AppendError sets error into scanner.
// Scanner satisfies yyLexer interface which need this function.
func (s *Scanner) AppendError(err error) {
	if err == nil {
		return
	}
	s.errs = append(s.errs, err)
}

func (s *Scanner) readRune() (rune, error) {
	if s.curPos >= len(s.s) {
		return 0, io.EOF
	}
	ret := s.s[s.curPos]
	s.curPos++
	return rune(ret), nil
}

func (s *Scanner) unreadRune() error {
	if s.curPos > 0 {
		s.curPos--
		return nil
	}
	return io.ErrNoProgress
}

// Lex returns a token and store the token Value in v.
// Scanner satisfies yyLexer interface.
// 0 and invalid are special token id this function would return:
// return 0 tells parser that scanner meets EOF,
// return invalid tells parser that scanner meets illegal character.
func (s *Scanner) Lex(v *yySymType) int {
	var r rune
	var err error
	s.startPos = s.curPos
	// Skip spaces.
	for {
		r, err = s.readRune()
		panicIfNonEOF(err)
		if err == io.EOF {
			return 0
		}
		if !unicode.IsSpace(r) || s.q.isInsideStr() {
			break
		}
		s.startPos++
	}

	// Handle delimiter.
	if !s.q.isInsideStr() {
		if r == ':' {
			v.ident = ":"
			return Colon
		} else if r == '|' {
			v.ident = "|"
			return OrBranch
		} else if r == '[' {
			v.ident = "["
			return LeftBr
		} else if r == ']' {
			v.ident = "]"
			return RightBr
		}
	}

	// Toggle isInsideStr.
	if r == '\'' || r == '"' {
		s.q.tryToggle(r)
	}

	// Handle identifier.
	stringBuf := string(r)
	for {
		r, err = s.readRune()
		panicIfNonEOF(err)
		if err == io.EOF {
			break
		}
		if (unicode.IsSpace(r) || isDelimiter(r) || isBracket(r)) && !s.q.isInsideStr() {
			if err := s.unreadRune(); err != nil {
				panic(fmt.Sprintf("Unable to unread rune: %s.", string(r)))
			}
			break
		}
		stringBuf += string(r)

		// Handle end str.
		if r == '\'' || r == '"' {
			if !s.q.isInsideStr() {
				panic(fmt.Sprintf("unexpected character: `%s` after `%s`", string(r), stringBuf))
			}
			if s.q.tryToggle(r) {
				break
			}
		}
	}
	v.ident = s.s[s.startPos:s.curPos]
	return identifier
}

// reset resets the sql string to be scanned.
func (s *Scanner) reset(str string) {
	s.s = str
	s.curPos = 0
	s.startPos = 0
	s.errs = s.errs[:0]
	s.warns = s.warns[:0]
}

// Parser represents a parser instance. Some temporary objects are stored in it to reduce object allocation during Parse function.
type Parser struct {
	result *Production
	src    string
	lexer  Scanner

	// the following fields are used by yyParse to reduce allocation.
	cache  []yySymType
	yylval yySymType
	yyVAL  yySymType
}

// NewParser returns a Parser object.
func NewParser() *Parser {
	return &Parser{
		cache: make([]yySymType, 200),
	}
}

// Parse converts a bnf string into a list of Production.
func (parser *Parser) Parse(bnf string) (result *Production, warns []error, err error) {
	parser.src = bnf
	parser.result = nil

	var l yyLexer
	parser.lexer.reset(bnf)
	l = &parser.lexer
	yyParse(l, parser)

	warns, errs := l.Errors()
	if len(warns) > 0 {
		warns = append([]error(nil), warns...)
	} else {
		warns = nil
	}
	if len(errs) != 0 {
		return nil, warns, errs[0]
	}
	return parser.result, warns, nil
}

func panicIfNonEOF(err error) {
	if err != nil && err != io.EOF {
		panic(fmt.Sprintf("unknown error: %v", err))
	}
}

func isDelimiter(r rune) bool {
	return r == '|' || r == ':'
}

func isBracket(r rune) bool {
	return r == '[' || r == ']'
}

type quote struct {
	c rune
}

func (q *quote) isInsideStr() bool {
	return q.c != 0
}

func (q *quote) tryToggle(other rune) bool {
	if q.c == 0 {
		q.c = other
		return true
	} else if q.c == other {
		q.c = 0
		return true
	}
	return false
}
