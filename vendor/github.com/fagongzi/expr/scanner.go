package expr

type scanner struct {
	len   int
	input []byte

	token      int
	bp         int
	sp         int
	scanOffset int
	ch         byte

	st *symbolTable
}

// NewScanner returns a scanner
func NewScanner(input []byte) Lexer {
	scan := &scanner{
		len:   len(input),
		input: input,
		bp:    -1,
		sp:    0,
		st: &symbolTable{
			tokens: make(map[int]string),
		},
	}

	scan.Next()

	return scan
}

func (scan *scanner) AddSymbol(symbol []byte, token int) {
	scan.st.addSymbol(symbol, token)
}

func (scan *scanner) Next() byte {
	if scan.bp >= scan.len {
		scan.ch = EOI
		return scan.ch
	}

	for {
		scan.bp++

		if scan.bp < scan.len {
			scan.ch = scan.input[scan.bp]
		} else {
			scan.ch = EOI
		}

		return scan.ch
	}
}

func (scan *scanner) NextToken() {
	for {
		if scan.ch == EOI {
			scan.token = TokenEOI
			scan.scanOffset = 0
			return
		}

		scan.skipWhitespaces()

		token := scan.findLongestToken()
		if token > 0 {
			scan.token = token
			scan.scanOffset = len(scan.TokenSymbol(token))
			scan.Next()
			return
		}

		scan.Next()
	}
}

func (scan *scanner) Current() byte {
	return scan.ch
}

func (scan *scanner) Token() int {
	return scan.token
}

func (scan *scanner) TokenIndex() int {
	return scan.bp - 1
}

func (scan *scanner) TokenSymbol(token int) string {
	return scan.st.tokens[token]
}

func (scan *scanner) ScanString() []byte {
	value := scan.input[scan.sp : scan.bp-scan.scanOffset]
	scan.sp = scan.bp

	found := false
	for i, ch := range value {
		if !isWhitespace(ch) {
			value = value[i:]
			found = true
			break
		}
	}

	if !found {
		return nil
	}

	for i := len(value) - 1; i >= 0; i-- {
		if !isWhitespace(value[i]) {
			value = value[0 : i+1]
			found = true
			break
		}
	}

	return value
}

func (scan *scanner) SkipString() {
	scan.sp = scan.bp
}

func (scan *scanner) findLongestToken() int {
	last := -1
	pos := -1

	for i := scan.bp; i < scan.len; i++ {
		token, maybe := scan.st.findToken(scan.input[scan.bp : i+1])
		if token > 0 {
			last = token
			pos = i
		}

		if !maybe {
			break
		}
	}

	if last > 0 {
		scan.bp = pos
		scan.ch = scan.input[pos]
	}

	return last
}

func (scan *scanner) skipWhitespaces() {
	for {
		if isWhitespace(scan.ch) {
			scan.Next()
			continue
		}

		break
	}
}

func isWhitespace(ch byte) bool {
	return ch == ' ' || ch == '\n' || ch == '\r' || ch == '\t' || ch == '\f' || ch == '\b'
}
