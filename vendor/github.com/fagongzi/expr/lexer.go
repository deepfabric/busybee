package expr

var (
	// EOI end of input
	EOI byte = 0x1A
	// TokenEOI eoi token
	TokenEOI = 0
)

// Lexer lexer to scan the input text
type Lexer interface {
	// AddSymbol add a symbol
	AddSymbol([]byte, int)
	// Next returns the next char
	Next() byte
	// NextToken scan the next token
	NextToken()
	// Current returns the current char
	Current() byte
	/// Token returns the current token
	Token() int
	// TokenIndex returns the index of the input chars
	TokenIndex() int
	// TokenSymbol returns token symbol
	TokenSymbol(int) string
	// ScanString returns the chars value between prev token and current token
	ScanString() []byte
	// SkipString move the sp to the current token
	SkipString()
}
