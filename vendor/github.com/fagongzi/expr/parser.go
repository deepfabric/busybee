package expr

import (
	"fmt"
	"regexp"

	"github.com/fagongzi/util/format"
)

const (
	tokenUnknown    = 10000
	tokenLeftParen  = 1
	tokenRightParen = 2
	tokenVarStart   = 3
	tokenVarEnd     = 4
	tokenLiteral    = 5
	tokenRegexp     = 6
	tokenCustom     = 100

	slash                    = '\\'
	quotation                = '"'
	vertical                 = '|'
	quotationConversion byte = 0x00
	slashConversion     byte = 0x01
	verticalConversion  byte = 0x02
)

var (
	symbolLeftParen  = []byte("(")
	symbolRightParen = []byte(")")
	symbolVarStart   = []byte("{")
	symbolVarEnd     = []byte("}")
	symbolLiteral    = []byte{quotation}
	symbolRegexp     = []byte{vertical}
)

// CalcFunc a calc function returns a result
type CalcFunc func(interface{}, Expr, interface{}) (interface{}, error)

// Parser expr parser
type Parser interface {
	Parse([]byte) (Expr, error)
}

type parser struct {
	expr      *node
	stack     stack
	prevToken int
	lexer     Lexer
	template  *parserTemplate
}

type parserTemplate struct {
	opts            *options
	startToken      int
	startConversion byte
	opsTokens       map[int]string
	opsFunc         map[int]CalcFunc
	varTypes        map[int]VarType
	varTokens       map[int]string
	factory         VarExprFactory
}

// NewParser returns a expr parser
func NewParser(factory VarExprFactory, opts ...Option) Parser {
	p := &parserTemplate{
		opts:       newOptions(),
		factory:    factory,
		opsTokens:  make(map[int]string),
		opsFunc:    make(map[int]CalcFunc),
		varTypes:   make(map[int]VarType),
		varTokens:  make(map[int]string),
		startToken: tokenCustom,
	}

	for _, opt := range opts {
		opt(p.opts)
	}

	p.init()
	return p
}

func (p *parserTemplate) init() {
	for op, opFunc := range p.opts.ops {
		p.addOP(op, opFunc)
	}

	for symbol, valueType := range p.opts.typs {
		p.addVarType(symbol, valueType)
	}
}

func (p *parserTemplate) addOP(op string, calcFunc CalcFunc) {
	p.startToken++
	p.opsTokens[p.startToken] = op
	p.opsFunc[p.startToken] = calcFunc
}

func (p *parserTemplate) addVarType(symbol string, varType VarType) {
	p.startToken++
	p.varTokens[p.startToken] = symbol
	p.varTypes[p.startToken] = varType
}

func (p *parserTemplate) Parse(input []byte) (Expr, error) {
	return p.newParser(input).parse()
}

func (p *parserTemplate) registerInternal(lexer Lexer) {
	lexer.AddSymbol(symbolLeftParen, tokenLeftParen)
	lexer.AddSymbol(symbolRightParen, tokenRightParen)
	lexer.AddSymbol(symbolVarStart, tokenVarStart)
	lexer.AddSymbol(symbolVarEnd, tokenVarEnd)
	lexer.AddSymbol(symbolLiteral, tokenLiteral)
	lexer.AddSymbol(symbolRegexp, tokenRegexp)

	for tokenValue, token := range p.opsTokens {
		lexer.AddSymbol([]byte(token), tokenValue)
	}

	for tokenValue, token := range p.varTokens {
		p.startToken++
		lexer.AddSymbol([]byte(token), tokenValue)
	}
}

func (p *parserTemplate) newParser(input []byte) *parser {
	lexer := NewScanner(conversion(input))
	p.registerInternal(lexer)

	return &parser{
		expr:      &node{},
		prevToken: tokenUnknown,
		template:  p,
		lexer:     lexer,
	}
}

func (p *parser) parse() (Expr, error) {
	p.stack.push(p.expr)
	for {
		p.lexer.NextToken()
		token := p.lexer.Token()

		var err error
		if token == tokenLeftParen {
			err = p.doLeftParen()
		} else if token == tokenRightParen {
			err = p.doRightParen()
		} else if token == tokenVarStart {
			err = p.doVarStart()
			token = tokenVarEnd
		} else if token == tokenLiteral {
			err = p.doLiteral()
		} else if token == tokenRegexp {
			err = p.doRegexp()
		} else if _, ok := p.template.opsTokens[token]; ok {
			err = p.doOp()
		} else if _, ok := p.template.varTypes[token]; ok {
			err = p.doVarType()
		} else if token == TokenEOI {
			err = p.doEOI()
			if err != nil {
				return nil, err
			}

			return p.stack.pop(), nil
		}

		if err != nil {
			return nil, err
		}

		if token != tokenLiteral && token != tokenRegexp {
			p.prevToken = token
		}
	}
}

func (p *parser) doLeftParen() error {
	if p.prevToken == tokenUnknown { // (a+b)
		p.stack.append(&node{})
	} else if p.prevToken == tokenLeftParen { // ((a+b)*10)
		p.stack.append(&node{})
	} else if fn, ok := p.template.opsFunc[p.prevToken]; ok { // 10 * (a+b)
		p.stack.appendWithOP(fn, &node{})
	} else {
		return fmt.Errorf("unexpect token <%s> before %d",
			p.lexer.TokenSymbol(p.prevToken),
			p.lexer.TokenIndex())
	}

	p.lexer.SkipString()
	return nil
}

func (p *parser) doRightParen() error {
	var err error
	if p.prevToken == tokenRightParen || p.prevToken == tokenVarEnd { // (c + (a + b))
		p.stack.pop()
		p.lexer.SkipString()
	} else if fn, ok := p.template.opsFunc[p.prevToken]; ok { // (a + b)
		expr, err := newConstExpr(p.lexer.ScanString())
		if err != nil {
			return err
		}
		p.stack.current().appendWithOP(fn, expr)
		p.stack.pop()
	} else {
		return fmt.Errorf("unexpect token <%s> before %d",
			p.lexer.TokenSymbol(p.prevToken),
			p.lexer.TokenIndex())
	}

	return err
}

func (p *parser) doVarStart() error {
	if p.prevToken == tokenUnknown { // {
		p.stack.append(&node{})
	} else if p.prevToken == tokenLeftParen { // ({
		p.stack.append(&node{})
	} else if fn, ok := p.template.opsFunc[p.prevToken]; ok { // a + {
		p.stack.appendWithOP(fn, &node{})
	} else {
		return fmt.Errorf("unexpect token <%s> before %d",
			p.lexer.TokenSymbol(p.prevToken),
			p.lexer.TokenIndex())
	}

	p.lexer.SkipString()

	varType := p.template.opts.defaultType
	for {
		p.lexer.NextToken()
		token := p.lexer.Token()
		if token == TokenEOI {
			return fmt.Errorf("missing }")
		} else if t, ok := p.template.varTypes[token]; ok {
			varType = t
			p.lexer.SkipString()
		} else if p.lexer.Token() == tokenVarEnd {
			break
		}
	}

	varExpr, err := p.template.factory(p.lexer.ScanString(), varType)
	if err != nil {
		return err
	}

	p.stack.current().append(varExpr)
	p.stack.pop()
	return nil
}

func (p *parser) doLiteral() error {
	if _, ok := p.template.opsFunc[p.prevToken]; ok { // a + "
		for {
			p.lexer.NextToken()
			if p.lexer.Token() == TokenEOI {
				return fmt.Errorf("missing \"")
			} else if p.lexer.Token() == tokenLiteral {
				break
			}
		}
	} else {
		return fmt.Errorf("unexpect token <%s> before %d",
			p.lexer.TokenSymbol(p.prevToken),
			p.lexer.TokenIndex())
	}

	return nil
}

func (p *parser) doRegexp() error {
	if _, ok := p.template.opsFunc[p.prevToken]; ok { // a + /
		for {
			p.lexer.NextToken()
			if p.lexer.Token() == TokenEOI {
				return fmt.Errorf("missing /")
			} else if p.lexer.Token() == tokenRegexp {
				break
			}
		}
	} else {
		return fmt.Errorf("unexpect token <%s> before %d",
			p.lexer.TokenSymbol(p.prevToken),
			p.lexer.TokenIndex())
	}

	return nil
}

func (p *parser) doVarEnd() error {
	varType := p.template.opts.defaultType
	if p.prevToken == tokenVarStart { // {a}

	} else if t, ok := p.template.varTypes[p.prevToken]; ok {
		varType = t
	} else {
		return fmt.Errorf("unexpect token <%s> before %d",
			p.lexer.TokenSymbol(p.prevToken),
			p.lexer.TokenIndex())
	}

	varExpr, err := p.template.factory(p.lexer.ScanString(), varType)
	if err != nil {
		return err
	}

	p.stack.current().append(varExpr)
	p.stack.pop()
	return nil
}

func (p *parser) doOp() error {
	var err error
	if p.prevToken == tokenUnknown { // 1 +
		expr, err := newConstExpr(p.lexer.ScanString())
		if err != nil {
			return err
		}
		p.stack.current().append(expr)
	} else if p.prevToken == tokenLeftParen { // (a+
		expr, err := newConstExpr(p.lexer.ScanString())
		if err != nil {
			return err
		}
		p.stack.current().append(expr)
	} else if p.prevToken == tokenRightParen { // (a+1) +
		p.lexer.SkipString()
	} else if fn, ok := p.template.opsFunc[p.prevToken]; ok { // a + b +
		expr, err := newConstExpr(p.lexer.ScanString())
		if err != nil {
			return err
		}
		p.stack.current().appendWithOP(fn, expr)
	} else if p.prevToken == tokenVarEnd { // {a} +
		p.lexer.SkipString()
	} else {
		return fmt.Errorf("unexpect token <%s> before %d",
			p.lexer.TokenSymbol(p.prevToken),
			p.lexer.TokenIndex())
	}

	return err
}

func (p *parser) doVarType() error {
	switch p.prevToken {
	case tokenVarStart:
	default:
		return fmt.Errorf("unexpect token <%s> before %d",
			p.lexer.TokenSymbol(p.prevToken),
			p.lexer.TokenIndex())
	}

	p.lexer.SkipString()
	return nil
}

func (p *parser) doEOI() error {
	if p.prevToken == tokenRightParen || p.prevToken == tokenVarEnd { // (a+b)

	} else if fn, ok := p.template.opsFunc[p.prevToken]; ok { // a + b
		expr, err := newConstExpr(p.lexer.ScanString())
		if err != nil {
			return err
		}
		p.stack.current().appendWithOP(fn, expr)
	} else {
		return fmt.Errorf("unexpect token <%s> before %d",
			p.lexer.TokenSymbol(p.prevToken),
			p.lexer.TokenIndex())
	}

	return nil
}

func newConstExpr(value []byte) (Expr, error) {
	if len(value) >= 2 && value[0] == quotation && value[len(value)-1] == quotation {
		return &constString{
			value: string(revertConversion(value[1 : len(value)-1])),
		}, nil
	}

	if len(value) >= 2 && value[0] == vertical && value[len(value)-1] == vertical {
		pattern, err := regexp.Compile(string(revertConversion(value[1 : len(value)-1])))
		if err != nil {
			return nil, err
		}

		return &constRegexp{
			value: pattern,
		}, nil
	}

	strValue := string(value)
	int64Value, err := format.ParseStrInt64(strValue)
	if err != nil {
		return &constString{
			value: strValue,
		}, nil
	}

	return &constInt64{
		value: int64Value,
	}, nil
}

func revertConversion(src []byte) []byte {
	var dst []byte
	for _, v := range src {
		if v == slashConversion {
			dst = append(dst, slash)
		} else if v == quotationConversion {
			dst = append(dst, quotation)
		} else if v == verticalConversion {
			dst = append(dst, vertical)
		} else {
			dst = append(dst, v)
		}
	}

	return dst
}

func conversion(src []byte) []byte {
	// \" -> 0x00
	// \\ -> \
	// \/ -> 0x01
	var dst []byte
	for {
		if len(src) == 0 {
			return dst
		} else if len(src) == 1 {
			dst = append(dst, src...)
			return dst
		}

		if src[0] != slash {
			dst = append(dst, src[0])
			src = src[1:]
			continue
		}

		if src[0] == slash && src[1] == slash {
			dst = append(dst, slashConversion)
		} else if src[0] == slash && src[1] == quotation {
			dst = append(dst, quotationConversion)
		} else if src[0] == slash && src[1] == vertical {
			dst = append(dst, verticalConversion)
		} else {
			dst = append(dst, src[0:2]...)
		}

		src = src[2:]
	}
}
