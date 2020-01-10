package expr

type valueType int
type cmp int
type logic int

var (
	stringValue = valueType(0)
	numberValue = valueType(1)
	regexpValue = valueType(2)
)

var (
	equal    = cmp(0) // ==
	notEqual = cmp(1) // !=
	gt       = cmp(2) // >
	ge       = cmp(3) // >=
	lt       = cmp(4) // <
	le       = cmp(5) // <=
	in       = cmp(6) // in
	notIn    = cmp(7) // !in
	match    = cmp(8) // ~
	notMatch = cmp(9) // !~
)

var (
	and = logic(0)
	or  = logic(1)
)

// Expr expr
type Expr interface {
	Exec(interface{}) (interface{}, error)
}

// VarExprFactory factory method
type VarExprFactory func([]byte, string) (Expr, error)

type stack struct {
	nodes []*node
}

func (s *stack) push(v *node) {
	s.nodes = append(s.nodes, v)
}

func (s *stack) append(v *node) {
	s.current().add(v)
	s.push(v)
}

func (s *stack) appendWithOP(fn CalcFunc, v *node) {
	s.current().appendWithOP(fn, v)
	s.push(v)
}

func (s *stack) current() *node {
	return s.nodes[len(s.nodes)-1]
}

func (s *stack) pop() Expr {
	n := len(s.nodes) - 1
	v := s.nodes[n]
	s.nodes[n] = nil
	s.nodes = s.nodes[:n]
	return v
}

type node struct {
	exprs []Expr
	fns   []CalcFunc
}

func (n *node) add(expr Expr) {
	n.exprs = append(n.exprs, expr)
}

func (n *node) append(expr Expr) {
	n.exprs = append(n.exprs, expr)
}

func (n *node) appendWithOP(fn CalcFunc, expr Expr) {
	n.exprs = append(n.exprs, expr)
	n.fns = append(n.fns, fn)
}

func (n *node) Exec(ctx interface{}) (interface{}, error) {
	left, err := n.exprs[0].Exec(ctx)
	if err != nil {
		return nil, err
	}

	for idx, right := range n.exprs[1:] {
		left, err = n.fns[idx](left, right, ctx)
		if err != nil {
			return nil, err
		}
	}

	return left, nil
}

type constString struct {
	value string
}

func (expr *constString) Exec(ctx interface{}) (interface{}, error) {
	return expr.value, nil
}

type constInt64 struct {
	value int64
}

func (expr *constInt64) Exec(ctx interface{}) (interface{}, error) {
	return expr.value, nil
}
