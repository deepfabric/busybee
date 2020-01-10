package expr

type symbolTable struct {
	tokens map[int]string
	items  []*item
}

func (st *symbolTable) addSymbol(symbol []byte, token int) {
	st.tokens[token] = string(symbol)

	for _, item := range st.items {
		if item.add(symbol, token) {
			return
		}
	}

	st.items = append(st.items, newItem(symbol, token))
}

func (st *symbolTable) findToken(value []byte) (int, bool) {
	maybe := false
	for _, item := range st.items {
		token, ok := item.find(value)
		if token > 0 {
			return token, ok
		}

		if ok {
			maybe = true
		}
	}

	return 0, maybe
}

type item struct {
	char  byte
	token int
	items []*item
}

func newItem(symbol []byte, token int) *item {
	var v *item
	var root *item
	for idx, c := range symbol {
		if v == nil {
			v = &item{char: c}
			root = v
		} else {
			v.items = append(v.items, &item{char: c})
			v = v.items[0]
		}

		if idx == len(symbol)-1 {
			v.token = token
		}
	}
	return root
}

func (i *item) add(symbol []byte, token int) bool {
	if i.char != symbol[0] {
		return false
	}

	if len(symbol) == 1 {
		i.token = token
		return true
	}

	symbol = symbol[1:]
	for _, item := range i.items {
		if item.add(symbol, token) {
			return true
		}
	}

	i.items = append(i.items, newItem(symbol, token))
	return true
}

func (i *item) find(symbol []byte) (int, bool) {
	if i.char != symbol[0] {
		return 0, false
	}

	if len(symbol) == 1 {
		return i.token, len(i.items) > 0
	}

	maybe := false
	symbol = symbol[1:]
	for _, item := range i.items {
		token, ok := item.find(symbol)
		if token > 0 {
			return token, ok
		}

		if ok {
			maybe = ok
		}
	}

	return 0, maybe
}
