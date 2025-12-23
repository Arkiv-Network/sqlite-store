package query

import (
	"log/slog"
	"slices"
	"strings"

	"github.com/alecthomas/participle/v2"
	"github.com/alecthomas/participle/v2/lexer"
)

const AnnotationIdentRegex string = `[\p{L}_][\p{L}\p{N}_]*`

// Define the lexer with distinct tokens for each operator and parentheses.
var lex = lexer.MustSimple([]lexer.SimpleRule{
	{Name: "Whitespace", Pattern: `[ \t\n\r]+`},
	{Name: "LParen", Pattern: `\(`},
	{Name: "RParen", Pattern: `\)`},
	{Name: "And", Pattern: `&&`},
	{Name: "Or", Pattern: `\|\|`},
	{Name: "Neq", Pattern: `!=`},
	{Name: "Eq", Pattern: `=`},
	{Name: "Geqt", Pattern: `>=`},
	{Name: "Leqt", Pattern: `<=`},
	{Name: "Gt", Pattern: `>`},
	{Name: "Lt", Pattern: `<`},
	{Name: "NotGlob", Pattern: `!~`},
	{Name: "Glob", Pattern: `~`},
	{Name: "Not", Pattern: `!`},
	{Name: "EntityKey", Pattern: `0x[a-fA-F0-9]{64}`},
	{Name: "Address", Pattern: `0x[a-fA-F0-9]{40}`},
	{Name: "String", Pattern: `"(?:[^"\\]|\\.)*"`},
	{Name: "Number", Pattern: `[0-9]+`},
	{Name: "Ident", Pattern: AnnotationIdentRegex},
	// Meta-annotations, should start with $
	{Name: "Owner", Pattern: `\$owner`},
	{Name: "Creator", Pattern: `\$creator`},
	{Name: "Key", Pattern: `\$key`},
	{Name: "Expiration", Pattern: `\$expiration`},
	{Name: "Sequence", Pattern: `\$sequence`},
	{Name: "All", Pattern: `\$all`},
	{Name: "Star", Pattern: `\*`},
})

type TopLevel struct {
	Expression *Expression `parser:"@@ | All | Star"`
}

func (t *TopLevel) Normalise() *TopLevel {
	if t.Expression != nil {
		return &TopLevel{
			Expression: t.Expression.Normalise(),
		}
	}
	return t
}

// Expression is the top-level rule.
type Expression struct {
	Or OrExpression `parser:"@@"`
}

func (e *Expression) Normalise() *Expression {
	normalised := e.Or.Normalise()
	// Remove unneeded OR+AND nodes that both only contain a single child
	// when that child is a parenthesised expression
	if len(normalised.Right) == 0 && len(normalised.Left.Right) == 0 && normalised.Left.Left.Paren != nil {
		// This has already been normalised by the call above, so any negation has
		// been pushed into the leaf expressions and we can safely strip away the
		// parentheses
		return &normalised.Left.Left.Paren.Nested
	}
	return &Expression{
		Or: *normalised,
	}
}

func (e *Expression) invert() *Expression {

	newLeft := e.Or.invert()

	if len(newLeft.Right) == 0 {
		// By construction, this will always be a Paren
		if newLeft.Left.Paren == nil {
			panic("This should never happen!")
		}
		return &newLeft.Left.Paren.Nested
	}

	return &Expression{
		Or: OrExpression{
			Left: *newLeft,
		},
	}
}

// OrExpression handles expressions connected with ||.
type OrExpression struct {
	Left  AndExpression `parser:"@@"`
	Right []*OrRHS      `parser:"@@*"`
}

func (e *OrExpression) Normalise() *OrExpression {
	var newRight []*OrRHS = nil

	exprs := e.Left.Normalise()
	for _, rhs := range e.Right {
		exprs = append(exprs, rhs.Normalise()...)
	}

	for _, rhs := range exprs[1:] {
		if newRight == nil {
			newRight = []*OrRHS{}
		}
		newRight = append(newRight, &OrRHS{
			Expr: rhs,
		})
	}

	return &OrExpression{
		Left:  exprs[0],
		Right: newRight,
	}
}

func (e *OrExpression) invert() *AndExpression {
	newLeft := EqualExpr{
		Paren: &Paren{
			IsNot: false,
			Nested: Expression{
				Or: *e.Left.invert(),
			},
		},
	}

	var newRight []*AndRHS = nil

	if e.Right != nil {
		newRight = make([]*AndRHS, 0, len(e.Right))
		for _, rhs := range e.Right {
			newRight = append(newRight, rhs.invert())
		}
	}

	return &AndExpression{
		Left:  newLeft,
		Right: newRight,
	}
}

// OrRHS represents the right-hand side of an OR.
type OrRHS struct {
	Expr AndExpression `parser:"(Or | 'OR' | 'or') @@"`
}

func (e *OrRHS) Normalise() []AndExpression {
	return e.Expr.Normalise()
}

func (e *OrRHS) invert() *AndRHS {
	return &AndRHS{
		Expr: EqualExpr{
			Paren: &Paren{
				IsNot: false,
				Nested: Expression{
					Or: *e.Expr.invert(),
				},
			},
		},
	}
}

// AndExpression handles expressions connected with &&.
type AndExpression struct {
	Left  EqualExpr `parser:"@@"`
	Right []*AndRHS `parser:"@@*"`
}

func (e *AndExpression) Normalise() []AndExpression {

	terms := e.Left.Normalise()

	if e.Right != nil {
		for _, rhs := range e.Right {
			terms = append(terms, rhs.Normalise()...)
		}
	}

	// At this point, every term is either:
	// * a paren containing a disjunction with multiple conjunctions
	// * a simple expression (no paren)
	//
	// The case of a paren containing a disjunction with a single
	// conjunction already gets simplified when we normalise such parens.

	exprs := [][][]EqualExpr{}
	for _, term := range terms {

		es := [][]EqualExpr{}

		if term.Paren != nil {
			// This paren should always be a disjunction with at least 2 terms,
			// since the paren was already normalised
			if len(term.Paren.Nested.Or.Right) == 0 {
				panic("AndExpression::Normalise: unnormalised paren: this is a bug! ")
			}

			ts := []EqualExpr{term.Paren.Nested.Or.Left.Left}
			for _, t := range term.Paren.Nested.Or.Left.Right {
				ts = append(ts, t.Expr)
			}

			es = append(es, ts)

			for _, rhs := range term.Paren.Nested.Or.Right {
				ts := []EqualExpr{rhs.Expr.Left}
				for _, t := range rhs.Expr.Right {
					ts = append(ts, t.Expr)
				}
				es = append(es, ts)
			}
		} else {
			es = append(es, []EqualExpr{term})
		}

		exprs = append(exprs, es)
	}

	// Calculate the cross product, this distributes the outer AND into the OR
	product := [][]EqualExpr{{}}

	for _, a := range exprs {
		results := [][]EqualExpr{}
		for _, r := range product {
			for _, b := range a {
				result := slices.Clone(r)
				result = append(result, b...)
				results = append(results, result)
			}
		}
		product = results
	}

	conjunctions := []AndExpression{}
	for _, p := range product {
		var right []*AndRHS
		for _, r := range p[1:] {
			if right == nil {
				right = []*AndRHS{}
			}
			right = append(right, &AndRHS{
				Expr: r,
			})
		}
		conjunctions = append(conjunctions, AndExpression{
			Left:  p[0],
			Right: right,
		})
	}

	return conjunctions
}

func (e *AndExpression) invert() *OrExpression {
	newLeft := AndExpression{
		Left: *e.Left.invert(),
	}

	var newRight []*OrRHS = nil

	if e.Right != nil {
		newRight = make([]*OrRHS, 0, len(e.Right))
		for _, rhs := range e.Right {
			newRight = append(newRight, rhs.invert())
		}
	}

	return &OrExpression{
		Left:  newLeft,
		Right: newRight,
	}
}

// AndRHS represents the right-hand side of an AND.
type AndRHS struct {
	Expr EqualExpr `parser:"(And | 'AND' | 'and') @@"`
}

func (e *AndRHS) Normalise() []EqualExpr {
	terms := []EqualExpr{}
	terms = append(terms, e.Expr.Normalise()...)
	return terms
}

func (e *AndRHS) invert() *OrRHS {
	return &OrRHS{
		Expr: AndExpression{
			Left: *e.Expr.invert(),
		},
	}
}

// EqualExpr can be either an equality or a parenthesized expression.
type EqualExpr struct {
	Paren *Paren `parser:"  @@"`

	Assign    *Equality  `parser:"| @@"`
	Inclusion *Inclusion `parser:"| @@"`

	LessThan           *LessThan           `parser:"| @@"`
	LessOrEqualThan    *LessOrEqualThan    `parser:"| @@"`
	GreaterThan        *GreaterThan        `parser:"| @@"`
	GreaterOrEqualThan *GreaterOrEqualThan `parser:"| @@"`
	Glob               *Glob               `parser:"| @@"`
}

// Normalise on an EqualExpr can return multiple EqualExpr if the expression
// was a Paren with only nested conjunctions that was simplified.
func (e *EqualExpr) Normalise() []EqualExpr {

	if e.Paren != nil {
		normalised := e.Paren.Normalise()

		// Remove parentheses that only contain a conjunction
		// (i.e. the OR only has a single child)
		if len(normalised.Nested.Or.Right) == 0 {
			// The terms in the nested conjunction
			right := normalised.Nested.Or.Left.Right
			// The exprs to return
			exprs := make([]EqualExpr, 0, 1+len(right))

			exprs = append(exprs, normalised.Nested.Or.Left.Left)

			for _, expr := range right {
				exprs = append(exprs, expr.Expr)
			}

			return exprs
		} else {
			return []EqualExpr{{Paren: normalised}}
		}
	}

	if e.LessThan != nil {
		return []EqualExpr{{LessThan: e.LessThan.Normalise()}}
	}

	if e.LessOrEqualThan != nil {
		return []EqualExpr{{LessOrEqualThan: e.LessOrEqualThan.Normalise()}}
	}

	if e.GreaterThan != nil {
		return []EqualExpr{{GreaterThan: e.GreaterThan.Normalise()}}
	}

	if e.GreaterOrEqualThan != nil {
		return []EqualExpr{{GreaterOrEqualThan: e.GreaterOrEqualThan.Normalise()}}
	}

	if e.Glob != nil {
		return []EqualExpr{{Glob: e.Glob.Normalise()}}
	}

	if e.Assign != nil {
		return []EqualExpr{{Assign: e.Assign.Normalise()}}
	}

	if e.Inclusion != nil {
		return []EqualExpr{{Inclusion: e.Inclusion.Normalise()}}
	}

	panic("This should not happen!")
}

func (e *EqualExpr) invert() *EqualExpr {
	if e.Paren != nil {
		return &EqualExpr{Paren: e.Paren.invert()}
	}

	if e.LessThan != nil {
		return &EqualExpr{GreaterOrEqualThan: e.LessThan.invert()}
	}

	if e.LessOrEqualThan != nil {
		return &EqualExpr{GreaterThan: e.LessOrEqualThan.invert()}
	}

	if e.GreaterThan != nil {
		return &EqualExpr{LessOrEqualThan: e.GreaterThan.invert()}
	}

	if e.GreaterOrEqualThan != nil {
		return &EqualExpr{LessThan: e.GreaterOrEqualThan.invert()}
	}

	if e.Glob != nil {
		return &EqualExpr{Glob: e.Glob.invert()}
	}

	if e.Assign != nil {
		return &EqualExpr{Assign: e.Assign.invert()}
	}

	if e.Inclusion != nil {
		return &EqualExpr{Inclusion: e.Inclusion.invert()}
	}

	panic("This should not happen!")
}

type Paren struct {
	IsNot  bool       `parser:"@(Not | 'NOT' | 'not')?"`
	Nested Expression `parser:"LParen @@ RParen"`
}

func (e *Paren) Normalise() *Paren {
	nested := e.Nested

	if e.IsNot {
		nested = *nested.invert()
	}

	return &Paren{
		IsNot:  false,
		Nested: *nested.Normalise(),
	}
}

func (e *Paren) invert() *Paren {
	return &Paren{
		IsNot:  !e.IsNot,
		Nested: e.Nested,
	}
}

type Glob struct {
	Var   string `parser:"@Ident"`
	IsNot bool   `parser:"((Glob | @NotGlob) | (@('NOT' | 'not')? ('GLOB' | 'glob')))"`
	Value string `parser:"@String"`
}

func (e *Glob) Normalise() *Glob {
	// TODO do we need to change casing here too?
	return e
}

func (e *Glob) invert() *Glob {
	return &Glob{
		Var:   e.Var,
		IsNot: !e.IsNot,
		Value: e.Value,
	}
}

type LessThan struct {
	Var   string `parser:"@Ident Lt"`
	Value Value  `parser:"@@"`
}

func (e *LessThan) Normalise() *LessThan {
	switch e.Var {
	case KeyAttributeKey, OwnerAttributeKey, CreatorAttributeKey:
		val := strings.ToLower(*e.Value.String)
		return &LessThan{
			Var: e.Var,
			Value: Value{
				String: &val,
			},
		}
	default:
		return e
	}
}

func (e *LessThan) invert() *GreaterOrEqualThan {
	return &GreaterOrEqualThan{
		Var:   e.Var,
		Value: e.Value,
	}
}

type LessOrEqualThan struct {
	Var   string `parser:"@Ident Leqt"`
	Value Value  `parser:"@@"`
}

func (e *LessOrEqualThan) Normalise() *LessOrEqualThan {
	switch e.Var {
	case KeyAttributeKey, OwnerAttributeKey, CreatorAttributeKey:
		val := strings.ToLower(*e.Value.String)
		return &LessOrEqualThan{
			Var: e.Var,
			Value: Value{
				String: &val,
			},
		}
	default:
		return e
	}
}

func (e *LessOrEqualThan) invert() *GreaterThan {
	return &GreaterThan{
		Var:   e.Var,
		Value: e.Value,
	}
}

type GreaterThan struct {
	Var   string `parser:"@Ident Gt"`
	Value Value  `parser:"@@"`
}

func (e *GreaterThan) Normalise() *GreaterThan {
	switch e.Var {
	case KeyAttributeKey, OwnerAttributeKey, CreatorAttributeKey:
		val := strings.ToLower(*e.Value.String)
		return &GreaterThan{
			Var: e.Var,
			Value: Value{
				String: &val,
			},
		}
	default:
		return e
	}
}

func (e *GreaterThan) invert() *LessOrEqualThan {
	return &LessOrEqualThan{
		Var:   e.Var,
		Value: e.Value,
	}
}

type GreaterOrEqualThan struct {
	Var   string `parser:"@Ident Geqt"`
	Value Value  `parser:"@@"`
}

func (e *GreaterOrEqualThan) Normalise() *GreaterOrEqualThan {
	switch e.Var {
	case KeyAttributeKey, OwnerAttributeKey, CreatorAttributeKey:
		val := strings.ToLower(*e.Value.String)
		return &GreaterOrEqualThan{
			Var: e.Var,
			Value: Value{
				String: &val,
			},
		}
	default:
		return e
	}
}

func (e *GreaterOrEqualThan) invert() *LessThan {
	return &LessThan{
		Var:   e.Var,
		Value: e.Value,
	}
}

// Equality represents a simple equality (e.g. name = 123).
type Equality struct {
	Var   string `parser:"@(Ident | Key | Owner | Creator | Expiration | Sequence)"`
	IsNot bool   `parser:"(Eq | @Neq)"`
	Value Value  `parser:"@@"`
}

func (e *Equality) Normalise() *Equality {
	switch e.Var {
	case KeyAttributeKey, OwnerAttributeKey, CreatorAttributeKey:
		val := strings.ToLower(*e.Value.String)
		return &Equality{
			Var:   e.Var,
			IsNot: e.IsNot,
			Value: Value{
				String: &val,
			},
		}
	default:
		return e
	}
}

func (e *Equality) invert() *Equality {
	return &Equality{
		Var:   e.Var,
		IsNot: !e.IsNot,
		Value: e.Value,
	}
}

type Inclusion struct {
	Var    string `parser:"@(Ident | Key | Owner | Creator | Expiration | Sequence)"`
	IsNot  bool   `parser:"(@('NOT'|'not')? ('IN'|'in'))"`
	Values Values `parser:"@@"`
}

func (e *Inclusion) Normalise() *Inclusion {
	switch e.Var {
	case KeyAttributeKey, OwnerAttributeKey, CreatorAttributeKey:
		vals := make([]string, 0, len(e.Values.Strings))
		for _, val := range e.Values.Strings {
			vals = append(vals, strings.ToLower(val))
		}
		return &Inclusion{
			Var: e.Var,
			Values: Values{
				Strings: vals,
			},
		}
	default:
		return e
	}
}

func (e *Inclusion) invert() *Inclusion {
	return &Inclusion{
		Var:    e.Var,
		IsNot:  !e.IsNot,
		Values: e.Values,
	}
}

// Value is a literal value (a number or a string).
type Value struct {
	String *string `parser:"  (@String | @EntityKey | @Address)"`
	Number *uint64 `parser:"| @Number"`
}

type Values struct {
	Strings []string `parser:"  '(' (@String | @EntityKey | @Address)+ ')'"`
	Numbers []uint64 `parser:"| '(' @Number+ ')'"`
}

var Parser = participle.MustBuild[TopLevel](
	participle.Lexer(lex),
	participle.Elide("Whitespace"),
	participle.Unquote("String"),
)

func Parse(s string, log *slog.Logger) (*TopLevel, error) {
	log.Info("parsing query", "query", s)

	v, err := Parser.ParseString("", s)
	if err != nil {
		return nil, err
	}
	return v.Normalise(), err
}
