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

type AST struct {
	Expr *ASTExpr
}
type ASTExpr struct {
	Or ASTOr
}
type ASTOr struct {
	Terms []ASTAnd
}
type ASTAnd struct {
	Terms []ASTTerm
}
type ASTTerm struct {
	Assign             *Equality
	Inclusion          *Inclusion
	LessThan           *LessThan
	LessOrEqualThan    *LessOrEqualThan
	GreaterThan        *GreaterThan
	GreaterOrEqualThan *GreaterOrEqualThan
	Glob               *Glob
}

type TopLevel struct {
	Expression *Expression `parser:"@@ | All | Star"`
}

func (t *TopLevel) Normalise() *AST {
	if t.Expression != nil {
		return &AST{
			Expr: t.Expression.Normalise(),
		}
	}
	return &AST{}
}

// Expression is the top-level rule.
type Expression struct {
	Or OrExpression `parser:"@@"`
}

func (e *Expression) Normalise() *ASTExpr {
	normalised := e.Or.Normalise()
	return &ASTExpr{
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

func (e *OrExpression) Normalise() *ASTOr {
	terms := e.Left.Normalise()
	for _, rhs := range e.Right {
		terms = append(terms, rhs.Normalise()...)
	}

	return &ASTOr{
		Terms: terms,
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

func (e *OrRHS) Normalise() []ASTAnd {
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

func (e *EqualExpr) convertToTerms() [][]ASTTerm {
	// First level is OR, second level is AND
	es := [][]ASTTerm{}

	if e.Paren != nil {
		// This is where we recursively convert to DNF and also where negations
		// get pushed down
		normalised := e.Paren.Normalise()
		for _, conjunction := range normalised.Or.Terms {
			// Add one array per OR term, containing all the AND terms
			es = append(es, conjunction.Terms)
		}
	} else {
		es = append(es, []ASTTerm{e.Normalise()})
	}

	return es
}

func (e *AndExpression) Normalise() []ASTAnd {
	// We have an AND node and all its terms are ASTs with potential nesting.
	// We can eliminate nesting by normalising the parens (which will recurse into
	// the sub-ASTs and flatten them into DNF), and then construct an array
	// with for every term a nested array representing the OR and AND nodes.
	terms := [][][]ASTTerm{e.Left.convertToTerms()}
	for _, rhs := range e.Right {
		terms = append(terms, rhs.Expr.convertToTerms())
	}

	// Calculate the cross product, this distributes the outer AND into the nested ORs
	// and gives us a new AST that has no nested disjunctions
	ast := []ASTAnd{{
		Terms: []ASTTerm{},
	}}

	for _, disjunctions := range terms {
		// The part of the AST that we construct in this step
		// This starts off empty at every step, and we fill it up based on the
		// AST that we build in the previous step, until we're done.
		tmpAst := []ASTAnd{}
		for _, conjunction := range ast {
			for _, terms := range disjunctions {
				// Copy the conjunction with the terms collected so far
				combined := slices.Clone(conjunction.Terms)
				// Add the additional terms found in this step
				combined = append(combined, terms...)
				// Add the new AND node to the AST that we're building
				tmpAst = append(tmpAst, ASTAnd{Terms: combined})
			}
		}
		ast = tmpAst
	}

	return ast
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

func (e *AndRHS) Normalise() ASTTerm {
	return e.Expr.Normalise()
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
func (e *EqualExpr) Normalise() ASTTerm {

	if e.Paren != nil {
		panic("Called EqualExpr::Normalise on a paren, this is a bug!")
	}

	if e.LessThan != nil {
		return ASTTerm{LessThan: e.LessThan.Normalise()}
	}

	if e.LessOrEqualThan != nil {
		return ASTTerm{LessOrEqualThan: e.LessOrEqualThan.Normalise()}
	}

	if e.GreaterThan != nil {
		return ASTTerm{GreaterThan: e.GreaterThan.Normalise()}
	}

	if e.GreaterOrEqualThan != nil {
		return ASTTerm{GreaterOrEqualThan: e.GreaterOrEqualThan.Normalise()}
	}

	if e.Glob != nil {
		return ASTTerm{Glob: e.Glob.Normalise()}
	}

	if e.Assign != nil {
		return ASTTerm{Assign: e.Assign.Normalise()}
	}

	if e.Inclusion != nil {
		return ASTTerm{Inclusion: e.Inclusion.Normalise()}
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

func (e *Paren) Normalise() *ASTExpr {
	nested := e.Nested

	if e.IsNot {
		nested = *nested.invert()
	}

	return nested.Normalise()
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

func Parse(s string, log *slog.Logger) (*AST, error) {
	log.Info("parsing query", "query", s)

	v, err := Parser.ParseString("", s)
	if err != nil {
		return nil, err
	}
	return v.Normalise(), err
}
