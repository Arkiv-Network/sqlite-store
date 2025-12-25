package query

import (
	"fmt"
	"strings"
)

type TablesEvaluator struct{}

var _ QueryEvaluator = TablesEvaluator{}

func (b *QueryBuilder) createLeafQuery(query string) string {
	tableName := b.nextTableName()

	b.writeComma()
	b.queryBuilder.WriteString(tableName)
	b.queryBuilder.WriteString(" AS (")
	b.queryBuilder.WriteString(query)
	b.queryBuilder.WriteString(")")

	return tableName
}

func (e TablesEvaluator) EvaluateAST(ast *AST, options *QueryOptions) (*SelectQuery, error) {
	builder := QueryBuilder{
		options:      *options,
		queryBuilder: &strings.Builder{},
		args:         []any{},
		needsComma:   false,
		needsWhere:   true,
	}

	if ast.Expr != nil {
		builder.queryBuilder.WriteString(strings.Join(
			[]string{
				" SELECT",
				builder.options.columnString(),
				"FROM",
				e.EvaluateExpr(ast.Expr, &builder),
				"AS keys INNER JOIN payloads AS e INDEXED BY payloads_entity_key_index ON keys.entity_key = e.entity_key AND keys.from_block = e.from_block",
			},
			" ",
		))
	} else {
		builder.queryBuilder.WriteString(strings.Join(
			[]string{
				"SELECT",
				builder.options.columnString(),
				"FROM payloads AS e",
			},
			" ",
		))
	}

	for i, orderBy := range builder.options.OrderByAnnotations {
		tableName := ""
		switch orderBy.Type {
		case "string":
			tableName = "string_attributes"
		case "numeric":
			tableName = "numeric_attributes"
		default:
			return nil, fmt.Errorf("a type of either 'string' or 'numeric' needs to be provided for the annotation '%s'", orderBy.Name)
		}

		sortingTable := fmt.Sprintf("arkiv_annotation_sorting%d", i)

		keyPlaceholder := builder.pushArgument(orderBy.Name)

		fmt.Fprintf(builder.queryBuilder,
			" LEFT JOIN %[1]s AS %s"+
				" ON %[2]s.entity_key = e.entity_key"+
				" AND %[2]s.from_block = e.from_block"+
				" AND %[2]s.key = %[3]s",

			tableName,
			sortingTable,
			keyPlaceholder,
		)
	}

	err := builder.addPaginationArguments()
	if err != nil {
		return nil, fmt.Errorf("error adding the pagination condition: %w", err)
	}

	if builder.needsWhere {
		builder.queryBuilder.WriteString(" WHERE ")
		builder.needsWhere = false
	} else {
		builder.queryBuilder.WriteString(" AND ")
	}

	blockArg := builder.pushArgument(builder.options.AtBlock)
	fmt.Fprintf(builder.queryBuilder, "%s BETWEEN e.from_block AND e.to_block - 1", blockArg)

	builder.queryBuilder.WriteString(" ORDER BY ")

	orderColumns := make([]string, 0, len(builder.options.OrderBy))
	for _, o := range builder.options.OrderBy {
		suffix := ""
		if o.Descending {
			suffix = " DESC"
		}
		orderColumns = append(orderColumns, o.Column.Name+suffix)
	}
	builder.queryBuilder.WriteString(strings.Join(orderColumns, ", "))

	fmt.Fprintf(builder.queryBuilder, " LIMIT %d", QueryResultCountLimit)

	return &SelectQuery{
		Query: builder.queryBuilder.String(),
		Args:  builder.args,
	}, nil
}

func (e TablesEvaluator) EvaluateExpr(expr *ASTExpr, builder *QueryBuilder) string {
	builder.queryBuilder.WriteString("WITH ")
	return e.EvaluateOr(&expr.Or, builder)
}

func (e TablesEvaluator) EvaluateOr(expr *ASTOr, b *QueryBuilder) string {
	leftTable := e.EvaluateAnd(&expr.Terms[0], b)
	tableName := leftTable

	for _, rhs := range expr.Terms[1:] {
		rightTable := e.EvaluateAnd(&rhs, b)
		tableName = b.nextTableName()

		b.writeComma()

		b.queryBuilder.WriteString(tableName)
		b.queryBuilder.WriteString(" AS (")
		b.queryBuilder.WriteString("SELECT * FROM ")
		b.queryBuilder.WriteString(leftTable)
		b.queryBuilder.WriteString(" UNION ")
		b.queryBuilder.WriteString("SELECT * FROM ")
		b.queryBuilder.WriteString(rightTable)
		b.queryBuilder.WriteString(")")

		// Carry forward the cumulative result of the UNION
		leftTable = tableName
	}

	return tableName
}

func (e TablesEvaluator) EvaluateAnd(expr *ASTAnd, b *QueryBuilder) string {
	leftTable := e.EvaluateTerm(&expr.Terms[0], b)
	tableName := leftTable

	for _, rhs := range expr.Terms[1:] {
		rightTable := e.EvaluateTerm(&rhs, b)
		tableName = b.nextTableName()

		b.writeComma()

		b.queryBuilder.WriteString(tableName)
		b.queryBuilder.WriteString(" AS (")
		b.queryBuilder.WriteString("SELECT * FROM ")
		b.queryBuilder.WriteString(leftTable)
		b.queryBuilder.WriteString(" INTERSECT ")
		b.queryBuilder.WriteString("SELECT * FROM ")
		b.queryBuilder.WriteString(rightTable)
		b.queryBuilder.WriteString(")")

		// Carry forward the cumulative result of the INTERSECT
		leftTable = tableName
	}

	return tableName
}

func (TablesEvaluator) EvaluateTerm(expr *ASTTerm, b *QueryBuilder) string {
	if expr.LessThan != nil {
		return expr.LessThan.Evaluate(b)
	}

	if expr.LessOrEqualThan != nil {
		return expr.LessOrEqualThan.Evaluate(b)
	}

	if expr.GreaterThan != nil {
		return expr.GreaterThan.Evaluate(b)
	}

	if expr.GreaterOrEqualThan != nil {
		return expr.GreaterOrEqualThan.Evaluate(b)
	}

	if expr.Glob != nil {
		return expr.Glob.Evaluate(b)
	}

	if expr.Assign != nil {
		return expr.Assign.Evaluate(b)
	}

	if expr.Inclusion != nil {
		return expr.Inclusion.Evaluate(b)
	}

	panic("This should not happen!")
}

func (b *QueryBuilder) createAnnotationQuery(
	attributeType string,
	whereClause string,
) string {

	tableName := "string_attributes"
	if attributeType == "numeric" {
		tableName = "numeric_attributes"
	}

	blockArg := b.pushArgument(b.options.AtBlock)

	return b.createLeafQuery(
		strings.Join(
			[]string{
				"SELECT a.entity_key, a.from_block FROM",
				tableName,
				"AS a",
				"WHERE",
				whereClause,
				fmt.Sprintf("AND %s BETWEEN a.from_block AND a.to_block - 1", blockArg),
			},
			" ",
		),
	)
}

func (e *Glob) Evaluate(b *QueryBuilder) string {
	varArg := b.pushArgument(e.Var)
	valArg := b.pushArgument(e.Value)

	op := "GLOB"
	if e.IsNot {
		op = "NOT GLOB"
	}

	return b.createAnnotationQuery(
		"string",
		fmt.Sprintf("a.key = %s AND a.value %s %s", varArg, op, valArg),
	)
}

func (e *LessThan) Evaluate(b *QueryBuilder) string {
	attrType := "string"
	varArg := b.pushArgument(e.Var)
	valArg := ""

	if e.Value.String != nil {
		valArg = b.pushArgument(*e.Value.String)
	} else {
		attrType = "numeric"
		valArg = b.pushArgument(*e.Value.Number)
	}

	return b.createAnnotationQuery(
		attrType,
		fmt.Sprintf("a.key = %s AND a.value < %s", varArg, valArg),
	)
}

func (e *LessOrEqualThan) Evaluate(b *QueryBuilder) string {
	attrType := "string"
	varArg := b.pushArgument(e.Var)
	valArg := ""

	if e.Value.String != nil {
		valArg = b.pushArgument(*e.Value.String)
	} else {
		attrType = "numeric"
		valArg = b.pushArgument(*e.Value.Number)
	}

	return b.createAnnotationQuery(
		attrType,
		fmt.Sprintf("a.key = %s AND a.value <= %s", varArg, valArg),
	)
}

func (e *GreaterThan) Evaluate(b *QueryBuilder) string {
	attrType := "string"
	varArg := b.pushArgument(e.Var)
	valArg := ""

	if e.Value.String != nil {
		valArg = b.pushArgument(*e.Value.String)
	} else {
		attrType = "numeric"
		valArg = b.pushArgument(*e.Value.Number)
	}

	return b.createAnnotationQuery(
		attrType,
		fmt.Sprintf("a.key = %s AND a.value > %s", varArg, valArg),
	)
}

func (e *GreaterOrEqualThan) Evaluate(b *QueryBuilder) string {
	attrType := "string"
	varArg := b.pushArgument(e.Var)
	valArg := ""

	if e.Value.String != nil {
		valArg = b.pushArgument(*e.Value.String)
	} else {
		attrType = "numeric"
		valArg = b.pushArgument(*e.Value.Number)
	}

	return b.createAnnotationQuery(
		attrType,
		fmt.Sprintf("a.key = %s AND a.value >= %s", varArg, valArg),
	)
}

func (e *Equality) Evaluate(b *QueryBuilder) string {
	attrType := "string"
	varArg := b.pushArgument(e.Var)
	valArg := ""

	op := "="
	if e.IsNot {
		op = "!="
	}

	if e.Value.String != nil {
		valArg = b.pushArgument(*e.Value.String)
	} else {
		attrType = "numeric"
		valArg = b.pushArgument(*e.Value.Number)
	}

	return b.createAnnotationQuery(
		attrType,
		fmt.Sprintf("a.key = %s AND a.value %s %s", varArg, op, valArg),
	)
}

func (e *Inclusion) Evaluate(b *QueryBuilder) string {
	var values []string
	attrType := "string"
	if len(e.Values.Strings) > 0 {

		values = make([]string, 0, len(e.Values.Strings))
		for _, value := range e.Values.Strings {
			if e.Var == OwnerAttributeKey ||
				e.Var == CreatorAttributeKey ||
				e.Var == KeyAttributeKey {
				values = append(values, b.pushArgument(strings.ToLower(value)))
			} else {
				values = append(values, b.pushArgument(value))
			}
		}

	} else {
		attrType = "numeric"
		values = make([]string, 0, len(e.Values.Numbers))
		for _, value := range e.Values.Numbers {
			values = append(values, b.pushArgument(value))
		}
	}

	paramStr := strings.Join(values, ", ")

	condition := fmt.Sprintf("a.value IN (%s)", paramStr)
	if e.IsNot {
		condition = fmt.Sprintf("a.value NOT IN (%s)", paramStr)
	}

	keyArg := b.pushArgument(e.Var)

	return b.createAnnotationQuery(
		attrType,
		fmt.Sprintf("a.key = %s AND %s", keyArg,
			condition,
		),
	)
}
