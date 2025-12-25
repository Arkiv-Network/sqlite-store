package query

import (
	"fmt"
	"strings"
)

type ExistsEvaluator struct{}

var _ QueryEvaluator = ExistsEvaluator{}

func (e ExistsEvaluator) EvaluateAST(ast *AST, options *QueryOptions) (*SelectQuery, error) {
	builder := QueryBuilder{
		options:      *options,
		queryBuilder: &strings.Builder{},
		args:         []any{},
		needsComma:   false,
		needsWhere:   true,
	}

	builder.queryBuilder.WriteString(strings.Join(
		[]string{
			"SELECT",
			builder.options.columnString(),
			"FROM payloads AS e",
		},
		" ",
	))

	for i, orderBy := range builder.options.OrderByAnnotations {
		tableName := ""
		indexName := ""
		switch orderBy.Type {
		case "string":
			tableName = "string_attributes"
			indexName = "string_attributes_entity_kv_idx"
		case "numeric":
			tableName = "numeric_attributes"
			indexName = "numeric_attributes_entity_kv_idx"
		default:
			return nil, fmt.Errorf("a type of either 'string' or 'numeric' needs to be provided for the annotation '%s'", orderBy.Name)
		}

		sortingTable := fmt.Sprintf("arkiv_annotation_sorting%d", i)

		keyPlaceholder := builder.pushArgument(orderBy.Name)

		fmt.Fprintf(builder.queryBuilder,
			" LEFT JOIN %[1]s AS %s INDEXED BY %[4]s"+
				" ON %[2]s.entity_key = e.entity_key"+
				" AND %[2]s.from_block = e.from_block"+
				" AND %[2]s.key = %[3]s",

			tableName,
			sortingTable,
			keyPlaceholder,
			indexName,
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

	if ast.Expr != nil {
		err := e.addOrConditions(&ast.Expr.Or, &builder)
		if err != nil {
			return nil, err
		}
	}

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

func (e ExistsEvaluator) addOrConditions(expr *ASTOr, b *QueryBuilder) error {
	b.queryBuilder.WriteString(" AND (")

	err := e.addAndConditions(&expr.Terms[0], b)
	if err != nil {
		return err
	}

	for _, r := range expr.Terms[1:] {
		b.queryBuilder.WriteString(") OR (")
		err = e.addAndConditions(&r, b)
		if err != nil {
			return err
		}
	}

	b.queryBuilder.WriteString(")")

	return nil
}

func (e ExistsEvaluator) addAndConditions(expr *ASTAnd, b *QueryBuilder) error {
	err := e.addTermConditions(&expr.Terms[0], b)
	if err != nil {
		return err
	}

	for _, r := range expr.Terms[1:] {
		b.queryBuilder.WriteString(" AND ")
		err = e.addTermConditions(&r, b)
		if err != nil {
			return err
		}
	}

	return nil
}

func (ExistsEvaluator) addTermConditions(term *ASTTerm, b *QueryBuilder) error {
	var (
		attrType  string
		key       string
		operation string
		value     string
	)

	if term.Assign != nil {
		key = b.pushArgument(term.Assign.Var)
		val := term.Assign.Value
		if val.String != nil {
			attrType = "string"
			value = b.pushArgument(*val.String)
		} else {
			attrType = "numeric"
			value = b.pushArgument(*val.Number)
		}

		operation = "="
		if term.Assign.IsNot {
			operation = "!="
		}
	} else if term.Inclusion != nil {
		key = b.pushArgument(term.Inclusion.Var)
		var values []string
		attrType = "string"
		if len(term.Inclusion.Values.Strings) > 0 {
			values = make([]string, 0, len(term.Inclusion.Values.Strings))
			for _, value := range term.Inclusion.Values.Strings {
				if term.Inclusion.Var == OwnerAttributeKey ||
					term.Inclusion.Var == CreatorAttributeKey ||
					term.Inclusion.Var == KeyAttributeKey {
					values = append(values, b.pushArgument(strings.ToLower(value)))
				} else {
					values = append(values, b.pushArgument(value))
				}
			}
		} else {
			attrType = "numeric"
			values = make([]string, 0, len(term.Inclusion.Values.Numbers))
			for _, value := range term.Inclusion.Values.Numbers {
				values = append(values, b.pushArgument(value))
			}
		}

		paramStr := strings.Join(values, ", ")
		value = fmt.Sprintf("(%s)", paramStr)

		operation = "IN"
		if term.Inclusion.IsNot {
			operation = "NOT IN"
		}
	} else if term.LessThan != nil {
		key = b.pushArgument(term.LessThan.Var)
		val := term.LessThan.Value
		if val.String != nil {
			attrType = "string"
			value = b.pushArgument(*val.String)
		} else {
			attrType = "numeric"
			value = b.pushArgument(*val.Number)
		}
		operation = "<"
	} else if term.LessOrEqualThan != nil {
		key = b.pushArgument(term.LessOrEqualThan.Var)
		val := term.LessOrEqualThan.Value
		if val.String != nil {
			attrType = "string"
			value = b.pushArgument(*val.String)
		} else {
			attrType = "numeric"
			value = b.pushArgument(*val.Number)
		}
		operation = "<="
	} else if term.GreaterThan != nil {
		key = b.pushArgument(term.GreaterThan.Var)
		val := term.GreaterThan.Value
		if val.String != nil {
			attrType = "string"
			value = b.pushArgument(*val.String)
		} else {
			attrType = "numeric"
			value = b.pushArgument(*val.Number)
		}
		operation = ">"
	} else if term.GreaterOrEqualThan != nil {
		key = b.pushArgument(term.GreaterOrEqualThan.Var)
		val := term.GreaterOrEqualThan.Value
		if val.String != nil {
			attrType = "string"
			value = b.pushArgument(*val.String)
		} else {
			attrType = "numeric"
			value = b.pushArgument(*val.Number)
		}
		operation = ">="
	} else if term.Glob != nil {
		key = b.pushArgument(term.Glob.Var)
		val := term.Glob.Value
		attrType = "string"
		value = b.pushArgument(val)

		operation = "GLOB"
		if term.Glob.IsNot {
			operation = "NOT GLOB"
		}
	} else {
		return fmt.Errorf("EqualExpr::addConditions: unnormalised expression, paren is non-nil")
	}

	attrTable := "string_attributes"
	attrIndex := "string_attributes_entity_kv_idx"
	if attrType == "numeric" {
		attrTable = "numeric_attributes"
		attrIndex = "numeric_attributes_entity_kv_idx"
	}

	b.queryBuilder.WriteString(strings.Join(
		[]string{
			"EXISTS (",
			"SELECT 1",
			"FROM",
			attrTable,
			"AS a",
			"INDEXED BY",
			attrIndex,
			"WHERE",
			"a.entity_key = e.entity_key",
			"AND a.from_block = e.from_block",
			"AND a.key =",
			key,
			"AND a.value",
			operation,
			value,
			")",
		},
		" ",
	))

	return nil
}
