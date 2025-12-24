package query

import (
	"fmt"
	"strings"
)

func (t *TopLevel) EvaluateExists(options *QueryOptions) (*SelectQuery, error) {
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

	if builder.options.IncludeData != nil {
		if builder.options.IncludeData.Owner {
			fmt.Fprintf(builder.queryBuilder,
				" INNER JOIN string_attributes AS ownerAttrs INDEXED BY string_attributes_entity_kv_idx"+
					" ON e.entity_key = ownerAttrs.entity_key"+
					" AND e.from_block = ownerAttrs.from_block"+
					" AND ownerAttrs.key = '%s'",
				OwnerAttributeKey,
			)
		}
		if builder.options.IncludeData.Expiration {
			fmt.Fprintf(builder.queryBuilder,
				" INNER JOIN numeric_attributes AS expirationAttrs INDEXED BY numeric_attributes_entity_kv_idx"+
					" ON e.entity_key = expirationAttrs.entity_key"+
					" AND e.from_block = expirationAttrs.from_block"+
					" AND expirationAttrs.key = '%s'",
				ExpirationAttributeKey,
			)
		}
		if builder.options.IncludeData.CreatedAtBlock {
			fmt.Fprintf(builder.queryBuilder,
				" INNER JOIN numeric_attributes AS createdAtBlockAttrs INDEXED BY numeric_attributes_entity_kv_idx"+
					" ON e.entity_key = createdAtBlockAttrs.entity_key"+
					" AND e.from_block = createdAtBlockAttrs.from_block"+
					" AND createdAtBlockAttrs.key = '%s'",
				CreatedAtBlockKey,
			)
		}
		if builder.options.IncludeData.LastModifiedAtBlock ||
			options.IncludeData.TransactionIndexInBlock ||
			options.IncludeData.OperationIndexInTransaction {
			fmt.Fprintf(builder.queryBuilder,
				" INNER JOIN numeric_attributes AS sequenceAttrs INDEXED BY numeric_attributes_entity_kv_idx"+
					" ON e.entity_key = sequenceAttrs.entity_key"+
					" AND e.from_block = sequenceAttrs.from_block"+
					" AND sequenceAttrs.key = '%s'",
				SequenceAttributeKey,
			)
		}
	}

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

	if t.Expression != nil {
		t.Expression.addConditions(&builder)
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

func (e *Expression) addConditions(b *QueryBuilder) {
	e.Or.addConditions(b)
}

func (e *OrExpression) addConditions(b *QueryBuilder) {
	b.queryBuilder.WriteString(" AND (")
	e.Left.addConditions(b)

	for _, r := range e.Right {
		b.queryBuilder.WriteString(") OR (")
		r.Expr.addConditions(b)
	}

	b.queryBuilder.WriteString(")")
}

func (e *AndExpression) addConditions(b *QueryBuilder) {
	e.Left.addConditions(b)

	for _, r := range e.Right {
		b.queryBuilder.WriteString(" AND ")
		r.Expr.addConditions(b)
	}

}

func (e *EqualExpr) addConditions(b *QueryBuilder) {
	var (
		attrType  string
		key       string
		operation string
		value     string
	)

	if e.Assign != nil {
		key = b.pushArgument(e.Assign.Var)
		val := e.Assign.Value
		if val.String != nil {
			attrType = "string"
			value = b.pushArgument(val.String)
		} else {
			attrType = "numeric"
			value = b.pushArgument(val.Number)
		}

		operation = "="
		if e.Assign.IsNot {
			operation = "!="
		}
	} else if e.Inclusion != nil {
		key = b.pushArgument(e.Inclusion.Var)
		var values []string
		attrType = "string"
		if len(e.Inclusion.Values.Strings) > 0 {
			values = make([]string, 0, len(e.Inclusion.Values.Strings))
			for _, value := range e.Inclusion.Values.Strings {
				if e.Inclusion.Var == OwnerAttributeKey ||
					e.Inclusion.Var == CreatorAttributeKey ||
					e.Inclusion.Var == KeyAttributeKey {
					values = append(values, b.pushArgument(strings.ToLower(value)))
				} else {
					values = append(values, b.pushArgument(value))
				}
			}
		} else {
			attrType = "numeric"
			values = make([]string, 0, len(e.Inclusion.Values.Numbers))
			for _, value := range e.Inclusion.Values.Numbers {
				values = append(values, b.pushArgument(value))
			}
		}

		paramStr := strings.Join(values, ", ")
		value = fmt.Sprintf("(%s)", paramStr)

		operation = "IN"
		if e.Inclusion.IsNot {
			operation = "NOT IN"
		}
	} else if e.LessThan != nil {
		key = b.pushArgument(e.LessThan.Var)
		val := e.LessThan.Value
		if val.String != nil {
			attrType = "string"
			value = b.pushArgument(val.String)
		} else {
			attrType = "numeric"
			value = b.pushArgument(val.Number)
		}
		operation = "<"
	} else if e.LessOrEqualThan != nil {
		key = b.pushArgument(e.LessOrEqualThan.Var)
		val := e.LessOrEqualThan.Value
		if val.String != nil {
			attrType = "string"
			value = b.pushArgument(val.String)
		} else {
			attrType = "numeric"
			value = b.pushArgument(val.Number)
		}
		operation = "<="
	} else if e.GreaterThan != nil {
		key = b.pushArgument(e.GreaterThan.Var)
		val := e.GreaterThan.Value
		if val.String != nil {
			attrType = "string"
			value = b.pushArgument(val.String)
		} else {
			attrType = "numeric"
			value = b.pushArgument(val.Number)
		}
		operation = ">"
	} else if e.GreaterOrEqualThan != nil {
		key = b.pushArgument(e.GreaterOrEqualThan.Var)
		val := e.GreaterOrEqualThan.Value
		if val.String != nil {
			attrType = "string"
			value = b.pushArgument(val.String)
		} else {
			attrType = "numeric"
			value = b.pushArgument(val.Number)
		}
		operation = ">="
	} else if e.Glob != nil {
		key = b.pushArgument(e.Glob.Var)
		val := e.Glob.Value
		attrType = "string"
		value = b.pushArgument(val)

		operation = "GLOB"
		if e.Glob.IsNot {
			operation = "NOT GLOB"
		}
	} else {
		panic("EqualExpr::addConditions: unnormalised expression, paren is non-nil")
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
}
