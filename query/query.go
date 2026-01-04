package query

import (
	"fmt"
	"strings"
)

type SelectQuery struct {
	Query string
	Args  []any
}

// Builder type to allow defining generic functions that can be re-used in other
// packages (like query-api)
type Builder interface {
	PushArgument(any) string
	WriteWhereClause(string)
	GetOptions() *QueryOptions
}

var _ Builder = &QueryBuilder{}

type QueryBuilder struct {
	queryBuilder *strings.Builder
	args         []any
	argsCount    uint32
	tableCounter uint32
	needsComma   bool
	needsWhere   bool
	options      QueryOptions
	sqlDialect   string
}

func (b *QueryBuilder) nextTableName() string {
	b.tableCounter = b.tableCounter + 1
	return fmt.Sprintf("table_%d", b.tableCounter)
}

func (b *QueryBuilder) PushArgument(arg any) string {
	b.args = append(b.args, arg)
	b.argsCount += 1
	return fmt.Sprintf("?%d", b.argsCount)
}

func (b *QueryBuilder) writeComma() {
	if b.needsComma {
		b.queryBuilder.WriteString(", ")
	} else {
		b.needsComma = true
	}
}

func (b *QueryBuilder) WriteWhereClause(s string) {
	if b.needsWhere {
		b.queryBuilder.WriteString(" WHERE ")
		b.needsWhere = false
	} else {
		b.queryBuilder.WriteString(" AND ")
	}

	b.queryBuilder.WriteString("(")
	b.queryBuilder.WriteString(s)
	b.queryBuilder.WriteString(")")
}

func (b *QueryBuilder) GetOptions() *QueryOptions {
	return &b.options
}

func AddPaginationArguments(b Builder) error {
	paginationConditions := []string{}

	if len(b.GetOptions().Cursor) > 0 {
		// Pre-allocate argument counters so that we don't need to duplicate them below
		args := make([]string, 0, len(b.GetOptions().Cursor))
		for _, val := range b.GetOptions().Cursor {
			args = append(args, b.PushArgument(val.Value))
		}

		for i := range b.GetOptions().Cursor {
			subcondition := []string{}
			for j, from := range b.GetOptions().Cursor {
				if j > i {
					break
				}
				var operator string
				if j < i {
					operator = "="
				} else if from.Descending {
					operator = "<"
				} else {
					operator = ">"
				}

				arg := args[j]

				columnIx, err := b.GetOptions().GetColumnIndex(from.ColumnName)
				if err != nil {
					return fmt.Errorf("error getting column index: %w", err)
				}
				column := b.GetOptions().Columns[columnIx]

				subcondition = append(
					subcondition,
					fmt.Sprintf("%s %s %s", column.QualifiedName, operator, arg),
				)
			}

			paginationConditions = append(
				paginationConditions,
				fmt.Sprintf("(%s)", strings.Join(subcondition, " AND ")),
			)
		}

		paginationCondition := strings.Join(paginationConditions, " OR ")

		b.WriteWhereClause(paginationCondition)
	}

	return nil
}
