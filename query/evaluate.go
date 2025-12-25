package query

type QueryEvaluator interface {
	EvaluateAST(ast *AST, options *QueryOptions) (*SelectQuery, error)
}

func (t *AST) Evaluate(options *QueryOptions, evaluator QueryEvaluator) (*SelectQuery, error) {
	return evaluator.EvaluateAST(t, options)
}
