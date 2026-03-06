package ast

type (
	ParseOpt      func(*optT)
	ValidatorFunc func(string) error
)

type optT struct {
	maxGen          uint32
	maxRank         uint32
	maxDepth        uint32
	strict          bool
	jqValidator     ValidatorFunc
	luaValidator    ValidatorFunc
	promQLValidator ValidatorFunc
}

// WithStrict sets the strict mode for parsing.
//   In strict mode, the parser will return an error if it encounters any unexpected keys in the YAML input. In non-strict mode, the parser will ignore unexpected keys and continue parsing.
//   When disabled, the parser will ignore any non operational keys in the YAML input
//   This is particularly true in the metadata sections where additional keys do
//   not have operational impact on the rules engine.

func WithStrict(strict bool) ParseOpt {
	return func(opts *optT) {
		opts.strict = strict
	}
}

func WithJQValidator(validator ValidatorFunc) ParseOpt {
	return func(opts *optT) {
		opts.jqValidator = selectValidator(validator)
	}
}

func WithLuaValidator(validator ValidatorFunc) ParseOpt {
	return func(opts *optT) {
		opts.luaValidator = selectValidator(validator)
	}
}

func WithPromQLValidator(validator ValidatorFunc) ParseOpt {
	return func(opts *optT) {
		opts.promQLValidator = selectValidator(validator)
	}
}

func WithMaxGen(maxGen uint32) ParseOpt {
	return func(opts *optT) {
		opts.maxGen = maxGen
	}
}

// WithMaxRank sets the maximum allowed rank for terms in the YAML input.
// This is a safeguard against excessively large numbers of terms that could lead to performance issues during parsing.
func WithMaxRank(maxRank uint32) ParseOpt {
	return func(opts *optT) {
		opts.maxRank = maxRank
	}
}

// WithMaxDepth sets the maximum allowed depth for rule definitions in the YAML input.
// This is a safeguard against excessively nested structures that could lead to stack overflows or performance issues during parsing.
func WithMaxDepth(maxDepth uint32) ParseOpt {
	return func(opts *optT) {
		opts.maxDepth = maxDepth
	}
}

func selectValidator(validator ValidatorFunc) ValidatorFunc {
	if validator == nil {
		return stubValidator
	}
	return validator
}

var stubValidator = func(string) error { return nil }

func parseOpts(opts ...ParseOpt) optT {
	opt := optT{
		maxGen:          defaultMaxGen,
		maxRank:         defaultMaxRank,
		maxDepth:        defaultMaxDepth,
		strict:          false,
		luaValidator:    stubValidator,
		promQLValidator: stubValidator,
		jqValidator:     stubValidator,
	}
	for _, f := range opts {
		f(&opt)
	}
	return opt
}
