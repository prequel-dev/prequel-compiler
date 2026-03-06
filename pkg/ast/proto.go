package ast

import (
	"fmt"
	"time"

	"github.com/prequel-dev/prequel-logmatch/pkg/match"
)

type protoNode struct {
	ty           AstNodeType
	window       time.Duration
	correlations []string
	event        *AstEventT
	terms        []*protoTerm
	negate       []*protoTerm
}

// A protoTerm represents either a field term or a child node term in the proto representation of the rule.
type protoTerm struct {
	field      *protoField
	child      AstNode
	negateOpts *AstNegateOptsT
}

func (t protoTerm) count() uint64 {
	if t.field != nil {
		return t.field.Count
	}
	return 1
}

type protoField struct {
	Field      string
	StrValue   string
	JqValue    string
	RegexValue string
	Count      uint64
	Extract    []AstExtractT
}

func protoTermsToAstFields(terms []*protoTerm) []AstFieldT {
	var fields []AstFieldT
	for _, term := range terms {
		if term.field != nil {
			// Convert protoField to AstField, applying negate options if present.
			// Must is ok as the fields have already been validated during parsing.
			fields = append(fields, term.field.MustField(term.negateOpts))
		}
	}
	return fields
}

func protoTermsToAstTerms(terms []*protoTerm) []AstTermT {
	var termsList []AstTermT
	for _, term := range terms {
		if term.child != nil {
			termsList = append(termsList, AstTermT{
				Term:       term.child,
				NegateOpts: term.negateOpts,
			})
		}
	}
	return termsList
}

// Validate the protoField and convert it to an AstField.
// This includes ensuring that at least one of StrValue, JqValue, RegexValue is set,
// and Count is non zero.
// The validation that only one of StrValue, JqValue, RegexValue is set is done elsewhere.
func (f *protoField) ToField(nOpts *AstNegateOptsT) (AstFieldT, error) {

	t := AstFieldT{
		Count:      f.Count,
		Field:      f.Field,
		Extracts:   f.Extract,
		NegateOpts: nOpts,
	}

	if t.Count == 0 {
		t.Count = 1
	}

	switch {
	case f.StrValue != "":
		t.TermValue = match.TermT{
			Type:  match.TermRaw,
			Value: f.StrValue,
		}

	case f.JqValue != "":
		t.TermValue = match.TermT{
			Type:  match.TermJqJson,
			Value: f.JqValue,
		}

	case f.RegexValue != "":
		t.TermValue = match.TermT{
			Type:  match.TermRegex,
			Value: f.RegexValue,
		}

	default:
		return AstFieldT{}, ErrBadField
	}

	return t, nil
}

func (f *protoField) MustField(nOpts *AstNegateOptsT) AstFieldT {
	field, err := f.ToField(nOpts)
	if err != nil {
		panic(fmt.Sprintf("invalid protoField: %v", err))
	}
	return field
}

// Validate the protoField to ensure it has a valid configuration.
// One of StrValue, JqValue, RegexValue must be set.
func (f *protoField) validate() error {

	var cnt int
	if f.StrValue != "" {
		cnt++
	}
	if f.JqValue != "" {
		cnt++
	}
	if f.RegexValue != "" {
		cnt++
	}

	switch cnt {
	case 1:
		return nil
	case 0:
		return fmt.Errorf("%w: one of ['%s','%s','%s'] must be set", ErrBadField, kwValue, kwJq, kwRegex)
	default:
		return fmt.Errorf("%w: only one of ['%s','%s','%s'] can be set", ErrBadField, kwValue, kwJq, kwRegex)
	}
}
