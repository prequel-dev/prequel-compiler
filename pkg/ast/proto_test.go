package ast

import (
	"reflect"
	"testing"

	"github.com/prequel-dev/prequel-logmatch/pkg/match"
)

func TestProtoField_ToField(t *testing.T) {
	tests := []struct {
		name    string
		field   protoField
		nOpts   *AstNegateOptsT
		want    AstFieldT
		wantErr bool
	}{
		{
			name: "StrValue set",
			field: protoField{
				Field:    "foo",
				StrValue: "bar",
				Count:    2,
			},
			nOpts: &AstNegateOptsT{Anchor: 1},
			want: AstFieldT{
				Field:      "foo",
				Count:      2,
				TermValue:  match.TermT{Type: match.TermRaw, Value: "bar"},
				NegateOpts: &AstNegateOptsT{Anchor: 1},
			},
			wantErr: false,
		},
		{
			name: "JqValue set",
			field: protoField{
				Field:   "foo",
				JqValue: ".baz",
			},
			nOpts: nil,
			want: AstFieldT{
				Field:      "foo",
				Count:      1,
				TermValue:  match.TermT{Type: match.TermJqJson, Value: ".baz"},
				NegateOpts: nil,
			},
			wantErr: false,
		},
		{
			name: "RegexValue set",
			field: protoField{
				Field:      "foo",
				RegexValue: "re.*",
			},
			nOpts: nil,
			want: AstFieldT{
				Field:      "foo",
				Count:      1,
				TermValue:  match.TermT{Type: match.TermRegex, Value: "re.*"},
				NegateOpts: nil,
			},
			wantErr: false,
		},
		{
			name:    "none set",
			field:   protoField{Field: "foo"},
			nOpts:   nil,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.field.ToField(tt.nOpts)
			if (err != nil) != tt.wantErr {
				t.Errorf("ToField() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ToField() = %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestProtoField_MustField(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("MustField() did not panic on error")
		}
	}()
	// This should panic because no value is set
	f := protoField{Field: "foo"}
	_ = f.MustField(nil)
}

func TestProtoField_validate(t *testing.T) {
	tests := []struct {
		name    string
		field   protoField
		wantErr bool
	}{
		{
			name:    "StrValue only",
			field:   protoField{StrValue: "a"},
			wantErr: false,
		},
		{
			name:    "JqValue only",
			field:   protoField{JqValue: ".jq"},
			wantErr: false,
		},
		{
			name:    "RegexValue only",
			field:   protoField{RegexValue: "re"},
			wantErr: false,
		},
		{
			name:    "none set",
			field:   protoField{},
			wantErr: true,
		},
		{
			name:    "multiple set",
			field:   protoField{StrValue: "a", JqValue: ".jq"},
			wantErr: true,
		},
		{
			name:    "all set",
			field:   protoField{StrValue: "a", JqValue: ".jq", RegexValue: "re"},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.field.validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestProtoTerm_count(t *testing.T) {
	tests := []struct {
		name string
		term protoTerm
		want uint64
	}{
		{
			name: "field with count",
			term: protoTerm{field: &protoField{Count: 5}},
			want: 5,
		},
		{
			name: "no field",
			term: protoTerm{field: nil},
			want: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.term.count()
			if got != tt.want {
				t.Errorf("count() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestProtoTermsToAstFields(t *testing.T) {
	terms := []*protoTerm{
		{field: &protoField{Field: "foo", StrValue: "bar"}},
		{field: nil},
	}
	fields := protoTermsToAstFields(terms)
	if len(fields) != 1 || fields[0].Field != "foo" {
		t.Errorf("protoTermsToAstFields() = %+v, want field 'foo'", fields)
	}
}

func TestProtoTermsToAstTerms(t *testing.T) {
	child := &AstMatchLeafT{}
	terms := []*protoTerm{
		{child: child, negateOpts: &AstNegateOptsT{Anchor: 2}},
		{child: nil},
	}
	astTerms := protoTermsToAstTerms(terms)
	if len(astTerms) != 1 || astTerms[0].Term != child || astTerms[0].NegateOpts.Anchor != 2 {
		t.Errorf("protoTermsToAstTerms() = %+v, want child and NegateOpts", astTerms)
	}
}
