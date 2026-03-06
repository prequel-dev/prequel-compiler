package ast

import (
	"reflect"
	"testing"

	"github.com/goccy/go-yaml/ast"
	"github.com/goccy/go-yaml/parser"
)

func mustParseYAMLNode(t *testing.T, src string) ast.Node {
	t.Helper()

	doc, err := parser.ParseBytes([]byte(src), 0)
	if err != nil {
		t.Fatalf("failed to parse yaml: %v", err)
	}

	if len(doc.Docs) != 1 {
		t.Fatalf("expected exactly one document, got %d", len(doc.Docs))
	}

	return doc.Docs[0].Body
}

func TestParseCreNode_TableDriven(t *testing.T) {
	tests := []struct {
		name    string
		yaml    string
		strict  bool
		wantErr error
		wantPos int
		wants   AstCreT
	}{
		{
			name: "valid CRE minimal",
			yaml: `
id: CRE-1234
title: Example CRE
`,
			strict: true,
			wants: AstCreT{
				Id:    "CRE-1234",
				Title: "Example CRE",
			},
		},
		{
			name: "valid CRE with everything",
			yaml: `
id: PREQUEL-2024-0006
severity: 2
title: Kafka Topic Operator Thread Blocked
category: message-queue-problem
author: Prequel
description: |
  There is a known issue in the Strimzi Kafka.
cause: |
  The Kafka Topic Operator is using a single thread to process events.
impact: |
  No Kafka topics will be created or updated.
tags:
- known-problem
- kafka
- strimzi
mitigation: |
  - Add additional CPU resources and restart the Kafka Topic Operator
  - Use the Zookeeper store instead of the Kafka Streams store for the Strimzi Kafka Topic Operator
mitigationScore: 2
impactScore: 8
reports: 11
references:
- https://github.com/strimzi/strimzi-kafka-operator/issues/6046
applications:
- name: "kafka"
`,
			strict: true,
			wants: AstCreT{
				Id:              "PREQUEL-2024-0006",
				Title:           "Kafka Topic Operator Thread Blocked",
				Severity:        2,
				Category:        "message-queue-problem",
				Author:          "Prequel",
				Description:     "There is a known issue in the Strimzi Kafka.\n",
				Cause:           "The Kafka Topic Operator is using a single thread to process events.\n",
				Impact:          "No Kafka topics will be created or updated.\n",
				Tags:            []string{"known-problem", "kafka", "strimzi"},
				Mitigation:      "- Add additional CPU resources and restart the Kafka Topic Operator\n- Use the Zookeeper store instead of the Kafka Streams store for the Strimzi Kafka Topic Operator\n",
				MitigationScore: 2,
				ImpactScore:     8,
				Reports:         11,
				References:      []string{"https://github.com/strimzi/strimzi-kafka-operator/issues/6046"},
				Applications:    []AstAppT{{Name: "kafka"}},
			},
		},
		{
			name:    "cre id is wrong type",
			yaml:    `id: 112333`,
			strict:  true,
			wantErr: ErrUnexpectedType,
			wantPos: 5, // Seems to be one based.
		},
		{
			name: "invalid id (too short)",
			yaml: `
id: ab
title: Bad CRE
`,
			strict:  true,
			wantErr: ErrBadIdentifier,
			wantPos: 6, // Pos of 'ab', one based.
		},
		{
			name: "unexpected key in strict mode",
			yaml: `
id: CRE-9999
title: Strict CRE
unexpected: value
`,
			strict:  true,
			wantErr: ErrUnexpectedKey,
			wantPos: 33, // Pos of 'unexpected', one based.
		},
		{
			name: "unexpected key in non-strict mode",
			yaml: `
id: CRE-9999
title: NonStrict CRE
unexpected: value
`,
			strict: false,
			wants: AstCreT{
				Id:    "CRE-9999",
				Title: "NonStrict CRE",
			},
		},
		{
			name: "valid CRE with applications",
			yaml: `
id: CRE-8888
title: App CRE
applications:
  - name: app1
    version: v1
    processName: nginx
    processPath: /usr/sbin/nginx
    containerName: nginx-container
    imageUrl: nginx:latest
    repoUrl: github.com/nginx/nginx
  - name: app2
    version: v2
`,
			strict: true,
			wants: AstCreT{
				Id:    "CRE-8888",
				Title: "App CRE",
				Applications: []AstAppT{
					{
						Name:          "app1",
						Version:       "v1",
						ProcessName:   "nginx",
						ProcessPath:   "/usr/sbin/nginx",
						ContainerName: "nginx-container",
						ImageUrl:      "nginx:latest",
						RepoUrl:       "github.com/nginx/nginx",
					},
					{Name: "app2", Version: "v2"},
				},
			},
		},
		{
			name: "strict app with extra key",
			yaml: `
applications:
  - name: app1
    unexpected: value
`,
			strict:  true,
			wantErr: ErrUnexpectedKey,
			wantPos: 45, // Pos of ':' in 'unexpected:'
		},
		{
			name: "strict app with extra key non-strict",
			yaml: `
id: CRE-7777
applications:
  - name: app1
    unexpected: value
`,
			strict: false,
			wants: AstCreT{
				Id: "CRE-7777",
				Applications: []AstAppT{
					{Name: "app1"},
				},
			},
		},
		{
			name: "bad app type",
			yaml: `
applications: badtype
`,
			wantErr: ErrUnexpectedType,
			wantPos: 16, // Pos of 'b' in 'badtype'
		},
		{
			name: "bad app value type",
			yaml: `
applications:
  - notamapping
`,
			wantErr: ErrUnexpectedType,
			wantPos: 20, // Pos of 'n' in 'notamapping'
		},
		{
			name: "bad app key",
			yaml: `
applications:
  - 11: badkey
`,
			strict:  true,
			wantErr: ErrUnexpectedType,
			wantPos: 20, // Pos of '11' in '11: badkey'
		},
		{
			name: "valid severity value",
			yaml: `
id: CRE-5555
severity: 3
`,
			strict: true,
			wants: AstCreT{
				Id:       "CRE-5555",
				Severity: SeverityLow,
			},
		},
		{
			name:    "negative severity value",
			yaml:    `severity: -1`,
			strict:  true,
			wantErr: ErrUnexpectedType,
			wantPos: 11, // Pos of '-' in '-1'
		},
		{
			name:    "invalid severity value",
			yaml:    `severity: 11`,
			strict:  true,
			wantErr: ErrBadSeverity,
		},
		{
			name:    "invalid node type",
			yaml:    `shrubbery`,
			wantErr: ErrUnexpectedType,
			wantPos: 1, // Pos of 's' in 'shrubbery'
		},
		{
			name:    "invalid mapping key type",
			yaml:    `11: invalid key type`,
			wantErr: ErrUnexpectedType,
			wantPos: 1, // Pos of '11' in '11: invalid key type'
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			node := mustParseYAMLNode(t, tt.yaml)
			p := &parserT{strict: tt.strict}
			cre, err := p.parseCreNode(node)

			if !checkParserError(t, err, tt.wantErr, tt.wantPos) {
				return
			}

			if !reflect.DeepEqual(*cre, tt.wants) {
				t.Errorf("expected CRE %+v, got %+v", tt.wants, *cre)
			}
		})
	}
}
