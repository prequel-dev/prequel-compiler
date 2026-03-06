package ast

import (
	"fmt"
	"regexp"

	"github.com/goccy/go-yaml/ast"
)

var validCreIdRegex = regexp.MustCompile(`^[A-Za-z0-9-]{4,}$`)

func (p *parserT) parseCreNode(node ast.Node) (*AstCreT, error) {

	mapping, err := p.nodeToMapping(node)
	if err != nil {
		return nil, err
	}

	var (
		cre AstCreT
	)

	for _, v := range mapping.Values {

		key, err := p.nodeToString(v.Key)
		if err != nil {
			return nil, err
		}

		switch key {
		case kwCreId:
			cre.Id, err = p.parseCreId(v.Value)

		case kwSeverity:
			cre.Severity, err = p.parseSeverityNode(v.Value)

		case kwTitle:
			cre.Title, err = p.nodeToString(v.Value)

		case kwCategory:
			cre.Category, err = p.nodeToString(v.Value)

		case kwTags:
			cre.Tags, err = p.nodeToStrs(v.Value)

		case kwAuthor:
			cre.Author, err = p.nodeToString(v.Value)

		case kwDescription:
			cre.Description, err = p.nodeToString(v.Value)

		case kwImpact:
			cre.Impact, err = p.nodeToString(v.Value)

		case kwImpactScore:
			cre.ImpactScore, err = p.nodeToUint(v.Value)

		case kwCause:
			cre.Cause, err = p.nodeToString(v.Value)

		case kwMitigation:
			cre.Mitigation, err = p.nodeToString(v.Value)

		case kwMitigationScore:
			cre.MitigationScore, err = p.nodeToUint(v.Value)

		case kwReferences:
			cre.References, err = p.nodeToStrs(v.Value)

		case kwReports:
			cre.Reports, err = p.nodeToUint(v.Value)

		case kwApplications:
			cre.Applications, err = p.parseApplicationsNode(v.Value)

		default:
			if p.strict {
				err = p.wrapError(v.Key, ErrUnexpectedKey)
			}
		}

		if err != nil {
			return nil, err
		}
	}

	if cre.Id == "" {
		err := fmt.Errorf("%w: %s", ErrMissingKey, kwCreId)
		return nil, p.wrapErrorParent(mapping, err)
	}

	return &cre, nil
}

func (p *parserT) parseCreId(v ast.Node) (string, error) {
	s, err := p.nodeToString(v)
	if err != nil {
		return "", err
	}

	if !validCreIdRegex.MatchString(s) {
		err := fmt.Errorf("%w: id value must be at least 4 characters and contain only letters, numbers, or hyphens", ErrBadIdentifier)
		return "", p.wrapError(v, err)
	}
	return s, nil
}

func (p *parserT) parseSeverityNode(v ast.Node) (uint, error) {
	severity, err := p.nodeToUint(v)
	if err != nil {
		return 0, err
	}

	switch severity {
	case SeverityCritical, SeverityHigh, SeverityMedium, SeverityLow, SeverityInfo:

	default:

		if p.strict {
			err := fmt.Errorf("%w: severity value must be between 0 and 4", ErrBadSeverity)
			return 0, p.wrapError(v, err)
		}
	}
	return severity, nil
}

func (p *parserT) parseApplicationsNode(v ast.Node) ([]AstAppT, error) {
	apps := []AstAppT{}

	seq, err := p.nodeToSequence(v)
	if err != nil {
		return nil, err
	}

	for _, appNode := range seq.Values {
		app, err := p.parseApplicationNode(appNode)
		if err != nil {
			return nil, err
		}
		apps = append(apps, *app)
	}

	return apps, nil
}

func (p *parserT) parseApplicationNode(node ast.Node) (*AstAppT, error) {

	mapping, err := p.nodeToMapping(node)
	if err != nil {
		return nil, err
	}

	var (
		app AstAppT
	)

	for _, v := range mapping.Values {

		key, err := p.nodeToString(v.Key)
		if err != nil {
			return nil, err
		}

		switch key {
		case kwAppName:
			app.Name, err = p.nodeToString(v.Value)

		case kwAppProcessName:
			app.ProcessName, err = p.nodeToString(v.Value)

		case kwAppProcessPath:
			app.ProcessPath, err = p.nodeToString(v.Value)

		case kwAppContainer:
			app.ContainerName, err = p.nodeToString(v.Value)

		case kwAppImage:
			app.ImageUrl, err = p.nodeToString(v.Value)

		case kwAppRepo:
			app.RepoUrl, err = p.nodeToString(v.Value)

		case kwAppVersion:
			app.Version, err = p.nodeToString(v.Value)

		default:
			if p.strict {
				err = p.wrapError(v, fmt.Errorf("%w: %s", ErrUnexpectedKey, key))
			}
		}

		if err != nil {
			return nil, err
		}
	}

	return &app, nil
}
