package schema

const (
	ScopeOrganization = "organization"
	ScopeCluster      = "cluster"
	ScopeNode         = "node"
	ScopeDefault      = "default"
)

type NodeTypeT string

const (
	NodeTypeSeq    NodeTypeT = "machine_seq"
	NodeTypeSet    NodeTypeT = "machine_set"
	NodeTypeLogSeq NodeTypeT = "log_seq"
	NodeTypeLogSet NodeTypeT = "log_set"
	NodeTypePromQL NodeTypeT = "promql"
)

func (t NodeTypeT) String() string {
	return string(t)
}
