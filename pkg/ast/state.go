package ast

// State struct for tracking rule metadata, origin count, unique ID generation, depth, and rank during parsing.
// This is passed through recursive calls to ensure consistent state management and error reporting.

type ruleState struct {
	meta      *AstMetadataT
	addr      *AstNodeAddressT
	origin    *int
	idCounter *uint32
	rank      uint32
}

func newRuleState(meta *AstMetadataT) ruleState {
	var (
		originCnt int
		idCounter uint32
	)
	return ruleState{
		meta:      meta,
		origin:    &originCnt,
		idCounter: &idCounter,
	}
}

func (s ruleState) incOrigin() int {
	*s.origin++
	return *s.origin
}

func (s ruleState) getOrigin() int {
	return *s.origin
}

func (s ruleState) incRank() ruleState {
	s.rank++
	return s
}

func (s ruleState) setRank(r uint32) ruleState {
	s.rank = r
	return s
}

func (s ruleState) nextId() uint32 {
	id := *s.idCounter
	*s.idCounter++
	return id
}

func (s ruleState) pushNode(ty AstNodeType) ruleState {
	var (
		depth     uint32
		reserveId = s.nextId()
	)

	if s.addr != nil {
		depth = s.addr.Depth + 1
	}

	s.addr = &AstNodeAddressT{
		Type:     ty,
		RuleId:   s.meta.Id,
		RuleHash: s.meta.Hash,
		Rank:     s.rank,
		Depth:    depth,
		NodeId:   reserveId,
	}

	return s
}
