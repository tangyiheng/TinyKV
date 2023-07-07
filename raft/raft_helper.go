package raft

import pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"

// reset 重置raft peer的状态
func (r *Raft) reset(term uint64) {
	if term != r.Term {
		r.Term = term
	}
}

func (r *Raft) stepFollower(m pb.Message) error {
	return nil
}

func (r *Raft) stepCandidate(m pb.Message) error {
	return nil
}

func (r *Raft) stepLeader(m pb.Message) error {
	return nil
}
