package raft

import (
	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// send 发送消息
func (r *Raft) send(m pb.Message) {
	if m.From == None {
		m.From = r.id
	}
	if m.Term == None {
		m.Term = r.Term
	}
	log.Debugf("%x [term: %d] send %s to %x", m.From, m.Term, pb.MessageType_name[int32(m.MsgType)], m.To)
	r.msgs = append(r.msgs, m)
}

// reset 重置raft peer的状态
func (r *Raft) reset(term uint64) {
	if term != r.Term {
		r.Term = term
	}
}

func (r *Raft) stepFollower(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		// 发起选举
		r.campaign()
	case pb.MessageType_MsgRequestVote:
		// 判断是否投票
		if (r.Vote == None && r.Lead == None) || r.Vote == m.From {
			// 同意投票
			r.send(pb.Message{To: m.From, Term: m.Term, MsgType: pb.MessageType_MsgRequestVoteResponse, Reject: false})
		}
		r.Vote = m.From
	}
	return nil
}

func (r *Raft) stepCandidate(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		// 发起选举
		r.campaign()
	case pb.MessageType_MsgRequestVoteResponse:
		// 更新投票情况
		r.votes[m.From] = !m.Reject
		// 检查投票结果
		var granted, rejected int
		for id, _ := range r.Prs {
			if v, ok := r.votes[id]; ok {
				if v {
					granted++
				} else {
					rejected++
				}
			}
		}
		if granted > len(r.Prs)/2 { // 赢得选举
			r.becomeLeader() // 成为leader
			// TODO: 当选后添加空日志条目，并广播append消息
		} else if rejected > len(r.Prs)/2 { // 输掉选举
			r.becomeFollower(r.Term, None) // 成为follower
		}
	}
	return nil
}

func (r *Raft) stepLeader(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		log.Debugf("%x ignoring MsgHup because already leader", r.id)
	case pb.MessageType_MsgBeat:
		// 广播心跳
		// 领导者向其跟随者发送'MessageType_MsgHeartbeat'类型的心跳信号
		r.bcastHeartbeat()
	}
	return nil
}

// follower或candidate需要记录选举超时
func (r *Raft) tickElection() {
	r.electionElapsed++
	// 选举超时
	if r.electionElapsed >= r.electionTimeout {
		r.electionElapsed = 0
		// 如果发生选举超时，节点应该将'MessageType_MsgHup'传递给其Step方法并开始新的选举。
		r.Step(pb.Message{From: r.id, MsgType: pb.MessageType_MsgHup})
	}
}

// leader需要记录心跳超时
func (r *Raft) tickHeartbeat() {
	r.heartbeatElapsed++
	// 心跳超时
	if r.heartbeatElapsed >= r.heartbeatTimeout {
		r.heartbeatElapsed = 0 // 重置心跳超时
		// 发送beat心跳消息（本地消息），用于通知领导者向其跟随者发送'MessageType_MsgHeartbeat'类型的心跳信号。
		r.Step(pb.Message{From: r.id, MsgType: pb.MessageType_MsgBeat})
	}
}

// bcastHeartbeat 向集群中其他节点发送心跳消息
func (r *Raft) bcastHeartbeat() {
	for id, _ := range r.Prs {
		if id == r.id {
			continue
		}
		// 发送心跳RPC
		r.sendHeartbeat(id)
	}
}

// campaign follower或candidate发起选举
func (r *Raft) campaign() {
	// 1.要开始一次选举过程，跟随者先要增加自己的当前任期号并且转换到候选人状态
	r.becomeCandidate()
	// 2.并行的向集群中的其他服务器节点发送请求投票的 RPCs 来给自己投票
	for id, _ := range r.Prs {
		if id == r.id {
			// 给自己投票
			r.Vote = r.id
			r.votes[r.id] = true
			// corner case：集群中只有一个节点
			if len(r.Prs) == 1 {
				r.becomeLeader()
				return
			}
			continue
		}
		r.send(pb.Message{To: id, MsgType: pb.MessageType_MsgRequestVote})
	}
}
