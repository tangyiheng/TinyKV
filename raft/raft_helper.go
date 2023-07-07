package raft

import pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"

// send 发送消息
func (r *Raft) send(m pb.Message) {
	if m.From == None {
		m.From = r.id
	}
	if m.Term == None {
		m.Term = r.Term
	}
	r.msgs = append(r.msgs, m)
}

// reset 重置raft peer的状态
func (r *Raft) reset(term uint64) {
	if term != r.Term {
		r.Term = term
	}
}

func (r *Raft) stepFollower(m pb.Message) error {
	// TODO
	return nil
}

func (r *Raft) stepCandidate(m pb.Message) error {
	// TODO
	return nil
}

func (r *Raft) stepLeader(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgBeat:
		// 广播心跳
		// 领导者向其跟随者发送'MessageType_MsgHeartbeat'类型的心跳信号
		r.bcastHeartbeat()
		return nil
	}
	return nil
}

// follower或candidate需要记录选举超时
func (r *Raft) tickElection() {
	// TODO
}

// leader需要记录心跳超时
func (r *Raft) tickHeartbeat() {
	r.heartbeatElapsed++
	// 心跳超时
	if r.heartbeatElapsed >= r.heartbeatTimeout {
		r.heartbeatElapsed = 0 // 重置心跳超时
		// 发送beat心跳消息（本地消息）
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
