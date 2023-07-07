// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"
	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	// peers包含raft集群中所有节点（包括自身）的ID。
	// 只有在启动新的raft集群时才应设置它。
	// 如果设置了peers，则从先前的配置重新启动raft将导致恐慌。
	// peer是私有的，目前仅用于测试。
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	// ElectionTick是在选举之间必须经过的Node.Tick调用次数。
	// 也就是说，如果一个跟随者在ElectionTick经过之前没有收到当前任期的领导者的任何消息，它将成为候选人并开始选举。
	// ElectionTick必须大于HeartbeatTick。我们建议将ElectionTick设置为10倍的HeartbeatTick，以避免不必要的领导者切换。
	ElectionTick int

	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	// HeartbeatTick是在心跳之间必须经过的Node.Tick调用次数。
	// 也就是说，领导者每经过HeartbeatTick次调用就发送心跳消息以维持其领导地位。
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
// Progress 代表领导者对追随者的进展观察。
// 领导者维护所有追随者的进度，并根据其进度向追随者发送条目。
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// 随机选举超时时间
	randomElectionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64
}

// newRaft return a raft peer with the given config
// 通过给定的config新建一个raft的实例
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	// 创建（或恢复）raft log
	log := newLog(c.Storage)
	// 获取已保存的HardState信息：Term（任期），Vote（已投票id），Commit（已提交日志index）
	// 获取已保存的ConfState信息：Nodes（节点id数组）
	hs, _, err := c.Storage.InitialState()
	if err != nil {
		panic(err)
	}
	// 创建（或恢复）raft 实例
	r := &Raft{
		id:                    c.ID, // raft节点id
		Term:                  0,
		Vote:                  hs.Vote,                    // 投票id
		RaftLog:               log,                        // raft日志
		Prs:                   make(map[uint64]*Progress), // 集群同步进度信息
		State:                 StateFollower,              // 状态
		votes:                 make(map[uint64]bool),      // 投票数组
		msgs:                  nil,
		Lead:                  0,
		heartbeatTimeout:      c.HeartbeatTick,
		electionTimeout:       c.ElectionTick,
		randomElectionTimeout: 0,
		heartbeatElapsed:      0,
		electionElapsed:       0,
		leadTransferee:        0,
		PendingConfIndex:      0,
	}
	// 初始化 Prs
	for _, id := range c.peers {
		r.Prs[id] = &Progress{
			Match: 0,
			Next:  0,
		}
	}
	// 初始化raft实例角色（follower）（当服务器程序启动时，他们都是跟随者身份）
	r.becomeFollower(r.Term, None)
	return r
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	return false
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	r.send(pb.Message{From: r.id, To: to, Term: r.Term, MsgType: pb.MessageType_MsgHeartbeat})
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	if r.State == StateLeader { // leader
		r.tickHeartbeat()
	} else { // follower 或 candidate
		r.tickElection()
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.reset(term)           // 设置term
	r.Lead = lead           // 设置lead
	r.State = StateFollower // 设置为follower状态
	log.Debugf("%x became follower at term %d", r.id, r.Term)
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.reset(r.Term + 1)      // 设置term（自增）
	r.Vote = r.id            // 设置lead（给自己投票）
	r.State = StateCandidate // 设置为follower状态
	log.Debugf("%x became candidate at term %d", r.id, r.Term)
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.reset(r.Term)       // 设置term
	r.Lead = r.id         // 设置lead为自身
	r.State = StateLeader // 设置为leader状态
	log.Debugf("%x became leader at term %d", r.id, r.Term)
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	// 判断任期
	switch {
	case m.Term == 0:
	// 本地消息
	case m.Term > r.Term:
		// 更高任期的消息
		// 如果一个服务器的当前任期小于另一个服务器的任期，则将其当前任期更新为较大的值
		// 如果候选人或领导者发现自己的任期已过时，则立即恢复为跟随者状态。
		log.Debugf("%x [term: %d] received a %s message with higher term from %x [term: %d]", r.id, r.Term, m.MsgType, m.From, m.Term)
		if m.MsgType == pb.MessageType_MsgRequestVote {
			r.becomeFollower(m.Term, None)
		} else if m.MsgType == pb.MessageType_MsgAppend {
			// TestCandidateFallback2AA
			r.becomeFollower(m.Term, m.From)
		}
	case m.Term < r.Term:
		return nil
	}
	// 判断消息类型
	switch m.MsgType {
	default:
		// 判断raft节点状态
		switch r.State {
		case StateFollower:
			return r.stepFollower(m)
		case StateCandidate:
			return r.stepCandidate(m)
		case StateLeader:
			return r.stepLeader(m)
		}
	}
	return nil
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}
