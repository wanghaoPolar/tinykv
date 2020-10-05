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
	"math/rand"
	"sync"

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
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
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
type Progress struct {
	Match, Next, Committed uint64
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
	votes   map[uint64]bool
	rejects map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// number of ticks since it reached last electionTimeout
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

	et    int
	ht    int
	mutex sync.Mutex
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	prs := make(map[uint64]*Progress)
	for _, pid := range c.peers {
		prs[pid] = &Progress{Next: 0, Match: 0, Committed: 0}
	}
	hard, _, _ := c.Storage.InitialState()
	raftLog := newLog(c.Storage)
	raftLog.committed = hard.Commit
	// read from storage
	raft := Raft{
		id:   c.ID,
		Term: hard.Term,
		// RaftLog: , // read from storage?
		State:            StateFollower,
		msgs:             make([]pb.Message, 0),
		et:               c.ElectionTick,
		ht:               c.HeartbeatTick,
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  randomElectionTimeout(c.ElectionTick),
		Lead:             None,
		Vote:             hard.Vote,
		RaftLog:          raftLog,
		Prs:              prs,
		votes:            make(map[uint64]bool),
		rejects:          make(map[uint64]bool),
	}
	return &raft
}

func randomElectionTimeout(et int) int {
	max := et * 2
	min := et
	result := rand.Intn(max-min) + min
	return result
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
// try match at progress.next
func (r *Raft) sendAppend(to uint64) bool {
	// when sending an AppendEntries RPC, the leader includes the index and term of the entry in its log that immediately precedes the new entries.
	// progress := r.Prs[to]
	// prevIndex := progress.Next - 1
	// prevTerm, err := r.RaftLog.Term(prevIndex)
	// if err != nil {
	// return false
	// }
	// entries := r.RaftLog.entries[prevIndex+1:]
	return false
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	// msg := pb.Message(pb.Message{From: r.id, To: to, Term: r.Term, MsgType: pb.MessageType_MsgHeartbeat})
}

func (r *Raft) sendRequestVote(to uint64) bool {
	return false
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	r.electionElapsed++
	r.heartbeatElapsed++

	if r.State == StateLeader && r.heartbeatElapsed >= r.heartbeatTimeout {
		r.appendHeartbeatMsg()
		r.heartbeatElapsed = 0
	}
	if r.electionElapsed >= r.electionTimeout {
		r.Step(pb.Message{From: r.id, To: r.id, MsgType: pb.MessageType_MsgHup})
		r.electionElapsed = 0
	}
}

func (r *Raft) appendHeartbeatMsg() {
	for peerID := range r.Prs {
		if r.id != peerID {
			r.msgs = append(r.msgs, pb.Message{From: r.id, To: peerID, Term: r.Term, MsgType: pb.MessageType_MsgHeartbeat})
		}
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	r.State = StateFollower
	r.Term = term
	r.Lead = lead
	r.Vote = lead
	r.electionTimeout = randomElectionTimeout(r.et)
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// fmt.Printf("id = %v become candidate\n", r.id)
	r.Lead = None
	r.Vote = r.id
	r.State = StateCandidate
	r.electionTimeout = randomElectionTimeout(r.et)
	r.Term++
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	r.State = StateLeader
	r.Lead = r.id
	// init progress
	for i := range r.Prs {
		if i != r.id {
			r.Prs[i] = &Progress{
				Next:      r.RaftLog.lastEntry().GetIndex() + 1,
				Match:     0,
				Committed: 0,
			}
		} else {
			lastIndex := r.RaftLog.LastIndex()
			r.Prs[i] = &Progress{
				Next:      lastIndex + 1,
				Match:     lastIndex,
				Committed: 0,
			}
		}
	}
	// fmt.Printf("%v become leader\n", r.id)
	// NOTE: Leader should propose a noop entry on its term
	r.Step(pb.Message{MsgType: pb.MessageType_MsgPropose, To: r.id, Entries: []*pb.Entry{{}}})
}

func (r *Raft) isEntryUpdated(term uint64, index uint64) bool {
	lastEntry := r.RaftLog.lastEntry()
	if lastEntry == nil {
		return true
	}
	// fmt.Printf("current term/index: %v/%v; incoming term/index: %v/%v\n", lastEntry.Term, lastEntry.Index, term, index)
	if lastEntry.Term == term {
		if lastEntry.Index <= index {
			return true
		}
		return false
	}
	return lastEntry.Term <= term
}

func (r *Raft) handleRequestVoteMsg(m pb.Message) error {
	var err error = nil
	reject := true
	// fmt.Printf("self id = %v, self term = %v, m.Term = %v, m.Index = %v, m.LogTerm = %v\n", r.id, r.Term, m.Term, m.Index, m.LogTerm)
	// Reply reject if term < currentTerm
	if m.Term < r.Term {
		// fmt.Printf("%v reject because term: %v < %v\n", r.id, m.Term, r.Term)
		reject = true
	} else {
		// If votedFor is null or candidateId
		// and candidate’s log is at least as up-to-date as receiver’s log
		// grant vot
		isUpdated := r.isEntryUpdated(m.LogTerm, m.Index)
		if (r.Vote == None || r.Vote == m.From) && isUpdated {
			reject = false
		}
		// fmt.Printf("%v, isUpdated %v, vote: %v\n", r.id, isUpdated, r.Vote)
	}
	// fmt.Printf("%v receive RequestVote from %v, reject: %v, current term : %v, request term: %v\n", r.id, m.From, reject, r.Term, m.Term)
	if !reject {
		r.Vote = m.From
	}
	r.msgs = append(r.msgs, pb.Message{From: r.id, To: m.From, Term: r.Term, MsgType: pb.MessageType_MsgRequestVoteResponse, Reject: reject, Leader: r.Lead})
	return err
}

func (r *Raft) handleMsgAppend(m pb.Message) error {
	if m.Term < r.Term {
		r.msgs = append(r.msgs, pb.Message{From: r.id, To: m.From, Term: r.Term, MsgType: pb.MessageType_MsgAppendResponse, Reject: true, Leader: r.Lead})
		return nil
	}
	r.Lead = m.From
	var err error = nil
	var selfLogTerm uint64 = 0
	// fmt.Printf("[handleMsgAppend] id = %v, m.Index = %v, m.L\n", r.id, m.Index)
	if m.Index != 0 {
		t, err := r.RaftLog.Term(m.Index)
		if err != nil {
			// TODO send msg back
			return err
		}
		selfLogTerm = t
	}
	// Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
	if selfLogTerm == NotFoundTerm || selfLogTerm != m.LogTerm {
		// fmt.Printf("[handleMsgAppend] reject because log not match, selfLogTerm = %v, m.LogTerm = %v\n", selfLogTerm, m.LogTerm)
		r.msgs = append(r.msgs, pb.Message{From: r.id, To: m.From, Term: r.Term, MsgType: pb.MessageType_MsgAppendResponse, Reject: true, Index: m.Index})
		return nil
	}
	// fmt.Printf("[handleMsgAppend] msg.entries: %v\n", m.Entries)
	// fmt.Printf("[handleMsgAppend] self.entries: %v\n", r.RaftLog.entries)
	// If an existing entry conflicts with a new one (same index but different terms)
	// delete the existing entry and all that follow it
	for i, e := range m.Entries {
		// index here is 1 based
		index := m.Index + uint64(i) + 1
		// follower don't have log here, can start copy
		if index > uint64(len(r.RaftLog.entries)) {
			var newEntries []pb.Entry
			for j := i; j < len(m.Entries); j++ {
				newEntries = append(newEntries, *m.Entries[j])
			}
			r.RaftLog.entries = append(r.RaftLog.entries, newEntries...)
			// TODO fix this
			// shouldn't change stabled on raft.go but some test need to change it
			// r.RaftLog.stabled = index - 1
			break
		}
		mTerm := e.Term
		selfTerm, err := r.RaftLog.Term(index)
		if err != nil {
			return err
		}
		// find confilict log, can start copy
		if mTerm != selfTerm {
			// fmt.Printf("[handleMsgAppend] conflict at Index: %v, mTerm: %v, selfTerm: %v\n", index, mTerm, selfTerm)
			r.RaftLog.entries = r.RaftLog.entries[0 : index-1]
			// Append any new entries not already in the log
			var newEntries []pb.Entry
			for i := 0; i < len(m.Entries); i++ {
				newEntries = append(newEntries, *m.Entries[i])
			}
			r.RaftLog.entries = append(r.RaftLog.entries, newEntries...)
			r.RaftLog.stabled = index - 1
			break
		}
	}

	// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	// fmt.Printf("r.id = %v, leaderCommit = %v, self commit = %v, self last entry index = %v\n", r.id, m.Commit, r.RaftLog.committed, r.RaftLog.lastEntry().Index)
	// fmt.Printf("%v\n", r.RaftLog.entries)
	if m.Commit > r.RaftLog.committed {
		r.RaftLog.committed = min(m.Commit, m.Index+uint64(len(m.Entries)))
		// fmt.Printf("r.id = %v, committed updated to %v\n", r.id, r.RaftLog.committed)
	}
	// the Index in MessageType_MsgAppendResponse is the index of the last log that just got replicated
	// the Commit in MessageType_MsgAppendResponse is the last committed index of follower's logs
	r.msgs = append(r.msgs, pb.Message{From: r.id, To: m.From, Term: r.Term, MsgType: pb.MessageType_MsgAppendResponse, Reject: false, Index: m.Index + uint64(len(m.Entries)), Commit: r.RaftLog.committed})
	return err
}

func (r *Raft) followerStep(m pb.Message) error {
	var err error = nil
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		{
			// fmt.Printf("id = %v receive MsgHup\n", r.id)
			r.becomeCandidate()
			r.startElection()
		}
	case pb.MessageType_MsgHeartbeat:
		{
			if m.Term > r.Term {
				r.becomeFollower(m.Term, m.Leader)
			}
			if m.Term >= r.Term {
				r.heartbeatElapsed = 0
			}
		}
	case pb.MessageType_MsgRequestVote:
		{
			err = r.handleRequestVoteMsg(m)
		}
	case pb.MessageType_MsgAppend:
		{
			if m.Term > r.Term {
				r.becomeFollower(m.Term, m.Leader)
			}
			// fmt.Printf("%v receive message append: LogTerm = %v\n", r.id, m.LogTerm)
			err = r.handleMsgAppend(m)
		}
	}
	return err
}
func (r *Raft) candidateStep(m pb.Message) error {
	var err error = nil
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		{
			r.becomeCandidate()
			r.startElection()
		}
	case pb.MessageType_MsgRequestVoteResponse:
		{
			// fmt.Printf("%v receive vote from %v, rejected: %v\n", r.id, m.From, m.Reject)
			// check if have majority now
			rejected := m.Reject
			if rejected {
				// test TestDuelingCandidates2AB do not allow this transition
				// if r.Term <= m.Term {
				// 	r.becomeFollower(m.Term, m.Leader)
				// 	break
				// }
				// if get more than half of reject, turn to follower
				r.rejects[m.From] = true
			}
			if !rejected {
				r.votes[m.From] = true
			}
			voteCount := 0
			rejectCount := 0
			for key := range r.Prs {
				if r.votes[key] {
					voteCount++
				} else if r.rejects[key] {
					rejectCount++
				}
			}
			// fmt.Printf("voteCount for %v: %v, rejectCount for: %v\n", r.id, voteCount, rejectCount)
			if voteCount*2 > len(r.Prs) {
				// fmt.Printf("%v become leader\n", r.id)
				r.becomeLeader()
			}
			if rejectCount*2 > len(r.Prs) {
				// this lead is not correct
				r.becomeFollower(m.Term, m.Leader)
			}
		}
	case pb.MessageType_MsgHeartbeat:
		{
			// check if another server is new leader
		}
	case pb.MessageType_MsgAppend:
		{
			if m.Term >= r.Term {
				r.becomeFollower(m.Term, m.Leader)
			}
			// fmt.Printf("%v receive message append: LogTerm = %v\n", r.id, m.LogTerm)
			err = r.handleMsgAppend(m)
		}
	case pb.MessageType_MsgRequestVote:
		{
			err = r.handleRequestVoteMsg(m)
		}
	}
	return err
}

func (r *Raft) leaderStep(m pb.Message) error {
	var err error = nil
	switch m.MsgType {
	case pb.MessageType_MsgBeat:
		{
			// fmt.Printf("[Heartbeat] leader id = %v, msgBeat", r.id)
			for peerID := range r.Prs {
				if peerID != r.id {
					r.msgs = append(r.msgs, pb.Message{MsgType: pb.MessageType_MsgHeartbeat, To: peerID})
				}
			}
		}
	case pb.MessageType_MsgPropose:
		{
			if len(m.Entries) != 1 {
				panic("entry length is not 1")
			}
			data := m.Entries[0].Data
			index := r.RaftLog.LastIndex() + 1
			// fmt.Printf("[leaderStep MsgPropose] index: %v\n", index)
			newEntry := pb.Entry{Data: data, Term: r.Term, Index: index}

			// 1. the leader appends the proposal to its log as a new entry
			r.RaftLog.entries = append(r.RaftLog.entries, newEntry)
			// update leader process
			r.Prs[r.id].Match = index
			r.Prs[r.id].Next = index + 1

			// 2. then issues AppendEntries RPCs in parallel to each of the other servers to replicate the entry.
			for i := range r.Prs {
				if i != r.id {
					// fmt.Printf("Prs[%v].Next = %v\n", i, r.Prs[i].Next)
					lastIndex := r.Prs[i].Next - 1
					lastTerm := uint64(0)
					if lastIndex != 0 {
						lastTerm, err = r.RaftLog.Term(lastIndex)
						if err != nil {
							panic(err)
						}
					}
					sendEntries := make([]*pb.Entry, 0)
					for j := lastIndex + 1; j <= uint64(len(r.RaftLog.entries)); j++ {
						sendEntries = append(sendEntries, &r.RaftLog.entries[j-1])
					}
					r.msgs = append(r.msgs, pb.Message{MsgType: pb.MessageType_MsgAppend, Entries: sendEntries, Term: r.Term, LogTerm: lastTerm, Index: lastIndex, Commit: r.RaftLog.committed, From: r.id, To: i})
				}
			}
			// 3. when sending an AppendEntries RPC, the leader includes the index and term of the entry in its log that immediately precedes the new entries.
			// 4. it writes the new entry into stable storage.
			// s.Append(r.RaftLog.unstableEntries())
			// edge case: only one node
			if len(r.Prs) == 1 {
				r.RaftLog.committed = index
			}
		}
	case pb.MessageType_MsgHeartbeatResponse:
		{
			if m.Term > r.Term {
				r.becomeFollower(m.Term, m.Leader)
				// TODO change to follower, lead is current_leader
				break
			}
		}
	case pb.MessageType_MsgAppend:
		{
			if m.Term > r.Term {
				r.becomeFollower(m.Term, m.Leader)
			}
			r.sendAppend(m.To)
		}
	case pb.MessageType_MsgAppendResponse:
		{
			// fmt.Printf("leader term = %v\n", r.Term)
			// fmt.Printf("[leaderStep MessageType_MsgAppendResponse] leader: %v, follower: %v, append reject: %v, m.Index = %v\n", r.id, m.From, m.Reject, m.Index)
			// update committed
			r.Prs[m.From].Committed = m.Commit
			if m.Reject {
				// not matched, decrease next by one
				r.Prs[m.From].Next--
				// send another one
				if m.Index == 0 {
					panic("[leaderStep MessageType_MsgAppendResponse] reject = true and m.Index = 0")
				}
				lastIndex := m.Index - 1
				lastTerm := uint64(0)
				if lastIndex != 0 {
					lastTerm, err = r.RaftLog.Term(lastIndex)
					if err != nil {
						panic(err)
					}
				}
				sendEntries := make([]*pb.Entry, 0)
				for i := m.Index; i <= uint64(len(r.RaftLog.entries)); i++ {
					sendEntries = append(sendEntries, &r.RaftLog.entries[i-1])
				}
				r.msgs = append(r.msgs, pb.Message{MsgType: pb.MessageType_MsgAppend, Entries: sendEntries, Term: r.Term, LogTerm: lastTerm, Index: lastIndex, Commit: r.RaftLog.committed, From: r.id, To: m.From})
			} else {
				if r.Prs[m.From].Match > m.Index {
					break
				}
				// update match
				matchedIndex := m.Index
				matchedTerm := uint64(0)
				if matchedIndex > 0 {
					matchedTerm, err = r.RaftLog.Term(matchedIndex)
					if err != nil {
						panic(err)
					}
				}

				// update progress
				r.Prs[m.From].Match = m.Index
				r.Prs[m.From].Next = m.Index + 1

				// fmt.Printf("Log Term = %v, Index = %v replicated to %v\n", matchedTerm, matchedIndex, m.From)
				// fmt.Printf("matchedTerm = %v, r.Term = %v, matchedIndex = %v, r.RaftLog.committed = %v\n", matchedTerm, r.Term, matchedIndex, r.RaftLog.committed)
				isCommittedUpdated := false
				if matchedTerm == r.Term && matchedIndex > r.RaftLog.committed {
					// if is replicated in major nodes, commit
					count := 1
					for i, e := range r.Prs {
						if i != r.id && e.Match >= matchedIndex {
							count++
						}
					}
					// fmt.Printf("count of matchedIndex %v = %v\n", matchedIndex, count)
					// get ceil
					if count >= ((len(r.Prs) + 2 - 1) / 2) {
						// fmt.Printf("leader raftlog committed updated: %v\n", matchedIndex)
						// TODO when committed updated, need to inform follower
						r.RaftLog.committed = matchedIndex
						isCommittedUpdated = true
					}
				}
				// fmt.Printf("[update committed] follower commited = %v, leader committed = %v\n", m.Commit, r.RaftLog.committed)

				// for a follower, if is fully matched, and committed is not updated
				// send void appendEntires
				lastIndex := r.RaftLog.LastIndex()
				for i, e := range r.Prs {
					if i == r.id {
						continue
					}
					if isCommittedUpdated || i == m.From {
						if e.Match >= lastIndex && e.Committed < r.RaftLog.committed {
							// send to update follower committed
							lastTerm, err := r.RaftLog.Term(lastIndex)
							if err != nil {
								panic("panic when update committed")
							}
							sendEntries := make([]*pb.Entry, 0)
							for j := e.Match + 1; j <= uint64(len(r.RaftLog.entries)); j++ {
								sendEntries = append(sendEntries, &r.RaftLog.entries[j-1])
							}
							// fmt.Printf("update follower %v committed to %v\n", i, r.RaftLog.committed)
							r.msgs = append(r.msgs, pb.Message{MsgType: pb.MessageType_MsgAppend, Entries: sendEntries, Term: r.Term, LogTerm: lastTerm, Index: lastIndex, Commit: r.RaftLog.committed, From: r.id, To: i})
						}
					}
				}
			}
		}
	case pb.MessageType_MsgRequestVoteResponse:
		{
			// fmt.Printf("%v receive MsgRequestVoteResponse as leader\n", r.id)
		}
	case pb.MessageType_MsgRequestVote:
		{
			err = r.handleRequestVoteMsg(m)
		}
	}
	return err
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	if m.Term > r.Term {
		// TODO fix
		r.becomeFollower(m.Term, m.Leader)
	}
	var err error = nil
	switch r.State {
	case StateFollower:
		{
			err = r.followerStep(m)
		}
	case StateCandidate:
		{
			err = r.candidateStep(m)
		}
	case StateLeader:
		{
			err = r.leaderStep(m)
		}
	}
	return err
}

func (r *Raft) startElection() {
	// fmt.Printf("%v start election\n", r.id)
	r.Vote = r.id
	r.votes = make(map[uint64]bool)
	r.rejects = make(map[uint64]bool)
	r.votes[r.id] = true

	if len(r.Prs) < 3 {
		r.becomeLeader()
	}

	lastLogIndex := r.RaftLog.LastIndex()
	lastLogTerm, err := r.RaftLog.Term(lastLogIndex)

	// fmt.Printf("lastLogIndex = %v, lastLogTerm = %v\n", lastLogIndex, lastLogTerm)

	if err != nil {
		panic(err)
	}

	for peerID := range r.Prs {
		if peerID != r.id {
			r.msgs = append(r.msgs, pb.Message{From: r.id, To: peerID, Term: r.Term, MsgType: pb.MessageType_MsgRequestVote, Index: lastLogIndex, LogTerm: lastLogTerm})
		}
	}
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	// r.msgs = append(r.msgs, m)
	// for TestHandleMessageType_MsgAppend2AB
	// Step directly
	r.Step(m)
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	r.msgs = append(r.msgs, m)
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
