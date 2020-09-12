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
	"math"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// RaftLog manage the log entries, its struct look like:
//
//  snapshot/first.....applied....committed....stabled.....last
//  --------|------------------------------------------------|
//                            log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	// see test TestFollowerAppendEntries2AB
	// stabled is all entries that follower confirm that is same with current leader
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
}

func getEntriesFromStorage(storage Storage) []pb.Entry {
	i, err := storage.FirstIndex()
	if err != nil {
		return make([]pb.Entry, 0)
	}
	j, err := storage.LastIndex()
	if err != nil {
		return make([]pb.Entry, 0)
	}
	result, err := storage.Entries(i, j+1)
	if err != nil {
		return make([]pb.Entry, 0)
	}
	return result
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	entries := getEntriesFromStorage(storage)
	raftLog := RaftLog{
		entries:   entries,
		storage:   storage,
		committed: None,
		applied:   None,
		stabled:   uint64(len(entries)),
	}
	return &raftLog
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	// stabled is 1-indexed
	return l.entries[l.stabled:]
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	return l.entries[l.applied:l.committed]
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	if len(l.entries) == 0 {
		return 0
	}
	return uint64(len(l.entries))
}

func (l *RaftLog) lastEntry() *pb.Entry {
	if len(l.entries) == 0 {
		return nil
	}
	return &l.entries[l.LastIndex()-1]
}

// NotFoundTerm means the log doesn't exist
const NotFoundTerm uint64 = math.MaxUint64

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	if i > uint64(len(l.entries)) {
		// fmt.Printf("[Term] i = %v, l.entries.len = %v\n", i, uint64(len(l.entries)))
		// doesn't exist
		return NotFoundTerm, nil
	}
	return l.entries[i-1].Term, nil
}
