package raft

import (
	"fmt"
	"log"
	"math/rand"
	"time"
)

// Debugging
const Debug = true

const baseElectionTimeout = 300
const baseHeartbeatTimeout = 150

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}

func RandomElectionTimeout() time.Duration {
	return time.Duration(baseElectionTimeout+rand.Intn(baseElectionTimeout)) * time.Millisecond
}

func StableHeartbeatTimeout() time.Duration {
	return time.Duration(baseHeartbeatTimeout) * time.Millisecond
}

func (args RequestVoteArgs) String() string {
	return fmt.Sprintf("{Term:%v, CandidateId:%v, LastLogIndex:%v, LastLogTerm:%v}", args.Term, args.CandidateId, args.LastLogIndex, args.LastLogTerm)
}

func (reply RequestVoteReply) String() string {
	return fmt.Sprintf("{Term:%v, VoteGranted:%v}", reply.Term, reply.VoteGranted)
}

func (args AppendEntriesArgs) String() string {
	return fmt.Sprintf("{Term:%v, LeaderId:%v, PrevLogIndex:%v, PrevLogTerm:%v, LeaderCommit:%v, Entries:%v}", args.Term, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, args.LeaderCommit, args.Entries)
}

func (reply AppendEntriesReply) String() string {
	return fmt.Sprintf("{Term:%v, Success:%v, ConflictIndex:%v, ConflictTerm:%v}", reply.Term, reply.Success, reply.ConflictIndex, reply.ConflictTerm)
}
