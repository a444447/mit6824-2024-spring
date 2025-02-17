package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"

	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type Entry struct {
	Command interface{}
	Term    int
}

// 阅读论文可知, 当Leader发现Follower要求回退的日志已经被SnapShot截断时, 需要发生InstallSnapshot RPC
type InstallSnapshotArgs struct {
	Term              int         // leader’s term
	LeaderId          int         // so follower can redirect clients
	LastIncludedIndex int         // the snapshot replaces all entries up through and including this index
	LastIncludedTerm  int         // term of lastIncludedIndex snapshot file
	Data              []byte      //[] raw bytes of the snapshot chunk
	LastIncludedCmd   interface{} // 自己新加的字段, 用于在0处占位
}

type InstallSnapshotReply struct {
	Term int // currentTerm, for leader to update itself
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.RWMutex        // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor    int
	log         []Entry

	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int

	//其他

	applyCh        chan ApplyMsg
	elecTimer      *time.Timer
	heartbeatTimer *time.Timer
	role           int
	voteCnt        int
	muVote         sync.Mutex

	//snapshot
	snapShot          []byte //快照
	lastIncludedIndex int    //快照中最高索引
	lastIncludedTerm  int    //快照中最高term
}

// 定义一个转换函数
// 规定virtual idx是全局真实递增的索引
// 规定real idx是我们实际访问rf.log用的索引
// 规定我们除了访问rf.log使用real idx外，其他地方都是virtual idx
func (rf *Raft) RealLogIdx(vIdx int) int {
	return vIdx - rf.lastIncludedIndex
}

func (rf *Raft) VirtualLogIdx(rIdx int) int {
	return rIdx + rf.lastIncludedIndex
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// Your code here (3A).
	term := rf.currentTerm

	return term, rf.role == Leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.votedFor)
	e.Encode(rf.currentTerm)
	e.Encode(rf.log)

	//3D
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var votedFor int
	var currentTerm int
	var log []Entry
	var lastIncludedIndex int
	var lastIncludedTerm int
	if d.Decode(&votedFor) != nil ||
		d.Decode(&currentTerm) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil {
		DPrintf("server %v readPersist failed\n", rf.me)
	} else {
		// 2C
		rf.votedFor = votedFor
		rf.currentTerm = currentTerm
		rf.log = log
		// 2D
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm

		rf.commitIndex = lastIncludedIndex
		rf.lastApplied = lastIncludedIndex
		DPrintf("server %v  readPersist 成功\n", rf.me)
	}
}

func (rf *Raft) readSnapshot(data []byte) {
	if len(data) == 0 {
		DPrintf("server %v 读取快照失败: 无快照\n", rf.me)
		return
	}
	rf.snapShot = data
	DPrintf("server %v 读取快照c成功\n", rf.me)
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.commitIndex < index || index <= rf.lastIncludedIndex {
		//首先我们保证快照保存的一定是已经committed了的index,所以commitIndex < index
		//另外我们需要避免重复保存快照，所以要 index <= lastIncludedIndex
		DPrintf("server %v 拒绝了 Snapshot 请求, 其index=%v, 自身commitIndex=%v, lastIncludedIndex=%v\n", rf.me, index, rf.commitIndex, rf.lastIncludedIndex)
		return
	}
	DPrintf("server %v 同意了 Snapshot 请求, 其index=%v, 自身commitIndex=%v, 原来的lastIncludedIndex=%v, 快照后的lastIncludedIndex=%v\n", rf.me, index, rf.commitIndex, rf.lastIncludedIndex, index)

	rf.snapShot = snapshot
	rf.lastIncludedTerm = rf.log[rf.RealLogIdx(index)].Term

	//截断log
	rf.log = rf.log[rf.RealLogIdx(index):] //index位置的log保存在0索引处
	rf.lastIncludedIndex = index
	if rf.lastApplied < index {
		rf.lastApplied = index
	}
	rf.persist()
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("server %v 收到来自%v的投票请求 term:%v", rf.me, args.CandidateId, rf.currentTerm)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		DPrintf("server %v 拒绝向 server %v投票: 旧的term: %v,\n\targs= %+v\n", rf.me, args.CandidateId, args.Term, args)
		return
	}

	if args.Term > rf.currentTerm {
		rf.votedFor = -1
		rf.currentTerm = args.Term
		rf.role = Follower
		rf.persist()
	}

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		if args.LastLogTerm > rf.log[len(rf.log)-1].Term ||
			(args.LastLogTerm == rf.log[len(rf.log)-1].Term && args.LastLogIndex >= len(rf.log)-1) {
			//debug的时候发现args.LastLogTerm写成LastLogIndex了，导致TestFollowerFailure3B过不了(原因是假设ABC三个服务器，产生分区A｜BC，当A reconnect后，它对其他服务器投票一直是不同意（毕竟我写错了)
			//问题的关键是，由于A是不可能被同意的，但是它会不断term++,然后它不同意其他服务器的时候也会导致另一个服务器term++,最后的情况是没一个服务器能当选。
			// 2. If votedFor is null or candidateId, and candidate’s log is least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)

			rf.votedFor = args.CandidateId
			reply.Term = rf.currentTerm
			rf.currentTerm = args.Term
			rf.role = Follower
			//收到消息后重置
			rf.elecTimer.Reset(GetRandomElecInterval())
			reply.VoteGranted = true
			DPrintf("server %v 同意向 server %v投票\n\targs= %+v\n", rf.me, args.CandidateId, args)
			rf.persist()
			return
		} else {
			if args.LastLogTerm < rf.log[len(rf.log)-1].Term {
				DPrintf("server %v 拒绝向 server %v 投票: 更旧的LastLogTerm, args = %+v\n", rf.me, args.CandidateId, args)
			} else {
				DPrintf("server %v 拒绝向 server %v 投票: 更短的Log, args = %+v\n", rf.me, args.CandidateId, args)
			}
		}
	} else {
		DPrintf("server %v 拒绝向 server %v投票: 已投票\n\targs= %+v\n", rf.me, args.CandidateId, args)
	}
	reply.Term = rf.currentTerm
	reply.VoteGranted = false

}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	rf.elecTimer.Reset(GetRandomElecInterval())
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.role = Follower
		rf.persist()
	}

	if len(args.Entries) == 0 {
		// 心跳函数
		DPrintf("server %v 接收到 leader &%v 的心跳\n", rf.me, args.LeaderId)
	} else {
		DPrintf("server %v 收到 leader %v 的的AppendEntries: %+v \n", rf.me, args.LeaderId, args)
	}
	isConflict := false
	//首先查看prevLogIndex位置是否存在日志项
	if args.PrevLogIndex >= len(rf.log) {
		//prevLogIndex不存在日志项
		reply.Term = -1
		reply.XLen = len(rf.log)
		isConflict = true
	} else if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		//有日志，但是Term冲突
		reply.Term = rf.log[args.PrevLogIndex].Term
		i := args.PrevLogIndex
		for rf.log[i].Term == args.PrevLogTerm {
			i -= 1
		}
		reply.XIndex = i + 1
		isConflict = true

	}
	if isConflict {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	// if args.PrevLogIndex >= len(rf.log) || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
	// 	reply.Term = rf.currentTerm
	// 	reply.Success = false
	// 	return
	// }

	// 3. If an existing entry conflicts with a new one (same index
	// but different terms), delete the existing entry and all that
	// follow it (§5.3)
	if len(args.Entries) != 0 && len(rf.log) > args.PrevLogIndex+1 {
		DPrintf("server %v 的log与args发生冲突, 进行移除\n", rf.me)
		rf.log = rf.log[:args.PrevLogIndex+1]
	}
	// 4. Append any new entries not already in the log
	// 补充apeend的业务
	rf.log = append(rf.log, args.Entries...)
	rf.persist()
	if len(args.Entries) != 0 {
		// DPrintf("server %v 成功进行apeend, log: %+v\n", rf.me, rf.log)
		DPrintf("server %v 成功进行apeend\n", rf.me)
	}

	reply.Success = true
	reply.Term = rf.currentTerm
	if args.LeaderCommit > rf.commitIndex {
		// 5.If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
		if args.LeaderCommit > len(rf.log)-1 {
			rf.commitIndex = len(rf.log) - 1
		} else {
			rf.commitIndex = args.LeaderCommit
		}
	}

}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {

}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Your code here (3B).
	if rf.role != Leader {
		return -1, -1, false
	}
	newEntry := &Entry{Term: rf.currentTerm, Command: command}
	rf.log = append(rf.log, *newEntry)
	rf.persist()
	return len(rf.log) - 1, rf.currentTerm, true
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) SendHeartBeats() {
	DPrintf("server %v 开始发送心跳", rf.me)
	for !rf.killed() {
		<-rf.heartbeatTimer.C
		rf.mu.Lock()
		if rf.role != Leader {
			rf.mu.Unlock()
			return
		}

		for peer := range rf.peers {
			if peer == rf.me {
				continue
			}
			args := &AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: rf.nextIndex[peer] - 1,
				LeaderCommit: rf.commitIndex,
			}
			sendInstallSnapshot := false
			if args.PrevLogIndex < rf.lastIncludedIndex {
				DPrintf("leader %v 取消向 server %v 广播新的心跳, 改为发送sendInstallSnapshot, lastIncludedIndex=%v, nextIndex[%v]=%v, args = %+v \n", rf.me, i, rf.lastIncludedIndex, i, rf.nextIndex[i], args)
				sendInstallSnapshot = true
			} else if rf.VirtualLogIdx(len(rf.log)-1) >= rf.nextIndex[peer] {
				//有新log需要发送
				args.Entries = rf.log[rf.RealLogIdx(args.PrevLogIndex+1):]
				DPrintf("leader %v 开始向 server %v 广播新的AppendEntries\n", rf.me, peer)
			} else {
				args.Entries = make([]Entry, 0)
				DPrintf("leader %v 开始向 server %v 广播新的心跳, args = %+v \n", rf.me, peer, args)
			}
			if sendInstallSnapshot {
				go rf.handleInstallSnapshot(peer)
			} else {
				args.PrevLogTerm = rf.log[rf.RealLogIdx(args.PrevLogIndex)].Term
				go rf.handleHeartbeat(peer, args)
			}

		}
		rf.mu.Unlock()
		rf.heartbeatTimer.Reset(GetStableHeartbeatInterval())
	}
}

func (rf *Raft) handleInstallSnapshot(peer int) {
	reply := &InstallSnapshotReply{}
	rf.mu.Lock()
	if rf.role != Leader {
		rf.mu.Unlock()
		return
	}
	args := &InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
		Data:              rf.snapShot,
		LastIncludedCmd:   rf.log[0].Command,
	}
	rf.mu.Unlock()

	ok := rf.sendInstallSnapshot(peer, args, reply)
	if !ok {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.role = Follower
		rf.votedFor = -1
		rf.elecTimer.Reset(GetRandomElecInterval())
		rf.persist()
		return
	}
	rf.nextIndex[peer] = rf.VirtualLogIdx(1)
}

func (rf *Raft) handleHeartbeat(peer int, args *AppendEntriesArgs) {
	reply := &AppendEntriesReply{}
	ok := rf.sendAppendEntries(peer, args, reply)
	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term != rf.currentTerm {
		return
	}

	if reply.Success {
		rf.matchIndex[peer] = args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[peer] = rf.matchIndex[peer] + 1

		// 需要判断是否可以commit
		N := len(rf.log) - 1

		for N > rf.commitIndex {
			count := 1 // 1表示包括了leader自己
			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					continue
				}
				if rf.matchIndex[i] >= N && rf.log[N].Term == rf.currentTerm {
					count += 1
				}
			}
			if count > len(rf.peers)/2 {
				// 如果至少一半的follower回复了成功, 更新commitIndex
				rf.commitIndex = N
				break
			}
			N -= 1
		}
		return
	}

	if reply.Term > rf.currentTerm {
		DPrintf("server %v 旧的leader收到了心跳函数中更新的term: %v, 转化为Follower\n", rf.me, reply.Term)
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.role = Follower
		rf.elecTimer.Reset(GetRandomElecInterval())
		rf.persist()
		return
	}

	if reply.Term == rf.currentTerm && rf.role == Leader {
		// term仍然相同, 且自己还是leader, 表名对应的follower在prevLogIndex位置没有与prevLogTerm匹配的项
		// 将nextIndex自减再重试
		if reply.XTerm == -1 {
			//表示prelogindex在follwer中不存在
			if rf.lastIncludedIndex >= reply.XLen {
				//发现回退到lastIncludedIndex不能满足
				go rf.handleInstallSnapshot(peer)
			} else {
				rf.nextIndex[peer] = reply.XLen
			}
			return
		}
		i := rf.nextIndex[peer] - 1
		if i < rf.lastIncludedIndex {
			i = rf.lastIncludedIndex
		}
		for i > rf.lastIncludedIndex && rf.log[rf.RealLogIdx(i)].Term > reply.XTerm {
			i -= 1
		}
		if i == rf.lastIncludedIndex && rf.log[rf.RealLogIdx(i)].Term > reply.XTerm {
			//还需要继续找，但是已经被snapshot截断
			go rf.handleInstallSnapshot(peer)
		} else if rf.log[rf.RealLogIdx(i)].Term == reply.XTerm {
			rf.nextIndex[peer] = i + 1
		} else {
			// 之前PrevLogIndex发生冲突位置时, Follower的Term自己没有
			DPrintf("leader %v 收到 server %v 的回退请求, 冲突位置的Term为%v, server的这个Term从索引%v开始, 而leader对应的XTerm不存在, 回退前的nextIndex[%v]=%v, 回退后的nextIndex[%v]=%v\n", rf.me, serverTo, reply.XTerm, reply.XIndex, serverTo, rf.nextIndex[serverTo], serverTo, reply.XIndex)
			if reply.XIndex <= rf.lastIncludedIndex {
				// XIndex位置也被截断了
				// 添加InstallSnapshot的处理
				go rf.handleInstallSnapshot(peer)
			} else {
				rf.nextIndex[peer] = reply.XIndex
			}
		}
	}
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.currentTerm += 1
	rf.role = Candidate
	rf.votedFor = rf.me
	rf.voteCnt = 1
	// rf.elecTimer.Reset(GetRandomElecInterval()) //给自己投票也算消息
	DPrintf("server %v 开始发起新一轮投票, 新一轮的term为: %v", rf.me, rf.currentTerm)
	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: len(rf.log) - 1,
		LastLogTerm:  rf.log[len(rf.log)-1].Term,
	}
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		} else {
			go rf.collectVote(peer, args)
		}
	}

}

func (rf *Raft) collectVote(serverTo int, args *RequestVoteArgs) {
	voteAnswer := rf.GetVoteAnswer(serverTo, args)
	if !voteAnswer {
		return
	}
	rf.muVote.Lock()
	if rf.voteCnt > len(rf.peers)/2 {
		rf.muVote.Unlock()
		return
	}

	rf.voteCnt += 1
	if rf.voteCnt > len(rf.peers)/2 {
		rf.mu.Lock()
		if rf.role == Follower {
			// 有另外一个投票的协程收到了更新的term而更改了自身状态为Follower
			rf.mu.Unlock()
			rf.muVote.Unlock()
			return
		}
		rf.role = Leader
		for i := 0; i < len(rf.nextIndex); i++ {
			rf.nextIndex[i] = len(rf.log)
			rf.matchIndex[i] = 0
		}
		rf.mu.Unlock()
		go rf.SendHeartBeats()
	}

	rf.muVote.Unlock()
}

func (rf *Raft) GetVoteAnswer(server int, args *RequestVoteArgs) bool {
	sendArgs := *args
	reply := RequestVoteReply{}
	DPrintf("server %v sendrequestVote to %v", rf.me, server)
	ok := rf.sendRequestVote(server, &sendArgs, &reply)
	if !ok {
		return false
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if sendArgs.Term != rf.currentTerm {
		// 如果此时检测到currentTerm与args中的term不同，说明当前peer外已经有leader诞生并且修改了term
		return false
	}

	if reply.Term > rf.currentTerm {
		// 已经是过时的term了
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.role = Follower
		rf.persist()
	}
	return reply.VoteGranted
}

func (rf *Raft) commitChecker() {
	for !rf.killed() {
		rf.mu.Lock()
		for rf.commitIndex > rf.lastApplied {
			rf.lastApplied += 1
			msg := &ApplyMsg{
				CommandValid: true,
				Command:      rf.log[rf.lastApplied].Command,
				CommandIndex: rf.lastApplied,
			}
			rf.applyCh <- *msg
			DPrintf("server %v 准备将命令 %v(索引为 %v ) 应用到状态机\n", rf.me, msg.Command, msg.CommandIndex)
		}
		rf.mu.Unlock()
		time.Sleep(time.Duration(baseCommitCheckInterval))
	}
}

func (rf *Raft) ticker() {

	for rf.killed() == false {

		// Your code here (3A)
		// Check if a leader election should be started.

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		<-rf.elecTimer.C
		rf.mu.Lock()
		if rf.role != Leader {
			go rf.startElection()
		}
		rf.elecTimer.Reset(GetRandomElecInterval())
		rf.mu.Unlock()

	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.log = make([]Entry, 0)
	rf.log = append(rf.log, Entry{Term: 0})
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.role = Follower
	rf.applyCh = applyCh
	rf.elecTimer = time.NewTimer(0)
	rf.heartbeatTimer = time.NewTimer(0)
	rf.elecTimer.Reset(GetRandomElecInterval())
	// Your initialization code here (3A, 3B, 3C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	for i := 0; i < len(rf.nextIndex); i++ {
		rf.nextIndex[i] = rf.VirtualLogIdx(len(rf.log)) // raft中的index是从1开始的
	}
	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.commitChecker()
	return rf
}
