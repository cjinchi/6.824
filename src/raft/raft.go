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
	"bytes"
	"encoding/gob"
	"math/rand"

	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

const (
	Follower  = iota
	Candidate = iota
	Leader    = iota
)

const (
	ElectionTimeoutFloor = 300
	ElectionTimeoutRange = 300
	HeartbeatInterval    = 100
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	// In figure 2: Persistent state on all servers
	currentTerm int
	votedFor    int // index of peers, -1 if null
	log         []LogEntry

	// In figure 2: Volatile state on all servers
	commitIndex int
	lastApplied int

	// In figure 2: Volatile state on leaders
	// SHOULD be reinitialized after election
	nextIndex  []int
	matchIndex []int

	//NOT in figure, for implementation
	roleState int
	timer     *time.Timer
	votes     int
	applyCh   chan ApplyMsg
}

type LogEntry struct {
	Term    int
	Command interface{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	//fmt.Printf("%v get lock in GetState\n", rf.me)
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.roleState == Leader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
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
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// AppendEntries RPC arguments structure.
//
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

//
// AppendEntries RPC reply structure.
//
type AppendEntriesReply struct {
	Term    int
	Success bool
}

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	//fmt.Printf("%v get lock in RequestVote\n", rf.me)
	defer rf.mu.Unlock()
	changed := false
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.changeRoleState(Follower)
		changed = true
	}

	if changed {
		rf.persist()
		rf.resetTimer()
	}

	changed = false
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
	} else if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && (len(rf.log) == 0 || logEntryCompare(rf.log[len(rf.log)-1].Term, len(rf.log), args.LastLogTerm, args.LastLogIndex) <= 0) {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		changed = true
	} else {
		reply.VoteGranted = false
	}
	reply.Term = rf.currentTerm

	if changed {
		rf.persist()
		rf.resetTimer()
	}

}

//
// AppendEntries RPC handler.
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	//fmt.Printf("term:%v; %v receives AE from %v before lock\n", rf.currentTerm, rf.me, args.LeaderId)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//fmt.Printf("term:%v; %v receives AE from %v\n", rf.currentTerm, rf.me, args.LeaderId)
	shouldTimerReset := false
	shouldPersist := false
	// make sure RPC comes from current leader
	if args.Term >= rf.currentTerm {
		shouldTimerReset = true
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		shouldPersist = true
		rf.changeRoleState(Follower)
	}

	if args.Term < rf.currentTerm {
		//1 in fig2
		reply.Success = false
	} else if args.PrevLogIndex != 0 && len(rf.log) < args.PrevLogIndex {
		// 2 in fig2
		reply.Success = false
	} else if args.PrevLogIndex != 0 && rf.log[args.PrevLogIndex-1].Term != args.PrevLogTerm {
		reply.Success = false
	} else {
		reply.Success = true
		// 3 & 4 in fig2
		if args.PrevLogIndex == 0 {
			rf.log = args.Entries[:]
		} else {
			rf.log = append(rf.log[:args.PrevLogIndex], args.Entries...)
		}
		shouldPersist = true
		// 5 in fig 2
		if args.LeaderCommit > rf.commitIndex {
			rf.updateCommitIndex(min(args.LeaderCommit, len(rf.log)))
		}
	}
	reply.Term = rf.currentTerm

	if shouldPersist {
		rf.persist()
	}
	if shouldTimerReset {
		rf.resetTimer()
	}
	//fmt.Printf("term:%v; %v receives AE from %v end\n", rf.currentTerm, rf.me, args.LeaderId)

}
func (rf *Raft) updateCommitIndex(newIndex int) {
	for i := rf.commitIndex + 1; i <= newIndex; i++ {
		rf.applyCh <- ApplyMsg{
			CommandValid: true,
			CommandIndex: i,
			Command:      rf.log[i-1].Command,
			//UseSnapshot: false,
			//Snapshot:    nil,
		}
		rf.lastApplied = i
	}
	rf.commitIndex = newIndex
}

func (rf *Raft) resetTimer() {
	duration := time.Duration(ElectionTimeoutFloor+rand.Intn(ElectionTimeoutRange)) * time.Millisecond
	rf.timer.Reset(duration)
}

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {

	// Your code here (2B).
	rf.mu.Lock()
	//fmt.Printf("%v get lock in Start\n", rf.me)
	defer rf.mu.Unlock()

	index := -1
	term := -1
	isLeader := rf.roleState == Leader

	if isLeader {
		var entry = LogEntry{
			Term:    rf.currentTerm,
			Command: command,
		}
		rf.log = append(rf.log, entry)
		index = len(rf.log)
		rf.persist()
		term = rf.currentTerm
	}

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		select {
		case <-rf.timer.C:
			rf.mu.Lock()
			//fmt.Printf("%v get lock in ticker\n", rf.me)
			if !rf.killed() && rf.roleState != Leader {
				rf.changeRoleState(Candidate)
			}
			rf.mu.Unlock()
		}

	}
}

func (rf *Raft) changeRoleState(newRoleState int) {
	if newRoleState == Follower {
		rf.roleState = Follower
	} else if newRoleState == Candidate {
		//fmt.Printf("term:%v; %v start election\n", rf.currentTerm, rf.me)
		rf.roleState = Candidate
		// start election
		rf.currentTerm++
		rf.votedFor = rf.me
		rf.votes = 1
		rf.resetTimer()
		rf.persist()
		rf.sendRequestVoteToAll()
	} else if newRoleState == Leader {
		rf.roleState = Leader
		rf.nextIndex = make([]int, len(rf.peers))
		rf.matchIndex = make([]int, len(rf.peers))
		for i := 0; i < len(rf.nextIndex); i++ {
			rf.nextIndex[i] = len(rf.log) + 1
		}
		for i := 0; i < len(rf.matchIndex); i++ {
			rf.matchIndex[i] = 0
		}

		go func() {
			for {
				rf.mu.Lock()
				//fmt.Printf("%v get lock in changeRoleState\n", rf.me)
				if !rf.killed() && rf.roleState == Leader {
					rf.sendAppendEntriesToAll()
					rf.mu.Unlock()
				} else {
					rf.mu.Unlock()
					break
				}

				time.Sleep(HeartbeatInterval * time.Millisecond)
			}
		}()
	}

}

func (rf *Raft) sendRequestVoteToAll() {
	var args = RequestVoteArgs{
		Term:        rf.currentTerm,
		CandidateId: rf.me,
	}
	if len(rf.log) == 0 {
		args.LastLogIndex = 0
		args.LastLogTerm = 0
	} else {
		args.LastLogIndex = len(rf.log)
		args.LastLogTerm = rf.log[len(rf.log)-1].Term
	}

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		target := i
		go func() {
			var reply = RequestVoteReply{}
			result := rf.sendRequestVote(target, &args, &reply)
			if result {
				rf.mu.Lock()
				//fmt.Printf("%v get lock in sendRequestVoteToAll\n", rf.me)
				shouldPersist := false
				if rf.currentTerm == args.Term && reply.VoteGranted {
					rf.votes++
					//fmt.Printf("term:%v; %v receives vote from %v\n", rf.currentTerm, rf.me, target)
					if rf.votes >= len(rf.peers)/2+1 && rf.roleState == Candidate {
						rf.changeRoleState(Leader)
					}
				}
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.votedFor = -1
					shouldPersist = true
					rf.changeRoleState(Follower)

				}
				if shouldPersist {
					rf.persist()
				}
				rf.mu.Unlock()
			}
		}()

	}

}

func (rf *Raft) sendAppendEntriesToAll() {
	for serverId := 0; serverId < len(rf.peers); serverId++ {
		if serverId == rf.me {
			continue
		}
		//fmt.Printf("term:%v; %v send AE to %v\n", rf.currentTerm, rf.me, serverId)

		target := serverId

		var args = AppendEntriesArgs{}
		args.Term = rf.currentTerm
		args.LeaderId = rf.me
		args.LeaderCommit = rf.commitIndex
		args.PrevLogIndex = rf.nextIndex[target] - 1
		if args.PrevLogIndex == 0 {
			args.PrevLogTerm = 0
		} else {
			args.PrevLogTerm = rf.log[args.PrevLogIndex-1].Term
		}
		args.Entries = rf.log[args.PrevLogIndex:]

		go func() {
			var reply = AppendEntriesReply{}
			result := rf.sendAppendEntries(target, &args, &reply)

			if !result || reply.Term <= args.Term && len(args.Entries) == 0 {
				return
			}

			rf.mu.Lock()
			//fmt.Printf("%v get lock in sendAppendEntriesToAll\n", rf.me)
			shouldPersist := false
			shouldResetTimer := false
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.votedFor = -1
				shouldPersist = true
				shouldResetTimer = true
				rf.changeRoleState(Follower)
			}
			if rf.roleState == Leader {
				if reply.Success {
					rf.nextIndex[target] = max(args.PrevLogIndex+len(args.Entries)+1, rf.nextIndex[target])
					rf.matchIndex[target] = max(args.PrevLogIndex+len(args.Entries), rf.matchIndex[target])
					N := rf.matchIndex[target]
					if N > rf.commitIndex && rf.log[N-1].Term == rf.currentTerm {
						deliveredCnt := 0
						for i := 0; i < len(rf.matchIndex); i++ {
							if i == rf.me || rf.matchIndex[i] >= N {
								deliveredCnt++
							}
						}
						if deliveredCnt >= len(rf.peers)/2+1 {
							rf.updateCommitIndex(N)

						}
					}
				} else {
					rf.nextIndex[target] = 1
				}
			}
			if shouldResetTimer {
				rf.resetTimer()
			}
			if shouldPersist {
				rf.persist()
			}
			rf.mu.Unlock()

		}()

	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.roleState = Follower
	rf.votes = 0
	rf.dead = 0
	rf.applyCh = applyCh
	rf.timer = time.NewTimer(time.Second)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	rf.resetTimer()
	go rf.ticker()

	return rf
}

func min(a int, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}

func max(a int, b int) int {
	if a > b {
		return a
	} else {
		return b
	}
}

// last paragraph of section 5.4.1
func logEntryCompare(aTerm int, aIndex int, bTerm int, bIndex int) int {
	if aTerm < bTerm {
		return -1
	} else if aTerm > bTerm {
		return 1
	} else {
		// aTerm == bTerm here
		if aIndex < bIndex {
			return -1
		} else if aIndex > bIndex {
			return 1
		} else {
			return 0
		}
	}
}
