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
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
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
	state     int

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// persistent, for all
	currentTerm int
	votedFor    int
	logs        []Entry

	// volatile, for all
	commitIndex int
	lastApplied int

	// volatile, for leaders
	nextIndex  []int
	matchIndex []int

	nextElectionTime time.Time

	applyCh chan ApplyMsg
}

const (
	Leader = iota
	Candidate
	Follower
)

type Entry struct {
	Command interface{}
	Term    int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.state == Leader

	return term, isleader
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

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	// Your data here (2A).
	Term    int
	Success bool
}

func (rf *Raft) changeState(state int) {
	if state == rf.state {
		return
	}
	DPrintf("Node[%v] change state from %v to %v on term %v", rf.me, rf.state, state, rf.currentTerm)
	switch state {
	case Follower:
		rf.votedFor = -1
		rf.nextElectionTime = time.Now().Add(randomizedTimeDuration())
		DPrintf("Node[%v] has Reseted EL timer when converting to Follower", rf.me)
		rf.state = Follower
	case Candidate:
		rf.nextElectionTime = time.Now()
		rf.state = Candidate
	case Leader:
		rf.state = Leader
		for i := range rf.nextIndex {
			rf.nextIndex[i] = len(rf.logs) - 1
		}
	}

}

func determinedTimeDuration() time.Duration {
	return 30 * time.Millisecond
}

func randomizedTimeDuration() time.Duration {
	return time.Duration((rand.Intn(150) + 300) * int(time.Millisecond))
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer DPrintf("Node[%v] RV handler: request: %v reply:%v", rf.me, args, reply)
	if args.Term < rf.currentTerm || (args.Term == rf.currentTerm && (!rf.isLogUTD(args.LastLogIndex, args.LastLogTerm) || (rf.votedFor != -1 && rf.votedFor != args.CandidateId))) {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	if args.Term > rf.currentTerm {
		rf.changeState(Follower)
		rf.currentTerm = args.Term
	}
	rf.changeState(Follower)
	reply.Term = rf.currentTerm
	reply.VoteGranted = true
	rf.votedFor = args.CandidateId
	rf.nextElectionTime = time.Now().Add(randomizedTimeDuration())
}

func (rf *Raft) isLogUTD(index int, term int) bool {
	if term > rf.currentTerm || (term == rf.currentTerm && index > len(rf.logs)-1) {
		return true
	}
	return false
}

func (rf *Raft) AppendEntries(request *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer DPrintf("Node[%v] AE handler: request: %v reply: %v", rf.me, request, reply)
	if request.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}
	if request.LeaderCommit > rf.commitIndex {
		rf.commitIndex = request.LeaderCommit
	}
	if len(rf.logs)-1 < request.PrevLogIndex {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}
	if rf.logs[request.PrevLogIndex].Term != request.PrevLogTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}
	rf.changeState(Follower)
	reply.Success = true
	reply.Term = rf.currentTerm
	rf.logs = append(rf.logs, request.Entries...)
	rf.nextElectionTime = time.Now().Add(randomizedTimeDuration())

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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	index = len(rf.logs) - 1
	term = rf.currentTerm
	if rf.state != Leader {
		isLeader = false
		rf.mu.Unlock()
		return index, term, isLeader
	}
	request := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: len(rf.logs) - 1,
		PrevLogTerm:  rf.logs[len(rf.logs)-1].Term,
		Entries: append([]Entry{}, Entry{
			Command: command,
			Term:    rf.currentTerm,
		}),
		LeaderCommit: rf.commitIndex,
	}
	rf.logs = append(rf.logs, Entry{
		Command: command,
		Term:    rf.currentTerm,
	})
	rf.mu.Unlock()

	appendCh := make(chan int, 1)
	appendCh <- 1
	appendGranted := 0

	for server := range rf.peers {
		reply := new(AppendEntriesReply)

		go func(server int) {
			<-appendCh
			if rf.sendAppendEntries(server, request, reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if rf.currentTerm != reply.Term || rf.state != Leader {
					return
				}
				appendCh <- 1
				if reply.Success {
					appendGranted += 1
					rf.nextIndex[server] = index + 1
					if appendGranted > len(rf.peers)/2 {
						rf.commitIndex = index
						rf.applyCh <- ApplyMsg{
							CommandValid: true,
							Command:      command,
							CommandIndex: index,

							SnapshotValid: false,
							Snapshot:      []byte{},
							SnapshotTerm:  -1,
							SnapshotIndex: -1,
						}
					}
				} else {
					rf.nextIndex[server] -= 1
				}
			}
		}(server)
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

func (rf *Raft) startElection() {
	request := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: len(rf.logs) - 1,
		LastLogTerm:  rf.logs[len(rf.logs)-1].Term,
	}
	voteGranted := 1
	for server := range rf.peers {
		if server == rf.me {
			continue
		}
		go func(server int) {
			reply := new(RequestVoteReply)
			if rf.sendRequestVote(server, request, reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if rf.state != Candidate || rf.currentTerm != request.Term {
					return
				}
				if reply.VoteGranted {
					voteGranted += 1
					if voteGranted > len(rf.peers)/2 {
						DPrintf("Node[%v] convert to term %v leader after receiving vote from Node[%v]", rf.me, rf.currentTerm, server)
						rf.changeState(Leader)
						rf.broadCastHeartBeat()

					}
				}

			}
		}(server)
	}
}

func (rf *Raft) broadCastHeartBeat() {
	request := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: len(rf.logs) - 1,
		PrevLogTerm:  rf.logs[len(rf.logs)-1].Term,
		Entries:      nil,
		LeaderCommit: rf.commitIndex,
	}

	for server := range rf.peers {
		if server == rf.me {
			continue
		}
		go func(server int) {
			reply := new(AppendEntriesReply)
			if rf.sendAppendEntries(server, request, reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if reply.Success {
					DPrintf("Leader Node[%v] received HB confirm from Node[%v]", rf.me, server)
					return
				} else {
					if reply.Term > rf.currentTerm {
						DPrintf("Node[%v] find a newer leader term %v, step down from term %v", rf.me, reply.Term, rf.currentTerm)
						rf.changeState(Follower)
					}
				}
			}
		}(server)
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		time.Sleep(10 * time.Millisecond)
		rf.mu.Lock()
		switch rf.state {
		case Follower:
			rf.mu.Unlock()
			DPrintf("Node[%v] in EL branch", rf.me)
			rf.mu.Lock()
			if rf.nextElectionTime.Before(time.Now()) {
				rf.changeState(Candidate)
				rf.mu.Unlock()
			} else {
				DPrintf("Node[%v] election timer has not fired", rf.me)
				rf.mu.Unlock()
			}
			time.Sleep(randomizedTimeDuration())
		case Candidate:
			rf.mu.Unlock()
			rf.mu.Lock()
			rf.currentTerm += 1
			rf.votedFor = rf.me
			rf.startElection()
			rf.mu.Unlock()
			time.Sleep(randomizedTimeDuration())
		case Leader:
			rf.mu.Unlock()
			DPrintf("Node[%v] in HB branch", rf.me)
			DPrintf("Node[%v](Leader) start heartbeating", rf.me)
			rf.broadCastHeartBeat()
			time.Sleep(determinedTimeDuration())
		}
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
	rf := &Raft{
		mu:               sync.Mutex{},
		peers:            peers,
		persister:        persister,
		me:               me,
		dead:             0,
		state:            Follower,
		currentTerm:      0,
		votedFor:         -1,
		logs:             make([]Entry, 1),
		commitIndex:      0,
		lastApplied:      0,
		nextIndex:        make([]int, len(peers)),
		matchIndex:       make([]int, len(peers)),
		nextElectionTime: time.Now().Add(randomizedTimeDuration()),
		applyCh:          make(chan ApplyMsg, 1),
	}
	DPrintf("Make")
	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
