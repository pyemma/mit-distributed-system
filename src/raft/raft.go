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
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"../labrpc"
)

// import "bytes"
// import "../labgob"

// This is a set of status for each raft server as listed in the paper
const (
	Follower  = "Follower"
	Candidate = "Candidate"
	Leader    = "Leader"

	HeartbeatTimeout     = 250 * time.Millisecond
	ElectionTimeOutBase  = 400
	ElectionTimeOutRange = 200
)

// Object of log entry
type LogEntry struct {
	Command interface{}
	Term    int
}

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
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
	currentTerm int
	votedFor    int
	logEntries  []LogEntry

	state           string        // variable to check the current state of the server
	electionTimeout time.Duration // reset upon each heartbeat
	electionTime    time.Time     // timer for election timeout
	heartbeatTime   time.Time     // timer for heartbeat timeout
}

// return currentTerm and whether this server
// believes it is the leader.
// the closure around this function needs to first acquire lock
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

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // candidate's term
	CandidateId  int // candidate's id
	LastLogIndex int // last index of candidate's log
	LastLogTerm  int // term of last log entry in candidate's log
}

type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // current term
	VoteGranted bool // vote the candidate or not
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// Convert to follower
func (rf *Raft) convertToFollower(term int) {
	rf.currentTerm = term
	rf.state = Follower
	rf.votedFor = -1
}

// Reset election timer
func (rf *Raft) resetElectionTimer() {
	rf.electionTime = time.Now()
	rf.electionTimeout = time.Duration(rand.Intn(ElectionTimeOutRange)+ElectionTimeOutBase) * time.Millisecond
}

// Check if the candidate is more up to date
func (rf *Raft) isCandidateUpdateToDate(args *RequestVoteArgs) bool {
	if len(rf.logEntries) > 0 { // there is some log entries on the server
		myLastLogEntry := rf.logEntries[len(rf.logEntries)-1]
		if myLastLogEntry.Term > args.LastLogTerm || (myLastLogEntry.Term == args.LastLogTerm && len(rf.logEntries)-1 > args.LastLogIndex) {
			return false
		}
	}
	return true
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("%d receive request vote from %d on term %d, current term %d", rf.me, args.CandidateId, args.Term, rf.currentTerm)

	// if candidate term is smaller than current term, directly reject
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	// if candidate term is larger than current term, convert to follower first
	if args.Term > rf.currentTerm {
		rf.convertToFollower(args.Term)
	}

	// not vote or the vote is the same as pervious candidate
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		// check if is more up to date
		if rf.isCandidateUpdateToDate(args) == false {
			reply.Term = rf.currentTerm
			reply.VoteGranted = false
			return
		}

		rf.votedFor = args.CandidateId // candidate is more up to date, vote for it
		rf.resetElectionTimer()        // whenever we make a vote, we need to reset the election timeout

		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		return
	}

	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	return
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// This is a heartbeat
	if len(args.Entries) == 0 {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if args.Term < rf.currentTerm {
			reply.Term = rf.currentTerm
			reply.Success = false
			return
		}

		if args.Term > rf.currentTerm {
			rf.convertToFollower(args.Term)
		}

		rf.resetElectionTimer()

		reply.Term = rf.currentTerm
		reply.Success = true
		return
	}
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

func (rf *Raft) startElection() {
	rf.mu.Lock()
	DPrintf("%d start election, pervious state %s", rf.me, rf.state)
	// change to candidate and increase term
	rf.currentTerm++
	rf.state = Candidate
	rf.votedFor = rf.me
	rf.resetElectionTimer() // need to reset election timeout since we start a new election
	// parameters to schedule RequestVote
	term := rf.currentTerm
	lastLogTerm := -1  // placeholder
	lastLogIndex := -1 // placeholder
	rf.mu.Unlock()
	cond := sync.NewCond(&rf.mu)

	count := 1 // counter to check the number of vote
	finished := 1
	// start send RequestVote in parallel
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}

		go func(server int, term int, lastLogTerm int, lastLogIndex int) {
			rf.mu.Lock()
			// server is still candidate and current term matches the term when we plan to request vote
			if rf.state != Candidate || rf.currentTerm != term {
				rf.mu.Unlock()
				return
			}

			DPrintf("%d send request vote to %d on term %d", rf.me, server, rf.currentTerm)
			args := RequestVoteArgs{
				Term:         term,
				CandidateId:  rf.me,
				LastLogTerm:  lastLogTerm,
				LastLogIndex: lastLogIndex,
			}

			reply := RequestVoteReply{}
			rf.mu.Unlock()

			ok := rf.sendRequestVote(server, &args, &reply)

			rf.mu.Lock()
			defer rf.mu.Unlock()
			finished++
			if ok {
				DPrintf("%d got reply from %d on term %d with %t", rf.me, server, rf.currentTerm, reply.VoteGranted)
				if reply.Term > rf.currentTerm {
					rf.convertToFollower(reply.Term)
					return
				}
				// vote is granted, and the term matches the current term, and server is still candidate
				if reply.VoteGranted && reply.Term == rf.currentTerm && rf.state == Candidate {
					count++
				}
			}
			cond.Broadcast()

		}(peer, term, lastLogTerm, lastLogIndex)
	}

	rf.mu.Lock()
	// wait for enough vote or all request has returned
	n := len(rf.peers)
	majority := (n / 2) + 1
	for count < majority && finished != n {
		cond.Wait()
	}

	if count >= majority {
		DPrintf("%d collect majority of votes", rf.me)
		// change to leader and send the initial batch of heartbeats
		rf.state = Leader
		rf.sendHeartbeat()
		return
	}
	rf.mu.Unlock()
	return
}

func (rf *Raft) sendHeartbeat() {
	term := rf.currentTerm
	rf.heartbeatTime = time.Now() // reset the heartbeatTime
	rf.mu.Unlock()

	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}

		go func(server int, term int) {
			rf.mu.Lock()
			// server is still Leader and current term matches the term when we schedule heartbeats
			if rf.state != Leader || rf.currentTerm != term {
				rf.mu.Unlock()
				return
			}

			args := &AppendEntriesArgs{
				Term:     term,
				LeaderId: rf.me,
				Entries:  make([]LogEntry, 0),
			}

			reply := &AppendEntriesReply{}
			rf.mu.Unlock()

			ok := rf.sendAppendEntries(server, args, reply)

			if ok {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				DPrintf("%d successful send heartbeat to %d on term %d, reply %d", rf.me, server, term, reply.Term)
				if reply.Term > rf.currentTerm {
					rf.convertToFollower(reply.Term)
				}
			}
			return
		}(peer, term)
	}
	return
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
	DPrintf("%d is killed on the current term %d", rf.me, rf.currentTerm)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
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

	// init server to the default status
	rf.state = Follower
	rf.electionTime = time.Now()
	rf.electionTimeout = time.Duration(rand.Intn(300)+500) * time.Millisecond
	rf.votedFor = -1

	// start the background thread to check election timeout
	go func() {
		for {
			rf.mu.Lock()
			if rf.killed() {
				rf.mu.Unlock()
				return
			}
			// follower or candidate could start election upon election timeout
			if rf.state != Leader && time.Now().Sub(rf.electionTime) >= rf.electionTimeout {
				rf.mu.Unlock()
				rf.startElection()
			} else {
				rf.mu.Unlock()
				time.Sleep(25 * time.Millisecond)
			}
		}
	}()

	// start the thread to send heartbeat if is leader
	go func() {
		for {
			rf.mu.Lock()
			if rf.killed() {
				rf.mu.Unlock()
				return
			}
			if rf.state == Leader && time.Now().Sub(rf.heartbeatTime) >= HeartbeatTimeout {
				rf.sendHeartbeat()
			} else {
				rf.mu.Unlock()
				time.Sleep(10 * time.Millisecond)
			}
		}
	}()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
