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

	commitIndex int
	lastApplied int
	nextIndex   []int // used by leader
	matchIndex  []int // used by leader
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
	if len(rf.logEntries) > 1 { // there is some log entries on the server
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

func (rf *Raft) checkPerv(logIndex int, logTerm int) bool {
	if logIndex == 0 {
		return true
	}
	if len(rf.logEntries) <= logIndex { // current log length less than leader
		return false
	}

	if rf.logEntries[logIndex].Term != logTerm { // second point
		return false
	}

	return true
}

func (rf *Raft) checkAndCopy(logIndex int, newEntries []LogEntry) {
	start := logIndex + 1
	idx := 0
	for i := 0; i < len(newEntries); i++ {
		if len(rf.logEntries) == start+i || rf.logEntries[start+i].Term != newEntries[i].Term {
			idx = i
			break
		}
	}
	DPrintf("Server %d tries to append value from idx %d", rf.me, idx)
	// append all the new entries and overwriting the parts after conflict ones
	rf.logEntries = append(rf.logEntries[:start+idx], newEntries[idx:]...)
	DPrintf("Server %d after append value %v", rf.me, rf.logEntries)
}

func (rf *Raft) checkCommit(leaderCommit int) {
	if leaderCommit > rf.commitIndex {
		if len(rf.logEntries)-1 <= leaderCommit {
			rf.commitIndex = len(rf.logEntries) - 1
		} else {
			rf.commitIndex = leaderCommit
		}
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
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

	if args.Term == rf.currentTerm && rf.state == Candidate {
		rf.convertToFollower(args.Term)
	}

	rf.resetElectionTimer()

	reply.Success = true // default to true, all the logic below would set it to false
	reply.Term = rf.currentTerm

	DPrintf("Server %d, args perv log %d, args perv term %d, my log %v", rf.me, args.PrevLogIndex, args.PrevLogTerm, rf.logEntries)
	pervCheck := rf.checkPerv(args.PrevLogIndex, args.PrevLogTerm)

	if pervCheck == false {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	if len(args.Entries) == 0 {
		rf.checkCommit(args.LeaderCommit)
		return
	}

	rf.checkAndCopy(args.PrevLogIndex, args.Entries)
	rf.checkCommit(args.LeaderCommit)
	return
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
	lastLogIndex := len(rf.logEntries) - 1
	lastLogTerm := rf.logEntries[lastLogIndex].Term
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
		rf.nextIndex = make([]int, len(rf.peers))
		rf.matchIndex = make([]int, len(rf.peers))
		for peer := range rf.peers {
			if peer == rf.me {
				continue
			}
			rf.nextIndex[peer] = len(rf.logEntries)
			rf.matchIndex[peer] = 0
		}
		rf.replicaLog(true)
		return
	}
	rf.mu.Unlock()
	return
}

// replicaLog is the function used by leader to send log to replica
func (rf *Raft) replicaLog(isHeartbeat bool) {
	term := rf.currentTerm
	if isHeartbeat == true {
		rf.heartbeatTime = time.Now() // rest the heartbeatTime
	}
	rf.mu.Unlock()

	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}

		go func(server int, term int, isHeartbeat bool) {
			rf.mu.Lock()
			if rf.state != Leader || rf.currentTerm != term {
				rf.mu.Unlock()
				return
			}

			nextIdx := rf.nextIndex[server]
			lastIdx := len(rf.logEntries) - 1
			if lastIdx < nextIdx && isHeartbeat == false { // in this case, we have nothing to update
				rf.mu.Unlock()
				return
			}

			perLogIdx := nextIdx - 1
			perLogTerm := rf.logEntries[perLogIdx].Term

			entries := rf.logEntries[nextIdx:]
			if isHeartbeat {
				entries = make([]LogEntry, 0)
			}

			args := &AppendEntriesArgs{
				Term:         term,
				LeaderId:     rf.me,
				PrevLogIndex: perLogIdx,
				PrevLogTerm:  perLogTerm,
				Entries:      entries,
				LeaderCommit: rf.commitIndex,
			}

			reply := &AppendEntriesReply{}
			rf.mu.Unlock()

			ok := rf.sendAppendEntries(server, args, reply)

			if ok {
				rf.mu.Lock()
				DPrintf("%d append entries get reply from %d, %t on term %d, is heartbeat? %t", rf.me, server, reply.Success, reply.Term, isHeartbeat)
				if reply.Term > rf.currentTerm { // at this time we need to step down
					rf.convertToFollower(reply.Term)
					rf.mu.Unlock()
					return
				}

				// check if the condition still matches when we schedule the RPC
				if reply.Term == rf.currentTerm && rf.state == Leader {
					if reply.Success == true {
						rf.matchIndex[server] = lastIdx
						rf.nextIndex[server] = lastIdx + 1
					} else {
						// need to do an optimization here
						rf.nextIndex[server] = nextIdx - 1
					}
				}

				rf.mu.Unlock()
				return
			}
		}(peer, term, isHeartbeat)
	}
}

func (rf *Raft) updateCommit() {
	DPrintf("leader %d, current match index %v, current commit index %d", rf.me, rf.matchIndex, rf.commitIndex)
	newCommit := len(rf.logEntries) - 1
	for ; newCommit > rf.commitIndex; newCommit -= 1 {
		commitCount := 1
		for _, match := range rf.matchIndex {
			if match >= newCommit && rf.logEntries[newCommit].Term == rf.currentTerm {
				commitCount += 1
			}
		}
		DPrintf("leader %d, current commit index %d, new commit index %d, commit count %d", rf.me, rf.commitIndex, newCommit, commitCount)
		if commitCount >= (len(rf.matchIndex)/2 + 1) {
			rf.commitIndex = newCommit
			break
		}
	}
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
	index := 0
	term := 0
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	isLeader = rf.state == Leader

	if isLeader == false {
		rf.mu.Unlock()
		return index, term, isLeader
	}

	index = len(rf.logEntries)
	term = rf.currentTerm
	rf.logEntries = append(rf.logEntries, LogEntry{Command: command, Term: term})
	rf.replicaLog(false)

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
	rf.logEntries = make([]LogEntry, 1)
	rf.state = Follower
	rf.electionTime = time.Now()
	rf.electionTimeout = time.Duration(rand.Intn(300)+500) * time.Millisecond
	rf.votedFor = -1
	rf.commitIndex = 0
	rf.lastApplied = 0

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
				rf.replicaLog(true)
			} else {
				rf.mu.Unlock()
				time.Sleep(10 * time.Millisecond)
			}
		}
	}()

	// start the thread to send replica if it is leader
	go func() {
		for {
			rf.mu.Lock()
			if rf.killed() {
				rf.mu.Unlock()
				return
			}
			if rf.state == Leader {
				rf.replicaLog(false)
			} else {
				rf.mu.Unlock()
			}
			time.Sleep(15 * time.Millisecond)
		}
	}()

	// start the thread that commit log
	go func() {
		for {
			rf.mu.Lock()
			if rf.killed() {
				rf.mu.Unlock()
				return
			}
			if rf.state == Leader {
				rf.updateCommit()
			}
			rf.mu.Unlock()
			time.Sleep(50 * time.Millisecond)
		}
	}()

	// apply committed log
	go func(applyCh chan ApplyMsg) {
		for {
			rf.mu.Lock()
			if rf.killed() {
				rf.mu.Unlock()
				return
			}
			if rf.commitIndex > rf.lastApplied {
				DPrintf("Server %d, update last applied from %d, current commit %d, %v", rf.me, rf.lastApplied, rf.commitIndex, rf.logEntries)
				rf.lastApplied++
				idx := rf.lastApplied
				msg := ApplyMsg{
					CommandValid: true,
					Command:      rf.logEntries[idx].Command,
					CommandIndex: idx,
				}
				applyCh <- msg
			}
			rf.mu.Unlock()
			time.Sleep(10 * time.Millisecond)
		}
	}(applyCh)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
