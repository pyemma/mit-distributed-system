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
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
)

// import "bytes"
// import "../labgob"

// This is a set of status for each raft server as listed in the paper
const (
	Follower  = "Follower"
	Candidate = "Candidate"
	Leader    = "Leader"

	HeartbeatTimeout     = 120 * time.Millisecond
	ElectionTimeOutBase  = 360
	ElectionTimeOutRange = 240
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

	state     string // variable to check the current state of the server
	voteCount int    // number of vote collected

	commitIndex int
	lastApplied int
	nextIndex   []int // used by leader
	matchIndex  []int // used by leader

	// use channel to send signals
	applyCh        chan ApplyMsg
	votedCh        chan bool
	winCh          chan bool
	heartBeatCh    chan bool
	backToFollower chan bool
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logEntries)
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var logEntries []LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logEntries) != nil {
		DPrintf("Fail to read state")
	} else {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.logEntries = logEntries
	}
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
	XTerm   int
	XIndex  int
	XLen    int
}

// Convert to follower
func (rf *Raft) convertToFollower(term int) {
	rf.currentTerm = term
	rf.state = Follower
	rf.votedFor = -1
	rf.persist()
	rf.sendSignal(rf.backToFollower, true)
}

func (rf *Raft) getElectionTimeout() time.Duration {
	return time.Duration(ElectionTimeOutBase + rand.Intn(ElectionTimeOutRange))
}

// Check if the candidate is more up to date
func (rf *Raft) isCandidateUpdateToDate(args *RequestVoteArgs) bool {
	if len(rf.logEntries) > 1 { // there is some log entries on the server
		DPrintf("My last term %d, my last idx %d, request last term %d, request last idx %d",
			rf.getLastEntryTerm(), rf.getLastEntryIndex(), args.LastLogTerm, args.LastLogIndex)
		if rf.getLastEntryTerm() > args.LastLogTerm || (rf.getLastEntryTerm() == args.LastLogTerm && rf.getLastEntryIndex() > args.LastLogIndex) {
			return false
		}
	}
	return true
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
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
	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	// not vote or the vote is the same as pervious candidate, and candidate is up-to-date
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && rf.isCandidateUpdateToDate(args) {
		rf.votedFor = args.CandidateId // candidate is more up to date, vote for it
		reply.VoteGranted = true
		rf.sendSignal(rf.votedCh, true)
	}
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

func (rf *Raft) updateXInfo(logIndex int, logTerm int, reply *AppendEntriesReply) {
	// case 3, follower too short
	if len(rf.logEntries) <= logIndex {
		reply.XTerm = -1
		reply.XIndex = -1
		reply.XLen = len(rf.logEntries)
		return
	}

	// case 1 and case 2
	if rf.logEntries[logIndex].Term != logTerm {
		reply.XTerm = rf.logEntries[logIndex].Term
		idx := logIndex
		for ; idx >= 0; idx-- {
			if rf.logEntries[idx].Term != reply.XTerm {
				break
			} else {
				reply.XIndex = idx
			}
		}
		// for idx, entry := range rf.logEntries {
		// 	if entry.Term == reply.XTerm {
		// 		reply.XIndex = idx
		// 		break
		// 	}
		// }
		reply.XLen = len(rf.logEntries)
		return
	}
}

func (rf *Raft) checkAndCopy(logIndex int, newEntries []LogEntry) {

	i, j := logIndex+1, 0
	for ; i < len(rf.logEntries) && j < len(newEntries); i, j = i+1, j+1 {
		if rf.logEntries[i].Term != newEntries[j].Term {
			break
		}
	}
	DPrintf("Server %d tries to append value from idx %d, new log %v", rf.me, i, newEntries[j:])
	rf.logEntries = append(rf.logEntries[:i], newEntries[j:]...)
	rf.persist()
	// DPrintf("Server %d after append value %v", rf.me, rf.logEntries)
}

func (rf *Raft) checkCommit(leaderCommit int) {
	if leaderCommit > rf.commitIndex {
		if rf.getLastEntryIndex() <= leaderCommit {
			rf.commitIndex = rf.getLastEntryIndex()
		} else {
			rf.commitIndex = leaderCommit
		}
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("I %d receive append entries prev log %d, perv term %d", rf.me, args.PrevLogIndex, args.PrevLogTerm)

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.XIndex = -1
		reply.XTerm = -1
		return
	}

	if args.Term > rf.currentTerm {
		rf.convertToFollower(args.Term)
	}

	if args.Term == rf.currentTerm && rf.state == Candidate {
		rf.convertToFollower(args.Term)
	}

	rf.sendSignal(rf.heartBeatCh, true)

	reply.Success = false
	reply.Term = rf.currentTerm

	// check the term in the PrevLogIdx
	// case 3, follower too short
	if args.PrevLogIndex > rf.getLastEntryIndex() {
		reply.XTerm, reply.XIndex, reply.XLen = -1, -1, rf.getLastEntryIndex()+1
		return
	}

	// case 1 and case 2, reply the term in its own log, and update to the first index pos
	if rf.logEntries[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.XTerm = rf.logEntries[args.PrevLogIndex].Term
		idx := args.PrevLogIndex
		for ; idx >= 0; idx-- {
			if rf.logEntries[idx].Term != reply.XTerm {
				break
			} else {
				reply.XIndex = idx
			}
		}
		reply.XLen = rf.getLastEntryIndex() + 1
		return
	}

	i, j := args.PrevLogIndex+1, 0
	for ; i < len(rf.logEntries) && j < len(args.Entries); i, j = i+1, j+1 {
		if rf.logEntries[i].Term != args.Entries[j].Term {
			break
		}
	}
	DPrintf("Server %d tries to append value from idx %d, new log %v", rf.me, i, args.Entries[j:])
	rf.logEntries = append(rf.logEntries[:i], args.Entries[j:]...)
	rf.persist()
	DPrintf("Server %d after append value %v", rf.me, rf.logEntries)

	reply.Success = true

	// rf.checkAndCopy(args.PrevLogIndex, args.Entries)
	rf.checkCommit(args.LeaderCommit)
	go rf.applyLogs()
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

func (rf *Raft) sendAppendEntriesV2(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)

	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term != rf.currentTerm || rf.state != Leader {
		return
	}

	if reply.Term > rf.currentTerm { // at this time we need to step down
		rf.convertToFollower(reply.Term)
		return
	}

	if reply.Success {
		matchIndexNew := args.PrevLogIndex + len(args.Entries)
		if matchIndexNew > rf.matchIndex[server] {
			rf.matchIndex[server] = matchIndexNew
		}
		rf.nextIndex[server] = rf.matchIndex[server] + 1
	} else {
		rf.nextIndex[server] = rf.updateNextIdx(reply)
		rf.matchIndex[server] = rf.nextIndex[server] - 1
	}

	rf.updateCommit()
	go rf.applyLogs()
	return
}

func (rf *Raft) broadcastAppendEntries() {
	if rf.state != Leader {
		return
	}

	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}

		perLogIdx := rf.nextIndex[peer] - 1
		perLogTerm := rf.logEntries[perLogIdx].Term
		entries := rf.logEntries[rf.nextIndex[peer]:]

		args := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: perLogIdx,
			PrevLogTerm:  perLogTerm,
			Entries:      entries,
			LeaderCommit: rf.commitIndex,
		}

		go rf.sendAppendEntriesV2(peer, &args, &AppendEntriesReply{})
	}
}

func (rf *Raft) sendRequestVoteV2(server int, args *RequestVoteArgs, reply *RequestVoteReply) {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)

	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// some explanation here, the key thing we would like to make sure is that, upon
	// we receive response from peers, the situation is still the same with the time
	// we are sending out the request, which means that we are still candidate, and
	// the term in request is still the current term, and the term in reply is responding
	// to our current term.
	// suppose the following situation
	//		candidate send vote request on term 1, vote timeout, start new vote on term 2
	//		if on term 1, follower 1 vote for candidate and we take that vote, then it is
	//		is a violation of the leader election process
	if rf.state != Candidate || reply.Term < rf.currentTerm || args.Term != rf.currentTerm {
		return
	}

	if reply.Term > rf.currentTerm {
		rf.convertToFollower(reply.Term)
		return
	}

	if reply.VoteGranted && reply.Term == rf.currentTerm {
		DPrintf("%d receive vote on term %d", rf.me, rf.currentTerm)
		rf.voteCount++
		if rf.voteCount >= len(rf.peers)/2+1 {
			rf.sendSignal(rf.winCh, true)
		}
	}
}

func (rf *Raft) broadcastRequestVote() {
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogTerm:  rf.getLastEntryTerm(),
		LastLogIndex: rf.getLastEntryIndex(),
	}

	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}

		go rf.sendRequestVoteV2(peer, &args, &RequestVoteReply{})
	}
}

// reset the channels once we have already change the state
// this means that we would drop all singals in our old state
// and only accept signals on the new channels
func (rf *Raft) resetChannels() {
	rf.winCh = make(chan bool)
	rf.votedCh = make(chan bool)
	rf.backToFollower = make(chan bool)
	rf.heartBeatCh = make(chan bool)
}

func (rf *Raft) sendSignal(ch chan bool, v bool) {
	select {
	case ch <- v:
	default:
	}
}

func (rf *Raft) convertToCandidate(fromState string) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// check during before and after the invoke, if
	// the state of the node has changed or not
	// invariant checking principle
	if rf.state != fromState {
		return
	}
	DPrintf("%d convert to Candidate on term %d", rf.me, rf.currentTerm)

	rf.resetChannels()

	rf.currentTerm++
	rf.state = Candidate
	rf.votedFor = rf.me
	rf.voteCount = 1
	rf.persist()

	rf.broadcastRequestVote()
}

func (rf *Raft) convertToLeader() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// check during before and after the invoke, if
	// the state of the node has changed or not
	// invariant checking principle
	if rf.state != Candidate {
		return
	}
	DPrintf("%d convert to Leader on term %d", rf.me, rf.currentTerm)

	rf.resetChannels()

	rf.state = Leader
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		rf.nextIndex[peer] = rf.getLastEntryIndex() + 1
	}

	rf.broadcastAppendEntries()
}

func (rf *Raft) getLastEntryIndex() int {
	return len(rf.logEntries) - 1
}

func (rf *Raft) getLastEntryTerm() int {
	return rf.logEntries[rf.getLastEntryIndex()].Term
}

func (rf *Raft) updateNextIdx(reply *AppendEntriesReply) int {
	idxMap := make(map[int]int)
	for idx, entry := range rf.logEntries {
		idxMap[entry.Term] = idx
	}
	// case 3, follower length too short
	if reply.XTerm == -1 {
		return reply.XLen
	}
	val, ok := idxMap[reply.XTerm]
	// case 1, leader has no conflict term
	if !ok {
		return reply.XIndex
	}
	// case 2, leader has conflict term, set to last entry
	return val
}

func (rf *Raft) updateCommit() {
	DPrintf("leader %d, current match index %v, current commit index %d, current log lenght %d", rf.me, rf.matchIndex, rf.commitIndex, len(rf.logEntries))

	for newCommit := rf.getLastEntryIndex(); newCommit >= rf.commitIndex; newCommit-- {
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
	defer rf.mu.Unlock()
	isLeader = rf.state == Leader

	if isLeader == false {
		return index, term, isLeader
	}

	index = len(rf.logEntries)
	term = rf.currentTerm
	rf.logEntries = append(rf.logEntries, LogEntry{Command: command, Term: term})
	rf.persist()

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

func (rf *Raft) applyLogs() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		msg := ApplyMsg{
			CommandValid: true,
			Command:      rf.logEntries[i].Command,
			CommandIndex: i,
		}
		rf.applyCh <- msg
		rf.lastApplied = i
	}
}

func (rf *Raft) startBackgroundEvent() {
	for {
		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()

		switch state {
		case Leader:
			select {
			case <-rf.backToFollower:
			case <-time.After(HeartbeatTimeout):
				DPrintf("I %d sending heartbeat signal", rf.me)
				DPrintf("I leader %d current log, %v", rf.me, rf.logEntries)
				rf.mu.Lock()
				rf.broadcastAppendEntries()
				rf.mu.Unlock()
			}
		case Follower:
			select {
			case <-rf.votedCh:
				DPrintf("I %d voted for %d on term %d", rf.me, rf.votedFor, rf.currentTerm)
			case <-rf.heartBeatCh:
				DPrintf("I %d receive heart beat", rf.me)
			case <-time.After(rf.getElectionTimeout() * time.Millisecond):
				rf.convertToCandidate(state)
			}
		case Candidate:
			select {
			case <-rf.backToFollower:
			case <-rf.winCh:
				rf.convertToLeader()
			case <-time.After(rf.getElectionTimeout() * time.Millisecond):
				rf.convertToCandidate(state)
			}
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
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).

	// init server to the default status
	rf.applyCh = applyCh
	rf.logEntries = make([]LogEntry, 1)
	rf.state = Follower
	rf.votedFor = -1
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.voteCount = 0
	rf.votedCh = make(chan bool)
	rf.heartBeatCh = make(chan bool)
	rf.winCh = make(chan bool)
	rf.backToFollower = make(chan bool)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.startBackgroundEvent()

	return rf
}
