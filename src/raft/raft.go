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
	"fmt"
	"labgob"
	"math/rand"
	"sync"
	"time"
)
import "labrpc"

const heartbeatInterval = 110
const electionTimeoutMin = 200
const electionTimeoutMax = 400

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

// An entry in log[]
type Log struct {
	command interface{}
	term 	int
}

const (
	leader = iota
	follower
	candidate
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
    state 		int
	receivedHeartbeat bool

	// Persistent state
	currentTerm	int
	votedFor 	int
	log 		[]Log

	// Volatile state
	commitIndex	int
	lastApplied	int

	// Volatile state on leaders
	nextIndex	[]int
	matchIndex	[]int
}

func (rf *Raft) IsLeader() bool {
	return rf.state == leader
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	return rf.currentTerm, rf.IsLeader()
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
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	// votedFor: candidateId that received vote in current term (or null if none)
	//e.Encode(rf.votedFor)
	// log entries; each entry contains command for state machine,
		// and term when entry was received by leader (first index is 1)
	//e.Encode(rf.logEntries)
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
}




//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int
	CandidateID int
	LastLogIndex int
	LastLogTerm int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	reply.Term = rf.currentTerm

	// Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		return
	}

	// If votedFor is null or candidateId, and candidate’s log is at
	// least as up-to-date as receiver’s log, grant vote

	// Raft determines which of two logs is more up-to-date by comparing
	// the index and term of the last entries in the logs. If the logs
	// have last entries with different terms, then the log with the later
	// term is more up-to-date. If the logs end with the same term, then
	// whichever log is longer is more up-to-date.
	isUpToDate := args.LastLogTerm > rf.currentTerm ||
		(args.LastLogTerm == rf.currentTerm && args.LastLogIndex >= rf.lastApplied)
	reply.VoteGranted = (rf.votedFor == -1 || rf.votedFor == args.CandidateID) && isUpToDate
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
	// Your code here (2B).


	return index, rf.currentTerm, rf.IsLeader()
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

func (rf *Raft) AppendEntries() {
	rf.receivedHeartbeat = true
	// TODO: process arguments...
}

func (rf *Raft) heartbeat() {
	for {
		time.Sleep(time.Duration(heartbeatInterval) * time.Microsecond)
		for idx, peer := range rf.peers {
			if idx != rf.me {
				ok := peer.Call("Raft.AppendEntries", nil, nil)
				if !ok {
					_ = fmt.Errorf("heartbeat")
				}
			}
		}
	}
}

func (rf *Raft) electLeader() {
	s1 := rand.NewSource(time.Now().UnixNano())
	r1 := rand.New(s1)
	for {
		time.Sleep(time.Duration(r1.Intn(electionTimeoutMax-electionTimeoutMin)+electionTimeoutMin) * time.Microsecond)
		if !rf.IsLeader() && !rf.receivedHeartbeat {
			// Begin an election
			rf.mu.Lock()
			rf.currentTerm += 1
			rf.state = candidate
			// votes for itself
			rf.votedFor = rf.me

			nVotes := 1
			var wg sync.WaitGroup
			wg.Add(len(rf.peers))
			mu := sync.Mutex{}
			for idx, peer := range rf.peers {
				if idx != rf.me {
					// issues RequestVote RPCs in parallel to each of the other servers in the cluster
					go func() {
						defer wg.Done()
						args := &RequestVoteArgs{}
						args.CandidateID = rf.me
						args.LastLogIndex = rf.commitIndex
						args.LastLogTerm = rf.currentTerm - 1
						reply := &RequestVoteReply{}
						ok := peer.Call("Raft.RequestVote", args, reply)
						if !ok {
							_ = fmt.Errorf("begin an election. Me: %d; lastTerm: %d", rf.me, args.LastLogTerm)
						} else {
							// Increment count
							mu.Lock() // lock nVotes
							if reply.Term == args.Term {
								if reply.VoteGranted {
									nVotes += 1
								}
							} else if reply.Term > args.Term {
								// Not specified in the paper, but I assume this is equivalent to seeing a new leader?
								fmt.Println("Not specified in the paper, but I assume this is equivalent to seeing a new leader?")
							}
							mu.Unlock()
						}
					}()
				}
			}
			rf.mu.Unlock()
			wg.Wait()
			// Count votes
			rf.mu.Lock()
			if rf.state == candidate { // if didn't receive an AppendEntries RPC from a legitimate leader
				if nVotes > len(rf.peers)/2 {
					rf.state = leader // promote to leader
					go rf.heartbeat()
				}

				// TODO: reset state and double check these conditions
				rf.receivedHeartbeat = false
				//for i:= range rf.nextIndex {
				//rf.nextIndex[i] = len(rf.log)
				//}
				//for i := range rf.matchIndex {
				//rf.matchIndex[i]

				//}
			}
			rf.mu.Unlock()
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
	rf.state = follower

	// Your initialization code here (2A, 2B, 2C).
	// create a background goroutine that will kick off leader election
	// periodically by sending out RequestVote RPCs when it hasn't heard
	// from another peer for a while.
	go rf.electLeader()

	// TODO: heartbeat goroutine


	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())


	return rf
}
