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
	"time"
)
import "labrpc"

// import "bytes"
// import "encoding/gob"

const heart_beat_ms = 100
const election_timeout_ms = 300

const (
	follower int = iota
	candidate
	leader
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

//LogEntry是AppendEntriesArgs的成员，因此需要大写首字母
type LogEntry struct {
	Command interface{}
	Term int
}

type ChangeToFollower struct {
	term int
	votedFor int
	shouldResetTimer bool
}

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
	//2A
	CurrentTerm int		//节点已知的最后一个任期号（首次启动后从 0 开始递增）
	VotedFor int		//当前任期内获得选票的候选人id（没有则为 null）
	role int			//角色id
	changeToFollower chan ChangeToFollower
	changeToFollowerDone chan bool
	receivedQuit chan bool
	quitCheckRoutine chan bool

	timeout *time.Timer
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.CurrentTerm
	role := rf.role
	isleader = (role == leader)
	DPrintf("GetState me:%d term:%d votedFor:%d isleader:%v", rf.me, term, rf.VotedFor, isleader)
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
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
}




//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	//2A
	Term int
	CandidateId int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	//2A
	Term int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	//响应投票
	DPrintf("[RequestVote] me:%v currentTerm:%v args:%v", rf.me, rf.CurrentTerm, args)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.CurrentTerm
	reply.VoteGranted = false

	if rf.CurrentTerm > args.Term {
		return
	}

	if rf.CurrentTerm < args.Term {
		reply.Term = args.Term
		reply.VoteGranted = true
		var changeToFollower ChangeToFollower = ChangeToFollower{args.Term ,args.CandidateId, reply.VoteGranted}
		DPrintf("[RequestVote] me:%d changeToFollower:%v", rf.me, changeToFollower)
		rf.PushChangeToFollower(changeToFollower)
	}
}

func (rf *Raft) PushChangeToFollower(changeToFollower ChangeToFollower) {
	rf.changeToFollower <- changeToFollower
	<- rf.changeToFollowerDone
}

//AppendEntries
type AppendEntriesArgs struct {
	//2A
	Term int
	LeaderId int
}

type AppendEntriesReply struct {
	//2A
	Term int
	Success bool
}

//响应心跳
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("[AppendEntries] me:%d currentTerm:%v received AppendEntriesArgs:%v", rf.me, rf.CurrentTerm, args)

	if rf.CurrentTerm > args.Term {
		reply.Term = rf.CurrentTerm
		reply.Success = false
	}else {
		reply.Term = args.Term
		reply.Success = true
	}
	DPrintf("[AppendEntries] me:%d currentTerm:%d votedFor:%d", rf.me, rf.CurrentTerm, rf.VotedFor)
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
	// DPrintf("[sendAppendEntries] to server:%d request:%v", server, args)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}


//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
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
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	DPrintf("[Kill] me:%d", rf.me)
	//加锁避免AppendEntries线程里写了ApplyMsg并返回response，但是未来得及持久化
	//该线程Kill然后Make
	rf.mu.Lock()
	close(rf.receivedQuit)
	close(rf.quitCheckRoutine)
	rf.mu.Unlock()
	DPrintf("[Kill] me:%d return", rf.me)
}

func (rf *Raft) ElectionTimeout() int {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	return r.Intn(election_timeout_ms) + election_timeout_ms
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
	//2A
	rf.changeToFollower = make(chan  ChangeToFollower)
	rf.changeToFollowerDone = make(chan bool)
	rf.receivedQuit = make(chan bool)
	rf.quitCheckRoutine = make(chan bool)
	rf.CurrentTerm = 0
	rf.VotedFor = -1

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.timeout = time.NewTimer(time.Duration(rf.ElectionTimeout()) * time.Millisecond)

	//启动服务后，节点身份状态变为跟随者
	go rf.BeFollower()
	DPrintf("[Make] me:%d return", rf.me)
	return rf
}

func (rf* Raft) BeFollower() {
	DPrintf("[BeFollower] me:%d before for loop", rf.me)
	rf.role = follower

	for {
		DPrintf("[BeFollower] me:%d begin wait select", rf.me)
		select {
		case v := <- rf.changeToFollower :
			DPrintf("[BeFollower] me:%d CurrentTerm:%v changeToFollower:%v", rf.me, rf.CurrentTerm, v)
			if v.term > rf.CurrentTerm {
				go rf.TransitionToFollower(v)
				return
			}
			rf.changeToFollowerDone <- true
			if v.shouldResetTimer {
				rf.timeout.Reset(time.Duration(rf.ElectionTimeout()) * time.Millisecond)
			}
		case <- rf.timeout.C:
			//If a follower receives no communication over a period of time called the election timeout,
			//then it assumes thers is no viable leader and begins an election to choose a new leader.
			DPrintf("[BeFollower] me:%d timeout", rf.me)
			go rf.BeCandidate()
			return
		case <- rf.receivedQuit:
			DPrintf("[BeFollower] me:%d quit", rf.me)
			return
		}
	}
}

func (rf *Raft) TransitionToFollower(changeToFollower ChangeToFollower) {
	DPrintf("[TransitionToFollower] me:%d changeToFollower:%v CurrentTerm:%d role:%v", rf.me, changeToFollower, rf.CurrentTerm, rf.role)
	rf.role = follower
	if rf.CurrentTerm < changeToFollower.term {
		rf.CurrentTerm = changeToFollower.term
	}
	if changeToFollower.votedFor != -1 {
		rf.VotedFor = changeToFollower.votedFor
	}
	//为什么重置nextIndex?
	//避免本身是旧leader，makeAppendEntryRequest还在使用nextIndex + rf.Logs构造AppendEntriesArgs
	//而如果rf.Logs被新的leader截断，那么可能出现nextIndex > len(rf.Logs)情况，导致makeAppendEntryRequest里index out of range
	//为什么重置nextIndex为0?
	//注意nextIndex不能设置为len(logs)，比如以下场景：
	//1. 发送AppendEntries时response，转化为follower，此时rf.Logs未修改，nextIndex = len(rf.Logs)
	//2. 接着收到新leader的AppendEntries，可能删减rf.Logs
	//3. BeFollower里的follower的case v:= <- changeToFollower触发，调用go rf.TransitionToFollower(v)后返回，释放AppendEntries函数里的锁
	//4. makeAppendEntryRequest使用删减后的rf.Logs 与 未修改的nextIndex，可能出错
	//5. go rf.TransitionToFollower(v)异步运行到这里，才设置nextIndex = len(rf.Logs)
	//rf.InitNextIndex()
	rf.persist()
	rf.changeToFollowerDone <- true

	if changeToFollower.shouldResetTimer {
		rf.timeout.Reset(time.Duration(rf.ElectionTimeout()) * time.Millisecond)
	}

	rf.BeFollower()
}

func (rf *Raft) BeCandidate() {
	DPrintf("[BeCandidate] me:%v begin.", rf.me)
	rf.role = candidate
	for {
		vote_ended := make(chan bool, len(rf.peers))
		go rf.StartElection(vote_ended)
		rf.timeout.Reset(time.Duration(rf.ElectionTimeout()) * time.Millisecond)

		select {
		case v := <- rf.changeToFollower:
			//If AppendEntries RPC received from new leader:convert to follower
			DPrintf("[BeCandidate] me:%d changeToFollower:%v", rf.me, v)
			go rf.TransitionToFollower(v)
			return
		case <- rf.receivedQuit:
			DPrintf("[BeCandidate] me:%d quit", rf.me)
			return
		case win := <- vote_ended:
			DPrintf("[BeCandidate] me:%d CurrentTerm:%v win:%v", rf.me, rf.CurrentTerm, win)
			//If vote received from majority of servers:become leader
			if win {
				go rf.BeLeader()
				return
			}
		case <- rf.timeout.C:
			//If election timeout elapses:start new election
			DPrintf("[BeCandidate] election timeout, start new election. me:%v CurrentTerm:%v", rf.me, rf.CurrentTerm)
		}
	}
}

func (rf *Raft) BeLeader() {
	//异步避免 AE/RV里get lock后尝试push channel
	//而这里尝试getlock后才pop channel
	go func() {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if rf.role != candidate {
			return
		}
		//放在最后一步，在rf.SendLogEntryMessageToAll前判断是否是leader角色
		rf.role = leader
	}()

	for {
		select {
		case v := <- rf.changeToFollower:
			//turn to follower
			DPrintf("[BeLeader] me:%d changeToFollower:%v", rf.me, v)
			go rf.TransitionToFollower(v)
			return
		case <- rf.receivedQuit:
			DPrintf("[BeLeader] me:%d quit", rf.me)
			return
		default:
			DPrintf("[BeLeader] me:%d default. rf.role:%v", rf.me, rf.role)
			//等待直到leader状态初始化完成
			if rf.role == leader {
				//Upon election: send initial empty AppendEntries RPCs(heartbeat) to each server;(这里我发送的可能不是empty)
				//repeat during idle periods to prevent election timeouts.
				//rf.SendLogEntryMessageToAll()
				//Hint: The tester requires that the leader send heartbeat RPCs no more than then times persecond
				time.Sleep(heart_beat_ms * time.Millisecond)
			}
		}
	}
}

func (rf *Raft) StartElection(win chan bool) {
	//On conversion to candidate, start election:
	//1. Increment currentTerm
	//2. Vote for self
	//3. Reset election timer
	//4. Send RequestVote RPCs to all other servers
	server_count := len(rf.peers)
	voted := make([]bool, server_count)

	rf.mu.Lock()

	//如果拿到锁后发现已经不是candidate，不再发起选举。
	if rf.role != candidate {
		rf.mu.Unlock()
		return
	}

	rf.CurrentTerm++
	rf.VotedFor = rf.me
	rf.persist()
	voted[rf.me] = true

	var request RequestVoteArgs
	request.Term = rf.CurrentTerm
	request.CandidateId = rf.me
	rf.mu.Unlock()

	//send request vote RPCs to all other servers
	for i := 0; i < server_count; i++ {
		if i != rf.me {
			//issues RequestVote RPCs in parallel to each of the other servers in the cluster
			go func(server_index int, voted []bool) {
				var reply RequestVoteReply
				DPrintf("[StartElection] sendRequestVote from me:%d to server_index:%d request:%v", rf.me, server_index, request)
				send_ok := rf.sendRequestVote(server_index, &request, &reply)
				DPrintf("[StartElection] sendRequestVote from me:%d to server_index:%d currentTerm:%d send_ok:%v reply:%v", rf.me, server_index, rf.CurrentTerm, send_ok, reply)

				voted[server_index] = send_ok && reply.VoteGranted

				if CheckIfWinHalfVote(voted, server_count) {
					//DPrintf("[StartElection] rf.me:%d voted:%v rf.Logs:%v rf.CurrentTerm:%v request:%v", rf.me, voted, rf.Logs, rf.CurrentTerm, request)
					win <- true
				}
			}(i, voted)
		}
	}
}

func CheckIfWinHalfVote(voted []bool, server_count int) bool {
	voted_count := 0
	for  i := 0; i < server_count; i++ {
		if voted[i] {
			voted_count++
		}
	}
	//win the vote if iut receives votes from a majority of the servers in the ful cluster for the same term.
	return voted_count >= (server_count/2 + 1)
}
