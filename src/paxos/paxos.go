package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (Fate, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import (
	"fmt"
	"log"
	"math"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// px.Status() return values, indicating
// whether an agreement has been decided,
// or Paxos has not yet reached agreement,
// or it was agreed but forgotten (i.e. < Min()).
type Fate int

const (
	Decided   Fate = iota + 1
	Pending        // not yet decided.
	Forgotten      // decided but forgotten.
)

type Instance struct {
	fate           Fate
	AcceptedValue  interface{}
	HighestAccept  int64
	HighestPrepare int64
}

type PrepareArguments struct {
	SeqNo      int
	ProposalNo int64
}

type PrepareReply struct {
	Ok         bool
	ProposalNo int64
	Value      interface{}
}

type AcceptArguments struct {
	SeqNo      int
	ProposalNo int64
	Value      interface{}
}

type AcceptReply struct {
	Ok         bool
	ProposalNo int64
}

type DecidedArguments struct {
	SeqNo         int
	Value         interface{}
	Me            int
	DoneSequences int
}

type DecidedReply struct {
	Ok bool
}

const delayBetweenInterval = time.Millisecond * 8
const maxDelayAllowed = 5

type Paxos struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	rpcCount   int32 // for testing
	peers      []string
	me         int // index into peers[]

	// Your data here.
	name             string
	isPersistent     bool
	instances        map[int]*Instance
	concurrencyMutex sync.Mutex
	maximumSeqNo     int
	minimumSeqNo     []int
}

func (ins *Instance) setInstance() {
	// Initialize instance
	ins.fate = Pending
	ins.AcceptedValue = nil
	ins.HighestAccept = 0
	ins.HighestPrepare = 0
}

// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
func call(srv string, name string, args interface{}, reply interface{}) bool {
	c, err := rpc.Dial("unix", srv)
	if err != nil {
		err1 := err.(*net.OpError)
		if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
			fmt.Printf("paxos Dial() failed: %v\n", err1)
		}
		return false
	}
	defer c.Close()

	err = c.Call(name, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
func (px *Paxos) Start(seq int, v interface{}) {
	// Your code here.
	fate, _ := px.Status(seq)
	// if trying to start instance which is already decided then return
	if fate == Forgotten {
		return
	}

	if px.maximumSeqNo < seq {
		px.maximumSeqNo = seq
	} else if px.minimumSeqNo[px.me] > seq {
		log.Fatal("no instances")
		return
	}
	go func() {
		// to be implemented by Ayush
		px.beginConsensusWorkflow(seq, v)
	}()
}

func (px *Paxos) beginConsensusWorkflow(seqNo int, v interface{}) {
	px.concurrencyMutex.Lock()

	px.mu.Lock()
	curr := px.getNodeInfo(seqNo)
	px.mu.Unlock()

	if curr == nil {
		return
	}
	for px.dead == 0 && curr.fate == Pending && seqNo > px.minimumSeqNo[px.me] {
		proposalNum := time.Now().UnixNano()
		proposalNum = int64(math.Abs(float64(proposalNum*int64(len(px.peers)) + int64(px.me))))
		minval := v

		// Prepare phase
		// Send prepare requests with current proposalNum to all peer
		proposedResponse := make(chan PrepareReply)
		sendPrepareRequests(px, proposedResponse, proposalNum, seqNo)
		// collect responses from prepareRequests and check majority
		isMajority, minval := px.ProcessPrepareReply(proposedResponse, v)
		if !isMajority {
			time.Sleep(time.Duration(rand.Int31()%1000) * time.Millisecond)
			continue
		}

		// Accept phase
		// more than majority OK's received from prepare, now send accept requests to all node
		acceptResponses := make(chan AcceptReply)
		sendAccept(px, acceptResponses, proposalNum, seqNo, minval)
		// collect responses from accept requests and check majority
		isMajority = px.ProcessAcceptReply(acceptResponses, proposalNum)
		if !isMajority {
			time.Sleep(time.Duration(rand.Int31()%1000) * time.Millisecond)
			continue
		}

		// as proposalNo accepted by majority, send decide to all nodes
		sendDecide(px, seqNo, minval)

	}

	defer px.concurrencyMutex.Unlock()
}

func sendPrepareRequests(px *Paxos, proposedResponse chan<- PrepareReply, proposalNum int64, seqNo int) {
	i := 0
	for i < len(px.peers) {
		// send prepare to all nodes
		args := PrepareArguments{seqNo, proposalNum}
		reply := PrepareReply{false, -2, nil}
		go func(pid int) {
			if pid == px.me {
				// if proposer is same as peer then call prepare directly
				px.Prepare(&args, &reply)
				proposedResponse <- reply
			} else {
				// else call prepare on peers
				ok := call(px.peers[pid], "Paxos.Prepare", &args, &reply)
				if ok {
					proposedResponse <- reply
				}
			}
		}(i)
		i++
	}
}

func (px *Paxos) ProcessPrepareReply(proposedResponse <-chan PrepareReply, v interface{}) (bool, interface{}) {
	// set delay Num, done response Num, min's value, max proposal no
	delayNum := 0
	done := false
	respNum := 0
	minval := v
	var maxProposalNum int64
	maxProposalNum = 0
	// check if majority of prepare ok's received
	majority := len(px.peers) / 2
	for !done {
		select {
		case reply := <-proposedResponse:
			if reply.Ok {
				// if prepare ok received from majority then set done to true
				respNum++
				if reply.ProposalNo > maxProposalNum && reply.Value != nil {
					maxProposalNum = reply.ProposalNo
					minval = reply.Value
				}
				if respNum > majority {
					done = true
				}
			}
		default:
			// if no response received then wait for some time and then check again
			time.Sleep(delayBetweenInterval)
			delayNum++
			if delayNum >= maxDelayAllowed {
				done = true
			}
		}
	}
	if respNum <= majority {
		return false, nil
	}
	return true, minval

}

func sendAccept(px *Paxos, acceptResponses chan<- AcceptReply, proposalNum int64, seqNo int, minval interface{}) {
	j := 0
	// send accept to all nodes
	for j < len(px.peers) {
		args := AcceptArguments{seqNo, proposalNum, minval}
		var reply AcceptReply
		// if proposer is same as peer then call accept directly
		go func(pid int) {
			if pid == px.me {
				px.Accept(&args, &reply)
			} else {
				call(px.peers[pid], "Paxos.Accept", &args, &reply)
			}
			acceptResponses <- reply
		}(j)

		j++
	}
}

func (px *Paxos) ProcessAcceptReply(acceptResponses <-chan AcceptReply, proposalNum int64) bool {

	respNum := 0
	delayNum := 0
	done := false
	majority := len(px.peers) / 2

	// check if majority of accept ok's received
	for !done {
		select {
		case reply := <-acceptResponses:
			// if accept ok received from majority then set done to true
			if reply.Ok && reply.ProposalNo == proposalNum {
				respNum++
				if respNum > majority {
					done = true
				}
			}
		default:
			// if no response received then sleep for some time and then check again
			time.Sleep(delayBetweenInterval)
			delayNum++
			if delayNum >= maxDelayAllowed {
				done = true
			}
		}
	}
	if respNum <= majority {
		return false
	}
	return true
}

func sendDecide(px *Paxos, seqNo int, minval interface{}) {
	k := 0
	// send decide to all nodes
	for k < len(px.peers) {
		args := DecidedArguments{seqNo, minval, px.me, px.minimumSeqNo[px.me]}
		var reply DecidedReply
		// if proposer is same as peer then call decide directly
		if k == px.me {
			px.Decide(&args, &reply)
		} else {
			// else call decide on peers
			go func(pid int) {
				if pid == px.me {
					px.Decide(&args, &reply)
				} else {
					call(px.peers[pid], "Paxos.Decide", &args, &reply)
				}
			}(k)
		}
		k++
	}
}

func (px *Paxos) getNodeInfo(seqNo int) *Instance {
	if seqNo < px.Min() {
		return nil
	}
	_, containsSeqNo := px.instances[seqNo]
	if containsSeqNo == false {
		px.instances[seqNo] = new(Instance)
		if px.instances[seqNo] == nil {
			log.Fatal("Failure during getNodeInfo")
		}
		px.instances[seqNo].setInstance()
	}
	return px.instances[seqNo]
}

// Prepare will be called in sendPrepareRequests function
func (px *Paxos) Prepare(args *PrepareArguments, response *PrepareReply) error {
	// GetNodeInfo will return the instance from seq number
	px.mu.Lock()

	// get instance from seq no and set highest prepare to proposal no
	ins := px.getNodeInfo(args.SeqNo)
	if ins != nil && args.ProposalNo > ins.HighestPrepare {
		ins.HighestPrepare = args.ProposalNo
		response.Ok = true
		response.ProposalNo = ins.HighestAccept
		response.Value = ins.AcceptedValue

	} else {
		response.Ok = false
	}
	defer px.mu.Unlock()
	return nil
}

func (px *Paxos) Accept(args *AcceptArguments, acceptReply *AcceptReply) error {
	px.mu.Lock()

	// get instance from seq no and set highest prepare to proposal no
	pxInstance := px.getNodeInfo(args.SeqNo)
	if pxInstance != nil && args.ProposalNo >= pxInstance.HighestPrepare {
		// set highest prepare to proposal no
		pxInstance.HighestPrepare = args.ProposalNo
		pxInstance.AcceptedValue = args.Value
		pxInstance.HighestAccept = args.ProposalNo
		acceptReply.Ok = true
		acceptReply.ProposalNo = args.ProposalNo
	} else {
		// if proposal no is less than highest prepare then set ok to false
		acceptReply.Ok = false
		acceptReply.ProposalNo = args.ProposalNo
	}
	defer px.mu.Unlock()
	return nil
}

func (px *Paxos) Decide(args *DecidedArguments, reply *DecidedReply) error {
	px.mu.Lock()

	// get instance from seq no and set fate to decided
	ins := px.getNodeInfo(args.SeqNo)
	if ins != nil {
		ins.fate = Decided
		ins.AcceptedValue = args.Value
	}
	px.minimumSeqNo[args.Me] = args.DoneSequences

	// call forget to free up memory
	px.Forget()

	defer px.mu.Unlock()
	return nil
}

func (px *Paxos) Forget() {
	min := px.Min()
	// forget all instances less than min seq no
	for i, _ := range px.instances {
		if i < min {
			delete(px.instances, i)
		}
	}
}

// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
func (px *Paxos) Done(seq int) {
	// Set min seq no for proposer to seq
	// this value will be used later in Min() function
	px.mu.Lock()
	defer px.mu.Unlock()
	if seq > px.minimumSeqNo[px.me] {
		px.minimumSeqNo[px.me] = seq
		// call forget to free up memory
		px.Forget()
	}

}

// the application wants to know the
// highest instance sequence known to
// this peer.2
func (px *Paxos) Max() int {
	return px.maximumSeqNo
}

// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
func (px *Paxos) Min() int {

	min := math.MaxInt32
	// find min seq no from all proposers
	for i := range px.minimumSeqNo {
		if min > px.minimumSeqNo[i] {
			min = px.minimumSeqNo[i]
		}
	}
	return min + 1
}

// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
func (px *Paxos) Status(seq int) (Fate, interface{}) {
	// Your code here.

	px.mu.Lock()
	defer px.mu.Unlock()

	// if seq is less than min seq no then we marked it as forgotten
	if seq < px.Min() {
		return Forgotten, nil
	}
	ins, ok := px.instances[seq]
	// if seq is not present in instances then it is pending
	if !ok {
		return Pending, nil
	} else {

		return px.instances[seq].fate, ins.AcceptedValue
	}
}

// tell the peer to shut itself down.
// for testing.
// please do not change these two functions.
func (px *Paxos) Kill() {
	atomic.StoreInt32(&px.dead, 1)
	if px.l != nil {
		px.l.Close()
	}
}

// has this peer been asked to shut down?
func (px *Paxos) isdead() bool {
	return atomic.LoadInt32(&px.dead) != 0
}

// please do not change these two functions.
func (px *Paxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&px.unreliable, 1)
	} else {
		atomic.StoreInt32(&px.unreliable, 0)
	}
}

func (px *Paxos) isunreliable() bool {
	return atomic.LoadInt32(&px.unreliable) != 0
}

// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
	px := &Paxos{}
	px.peers = peers
	px.me = me

	// Your initialization code here.
	// Map of instances
	px.instances = make(map[int]*Instance)
	px.maximumSeqNo = 0
	px.minimumSeqNo = make([]int, len(peers))

	// set min seq no to -1 for all proposers
	i := 0
	for i < len(px.minimumSeqNo) {
		px.minimumSeqNo[i] = -1
		i++
	}

	if rpcs != nil {
		// caller will create socket &c
		rpcs.Register(px)
	} else {
		rpcs = rpc.NewServer()
		rpcs.Register(px)

		// prepare to receive connections from clients.
		// change "unix" to "tcp" to use over a network.
		os.Remove(peers[me]) // only needed for "unix"
		l, e := net.Listen("unix", peers[me])
		if e != nil {
			log.Fatal("listen error: ", e)
		}
		px.l = l

		// please do not change any of the following code,
		// or do anything to subvert it.

		// create a thread to accept RPC connections
		go func() {
			for px.isdead() == false {
				conn, err := px.l.Accept()
				if err == nil && px.isdead() == false {
					if px.isunreliable() && (rand.Int63()%1000) < 100 {
						// discard the request.
						conn.Close()
					} else if px.isunreliable() && (rand.Int63()%1000) < 200 {
						// process the request but force discard of reply.
						c1 := conn.(*net.UnixConn)
						f, _ := c1.File()
						err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
						if err != nil {
							fmt.Printf("shutdown: %v\n", err)
						}
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					} else {
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					}
				} else if err == nil {
					conn.Close()
				}
				if err != nil && px.isdead() == false {
					fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
				}
			}
		}()
	}

	return px
}
