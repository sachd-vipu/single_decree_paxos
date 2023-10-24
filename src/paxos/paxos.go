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

import "net"
import "net/rpc"
import "log"

import "os"
import "syscall"
import "sync"
import "sync/atomic"
import "fmt"
import "math/rand"
import "math"
import "time"

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
	acceptedValue  interface{}
	highestAccept  int64
	highestPrepare int64
}

type prepareArguments struct {
	seqNo      int
	proposalNo int64
}

type prepareReply struct {
	ok         bool
	proposalNo int64
	value      interface{}
}

type acceptArguments struct {
	seqNo      int
	proposalNo int64
	value      interface{}
}

type acceptReply struct {
	ok         bool
	proposalNo int64
}

type decidedArguments struct {
	seqNo         int
	value         interface{}
	me            int
	doneSequences int
}

type decidedReply struct {
	ok bool
}

type pollArguments struct {
	seqNo int
}

type pollReply struct {
	ok    bool
	value interface{}
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

type Reply struct {
	Valid int
	ProposalNum int64
	Value interface{}
  }

func (ins *Instance) setInstance() {
	ins.fate = Pending
	ins.acceptedValue = nil
	ins.highestAccept = 0
	ins.highestPrepare = 0
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
	if px.maximumSeqNo < seq {
		px.maximumSeqNo = seq
	} else if px.minimumSeqNo[px.me] < seq {
		log.Fatal("no instances")
		return
	}
	go func() {
		// to be implemented by Ayush
		px.proposeValue(seq, v)
	}()
}

func (px *Paxos) proposeValue(seqNo int, v interface{}) {
	px.concurrencyMutex.Lock()
	defer px.concurrencyMutex.Unlock()

	px.mu.Lock()
	currInstance := px.getNodeInfo(seqNo)
	px.mu.Unlock()

	if currInstance == nil {
		return
	}
	for px.dead == 0 && currInstance.fate == Pending && seqNo > px.minimumSeqNo[px.me] {
		proposalNo := time.Now().UnixNano()
		proposalNo = proposalNo*int64(len(px.peers)) + int64(px.me)

		proposeResponses := make(chan prepareReply)
		peerId := 0
		for peerId < len(px.peers) {
			args := prepareArguments{seqNo, proposalNo}
			reply := prepareReply{false, -2, nil}
			go func(peerId int) {
				if peerId == px.me {
					px.Prepare(&args, &reply)
					proposeResponses <- reply
				} else {
					ok := call(px.peers[peerId], "Paxos.Prepare", &args, &reply)
					if ok {
						proposeResponses <- reply
					}
				}
			}(peerId)
			peerId++
		}

		majority := len(px.peers) / 2
		respNum := 0
		var maxProposalNo int64
		maxProposalNo = 0
		minval := v
		delayNum := 0
		done := false
		for !done {
			reply := <-proposeResponses
			if reply.ok {
				respNum++
				if reply.proposalNo > maxProposalNo && reply.value != nil {
					maxProposalNo = reply.proposalNo
					minval = reply.value
				}
				if respNum > majority {
					done = true
				}
			} else {
				time.Sleep(delayBetweenInterval)
				delayNum++
				if delayNum >= maxDelayAllowed {
					done = true
				}
			}
		}
		if respNum <= len(px.peers)/2 {
			ms := rand.Int31() % 1000
			time.Sleep(time.Duration(ms) * time.Millisecond)
			continue
		}

		acceptResponses := make(chan acceptReply)
		for peerId := 0; peerId < len(px.peers); peerId++ {
			args := acceptArguments{seqNo, proposalNo, minval}
			var reply acceptReply
			go func(peerId int) {
				if peerId == px.me {
					// To be implemented by Vipul
					px.Accept(&args, &reply)
				} else {
					call(px.peers[peerId], "Paxos.Accept", &args, &reply)
				}
				acceptResponses <- reply
			}(peerId)
		}
		respNum = 0
		delayNum = 0
		done = false
		for done == false {
			reply := <-acceptResponses

			if reply.ok {

				if reply.proposalNo == proposalNo {
					respNum++
					if respNum > len(px.peers)/2 {
						done = true
					}
				}
			} else {

				time.Sleep(delayBetweenInterval)
				delayNum++
				if delayNum >= maxDelayAllowed {
					done = true
				}
			}
		}
		majority = len(px.peers) / 2
		if respNum <= majority {
			time.Sleep(time.Duration(rand.Int31()%1000) * time.Millisecond)
			continue
		}
		peerId = 0
		for peerId < len(px.peers) {
			var reply decidedReply
			args := decidedArguments{seqNo, minval, px.me, px.minimumSeqNo[px.me]}
			if peerId == px.me {
				// to be implemented by Ayush
				px.Decide(&args, &reply)
			} else {
				go func(peerId int) {
					if peerId == px.me {
						px.Decide(&args, &reply)
					} else {
						call(px.peers[peerId], "Paxos.Decide", &args, &reply)
					}
				}(peerId)
			}
			peerId++
		}
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

// Prepare will be called in Propose function
func (px *Paxos) Prepare(seq int, proposalNum int64, prepReply *Reply) {
	
		// GetNodeInfo will return the instance from seq number
		pxInstance := px.getNodeInfo(seq)
		if pxInstance != nil && proposalNum > pxInstance.highestPrepare {
			pxInstance.highestPrepare = proposalNum
			prepReply.Valid = 1
			prepReply.ProposalNum = pxInstance.highestAccept
			prepReply.Value = pxInstance.acceptedValue
		} 
		else {
			prepReply.Valid = 0
		}
		return
	}
	


func (px *Paxos) Accept(seq int, proposalNum int64, acceptReply *Reply) {

	pxInstance := px.getNodeInfo(seq)
	if pxInstance != nil && proposalNum >= pxInstance.highestPrepare {
		pxInstance.highestPrepare = proposalNum
		pxInstance.acceptedValue = args.Value
		pxInstance.highestAccept = proposalNum
		acceptReply.Valid = 1
		acceptReply.proposalNum = proposalNum
	} else {
		acceptReply.Valid = 0
		acceptReply.ProposalNum = proposalNum
	}
	return 
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
func (px *Paxos) Done(seq int) {
	// Set min seq no for proposer to seq
	// this value will be used later in Min() function
	px.minimumSeqNo[px.me] = seq

}

// the application wants to know the
// highest instance sequence known to
// this peer.
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
	// let min be some maximum value so it can be compared to the minimum of all the minseq
	min := math.MaxInt32
	i := 0
	// Check minimumSeqNo for all proposers
	for i < len(px.minimumSeqNo) {
		if min > px.minimumSeqNo[i] {
			min = px.minimumSeqNo[i]
		}
		i++
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

	// if seq is less than min, return forgotten
	if seq < px.Min() {
		return Forgotten, nil
	}

	pxInstance, instanceExists := px.instances[seq]
	if !instanceExists {
		return 0, nil
	} else {
		return px.instances[seq].fate, pxInstance.acceptedValue
	}

	return Pending, nil
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

	// set min seq no to 01 for all proposers
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
