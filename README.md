[![Review Assignment Due Date](https://classroom.github.com/assets/deadline-readme-button-24ddc0f5d75046c5622901739e7c5dd533143b0c8e959d652212380cedb1ea36.svg)](https://classroom.github.com/a/fmem5k7Z)
<br />Name 1: Vipul Sachdeva
<br />Name 2: Ayush Tanwar

## Submission
You only need to push your code to the github repo. No further action 
needed on Canvas. Please make sure you push your latest changes before the 
cutoff time. After the deadline, you are not able to push any commits.

## Project Overview
In this assignment, you'll implement Paxos, specifically single-decree paxos.
You just need to fill in the necessary parts in `src/paxos/paxos.go` to make 
sure Paxos works correctly. The communication among peers is based on rpc, 
which is also provided in the template (`call()`).

**Please read all the comments in paxos.go carefully, and please DO NOT change
any default skeleton codes**

You are free to add your own members in the `Paxos` struct and your own
functions.

You must implement the interfaces listed below
```
px = paxos.Make(peers []string, me int) // constructor
px.Start(seq int, v interface{}) // start agreement on new instance
px.Status(seq int) (fate Fate, v interface{}) // get info about an instance
px.Done(seq int) // ok to forget all instances <= seq
px.Max() int // highest instance seq known, or -1
px.Min() int // instances before this have been forgotten
```

An application calls `Make(peers,me)` to create a Paxos peer, which will be 
used in your next assignment. You don't need to implement the part that 
calls `Make(peers, me)` in this assignment. The `peers` argument contains the 
ports of all the peers (including this one), and the `me` argument is the 
index of this peer in the peers array. 

`Start(seq,v)` asks Paxos to start agreement on instance seq, with proposed
value v; `Start()` should return immediately, without waiting for agreement 
to complete. The application calls `Status(seq)` to find out whether the 
Paxos peer thinks the instance has reached agreement, and if so what the agreed 
value is. `Status()` should consult the local Paxos peer's state and return 
immediately; it should not communicate with other peers. The application may 
call `Status()` for old instances (but see the discussion of `Done()` below).

Your implementation should be able to make progress on agreement for multiple
instances at the same time. That is, if application peers call `Start()` with
different sequence numbers at about the same time, your implementation should
run the Paxos protocol concurrently for all of them. You should not wait for
agreement to complete for instance i before starting the protocol for instance
i+1. Each instance should have its own separate execution of the Paxos protocol.

A long-running Paxos-based server must forget about instances that are no longer
needed, and free the memory storing information about those instances. An
instance is needed if the application still wants to be able to call `Status()`
for that instance, or if another Paxos peer may not yet have reached agreement
on that instance. Your Paxos should implement freeing of instances in the
following way. When a particular peer application will no longer need to call
`Status()` for any instance <= x, it should call `Done(x)`. That Paxos peer can't
yet discard the instances, since some other Paxos peer might not yet have agreed
to the instance. So each Paxos peer should tell each other peer the highest Done
argument supplied by its local application. Each Paxos peer will then have a
Done value from each other peer. It should find the minimum, and discard all
instances with sequence numbers <= that minimum. The `Min()` method returns this
minimum sequence number plus one.

It's OK for your Paxos to piggyback the Done value in the agreement protocol
packets; that is, it's OK for peer P1 to only learn P2's latest Done value the
next time that P2 sends an agreement message to P1. If `Start()` is called with a
sequence number less than `Min()`, the `Start()` call should be ignored. If 
`Status()` is called with a sequence number less than Min(), Status() should 
return Forgotten.

Here is the Paxos pseudo-code:
```
proposer(v):
    while not decided:
        choose n, unique and higher than any n seen so far
        send prepare(n) to all servers including self
        if prepare_ok(n, n_a, v_a) from majority:
            v' = v_a with highest n_a; choose own v otherwise
            send accept(n, v') to all
            if accept_ok(n) from majority:
                send decided(v') to all

acceptor's state:
    n_p (highest prepare seen)
    n_a, v_a (highest accept seen)

acceptor's prepare(n) handler:
    if n > n_p
        n_p = n
        reply prepare_ok(n, n_a, v_a)
    else
        reply prepare_reject

acceptor's accept(n, v) handler:
    if n >= n_p
        n_p = n
        n_a = n
        v_a = v
        reply accept_ok(n)
    else
        reply accept_reject
```

## Test
To test your codes, try `go test` under the paxos folder. You may see some 
error messages during the test, but as long as it shows "Passed" in the end 
of the test case, it passes the test case. When you pass all the tests, you 
will see output as follows.
```
Test: Single proposer ...    
... Passed
Test: Many proposers, same value ...
... Passed
Test: Many proposers, different values ...
... Passed
Test: Out-of-order instances ...
... Passed
Test: Deaf proposer ...
... Passed
Test: Forgetting ...
... Passed
Test: Lots of forgetting ...
... Passed
Test: Paxos frees forgotten instance memory ...
... Passed
Test: Many instances ...
... Passed
Test: Minority proposal ignored ...
... Passed
Test: Many instances, unreliable RPC ...
... Passed
Test: No decision if partitioned ...
... Passed
Test: Decision in majority partition ...
... Passed
Test: All agree after full heal ...
... Passed
Test: One peer switches partitions ...
... Passed
Test: One peer switches partitions, unreliable ...
... Passed
Test: Many requests, changing partitions ...
... Passed
PASS
ok      paxos   59.523s
```
All the test cases provided in the skeleton are the ones to grade your 
assignment.

## Hints
Here's a plan for reference:

1. Add elements to the Paxos struct in paxos.go to hold the state you'll need,
   according to the lecture pseudo-code. You'll need to define a struct to hold
   information about each agreement instance.
2. Define RPC argument/reply type(s) for Paxos protocol messages, based on the
   paper pseudo-code. The RPCs must include the sequence number for the
   agreement instance to which they refer. Remember the field names in the RPC
   structures must start with capital letters.
3. Write a proposer function that drives the Paxos protocol for an instance,
   and RPC handlers that implement acceptors. Start a proposer function in its
   own thread for each instance, as needed (e.g. in Start()).
4. At this point you should be able to pass the first few tests.
5. Now implement forgetting.

Hint: more than one Paxos instance may be executing at a given time, and they
may be Start()ed and/or decided out of order (e.g. seq 10 may be decided before
seq 5).

Hint: in order to pass tests assuming unreliable network, your paxos should call
the local acceptor through a function call rather than RPC.

Hint: remember that multiple application peers may call Start() on the same
instance, perhaps with different proposed values. An application may even call
Start() for an instance that has already been decided.

Hint: think about how your paxos will forget (discard) information about old
instances before you start writing code. Each Paxos peer will need to store
instance information in some data structure that allows individual instance
records to be deleted (so that the Go garbage collector can free / re-use the
memory).

Hint: you do not need to write code to handle the situation where a Paxos peer
needs to re-start after a crash. If one of your Paxos peers crashes, it will
never be re-started.

Hint: have each Paxos peer start a thread per un-decided instance whose job is
to eventually drive the instance to agreement, by acting as a proposer.

Hint: a single Paxos peer may be acting simultaneously as acceptor and proposer
for the same instance. Keep these two activities as separate as possible.

Hint: a proposer needs a way to choose a higher proposal number than any seen so
far. This is a reasonable exception to the rule that proposer and acceptor
should be separate. It may also be useful for the propose RPC handler to return
the highest known proposal number if it rejects an RPC, to help the caller pick
a higher one next time. The px.me value will be different in each Paxos peer, so
you can use px.me to help ensure that proposal numbers are unique.

Hint: figure out the minimum number of messages Paxos should use when reaching
agreement in non-failure cases and make your implementation use that minimum.

Hint: the tester calls Kill() when it wants your Paxos to shut down; Kill() sets
px.dead. You should call px.isdead() in any loops you have that might run for a
while, and break out of the loop if px.isdead() is true. It's particularly
important to do this any in any long-running threads you create.

