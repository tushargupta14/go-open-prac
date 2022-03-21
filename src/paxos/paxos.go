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
// px.Status(seq int) (decided bool, v interface{}) -- get info about an instance
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
import "fmt"
import "math/rand"
import "time"

type Paxos struct {
  mu sync.Mutex
  l net.Listener
  dead bool
  unreliable bool
  rpcCount int
  peers []string
  me int // index into peers[]

  instance_map map[int]*Instance
  seq_max int
  done_index []int
  //n_peers int
  // Your data here.
}

type Instance struct{
  np int
  na int
  va interface{}
  is_decided bool
}


type PrepareArgs struct{
  Seq int
  N int
  Z_i int
  Index int
}

type PrepareReply struct{
  State string
  Na int
  Va interface{}
  Z_i int
  Index int
}

type AcceptArgs struct{
  Seq int
  N int
  V interface{}
  Z_i int
  Index int
}

type AcceptReply struct{
  State string
  Na int
  Z_i int
  Index int
}

type DecideArgs struct{
  Seq int
  Na int
  Va interface{}
  Z_i int
  Index int
}

type DecideReply struct{
  State string
  Z_i int
  Index int
}

//
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
//
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


//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
 
func (px *Paxos) Prepare(args *PrepareArgs, reply *PrepareReply) error {
  px.mu.Lock()
  defer px.mu.Unlock()

  if args.Z_i > px.done_index[args.Index]{
    px.done_index[args.Index] = args.Z_i
  }

  instance, ok := px.instance_map[args.Seq]
  if !ok {
    instance = &Instance{-1, -1, nil, false}
    px.instance_map[args.Seq] = instance
  }
  if args.N > instance.np{
    instance.np = args.N
    reply.State = "PREPARE_OK"
    reply.Na = instance.na
    reply.Va = instance.va
  } else {
    reply.State = "PREPARE_REJECT"
    reply.Na = instance.na
    reply.Va = instance.va
  }
  reply.Z_i = px.done_index[px.me]
  reply.Index = px.me
  return nil
} 


func (px *Paxos) Accept(args *AcceptArgs, reply *AcceptReply) error {
  px.mu.Lock()
  defer px.mu.Unlock()

  if args.Z_i > px.done_index[args.Index]{
    px.done_index[args.Index] = args.Z_i
  }

  instance, ok := px.instance_map[args.Seq]
  if !ok {
    instance = &Instance{-1, -1, nil, false}
    px.instance_map[args.Seq] = instance
  }

  if args.N >= instance.np{
    instance.np = args.N
    instance.na = args.N
    instance.va = args.V

    reply.State = "ACCEPT_OK"
    reply.Na = args.N
  } else {
    reply.State = "ACCEPT_REJECT"
    reply.Na = args.N
  }
  reply.Z_i = px.done_index[px.me]
  reply.Index = px.me
  return nil
}

func (px *Paxos) Decide(args *DecideArgs, reply *DecideReply) error {
  px.mu.Lock()
  defer px.mu.Unlock()
  //Update the Z_i from proposer
  if args.Z_i > px.done_index[args.Index]{
    px.done_index[args.Index] = args.Z_i
  }

  instance, ok := px.instance_map[args.Seq]
  if !ok {
    instance = &Instance{-1, -1, nil, false}
    px.instance_map[args.Seq] = instance
  }

  instance.na = args.Na
  instance.va = args.Va
  instance.is_decided = true
  reply.Z_i = px.done_index[px.me]
  reply.Index = px.me
  return nil
}

func (px *Paxos) StartAgreement(seq int, v interface{}){

  num_peers := len(px.peers)
  instance := &Instance{np: -1, na: -1, va: nil, is_decided: false}
  px.mu.Lock()
  px.instance_map[seq] = instance
  px.mu.Unlock()
  //decided, _ := px.Status(seq)
  to := 10 * time.Millisecond
  for !instance.is_decided{
    // Chosse n
    if px.dead{
      return
    }
    time.Sleep(to)
    if to < time.Second {
      to *= 2
    }
    propose_n := instance.np + 1 + px.me
    //fmt.Println("Starting Prepare Phase for seq: ", seq)
    
    max_done := px.done_index[px.me]
    prepareargs := PrepareArgs{seq, propose_n, max_done, px.me}
    var preparereply PrepareReply
    count_prepare := 0 
    max_n := -100
    var v_dash interface{}
    // Prepare phase
    for index,peer := range px.peers{

      if index != px.me{
        call(peer, "Paxos.Prepare", &prepareargs, &preparereply)
      } else if index == px.me{  
        px.Prepare(&prepareargs, &preparereply)
      }

      if preparereply.State == "PREPARE_OK"{
          count_prepare++
          if preparereply.Na > max_n && index != px.me{
            max_n = preparereply.Na
            v_dash = preparereply.Va
          }
        }
      preparereply.State = ""
      if preparereply.Z_i > px.done_index[preparereply.Index]{
        px.done_index[preparereply.Index] = preparereply.Z_i
      }

    }

    if count_prepare <= num_peers/2{
      continue
    }
    if v_dash == nil{
      v_dash = v
    }
    // Accept phase
    //fmt.Println("Starting Accept Phase for seq: ", seq)
    max_done = px.done_index[px.me]
    acceptargs := AcceptArgs{seq, propose_n, v_dash, max_done, px.me}
    var acceptreply AcceptReply
    count_accept := 0 
    
    // Prepare phase
    for index,peer := range px.peers{

      if index != px.me{
        call(peer, "Paxos.Accept", &acceptargs, &acceptreply)
      } else if index == px.me{  
        px.Accept(&acceptargs, &acceptreply)
      }

      if acceptreply.State == "ACCEPT_OK"{
          count_accept++
        }
      acceptreply.State = ""
      if acceptreply.Z_i > px.done_index[acceptreply.Index]{
        px.done_index[acceptreply.Index] = acceptreply.Z_i
      }
    }

    if count_accept <= num_peers/2{
      continue
    }
    //fmt.Println("count_accept", count_accept)
    // Decide Phase
    //fmt.Println("Starting Decide Phase for seq: ", seq)
    max_done = px.done_index[px.me]
    decideargs := DecideArgs{Seq: seq, Na: propose_n, Va: v_dash, Z_i: max_done, Index: px.me}
    var decidereply DecideReply

    for index,peer := range px.peers{
      //fmt.Println("Here")
      if index != px.me{
        call(peer, "Paxos.Decide", &decideargs, &decidereply)
      } else if index == px.me{  
        px.Decide(&decideargs, &decidereply)
      }
      if decidereply.Z_i > px.done_index[decidereply.Index]{
        px.done_index[decidereply.Index] = decidereply.Z_i
      }
     }

    instance.is_decided = true
  } 
  
}
func (px *Paxos) Start(seq int, v interface{}) {
  // Your code here.
  //px.mu.Lock()
  //defer px.mu.Unlock()
  if seq < px.Min(){
    return
  }
  if seq > px.seq_max{
    px.seq_max = seq 
  }
  status, _ := px.Status(seq)
  if status{
    return
  }

  go px.StartAgreement(seq, v)
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
  // Your code here.
  px.mu.Lock()
  defer px.mu.Unlock()
  if px.done_index[px.me] < seq{
    px.done_index[px.me] = seq
  }
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
  // Your code here.
  px.mu.Lock()
  defer px.mu.Unlock()
  return px.seq_max
}

//
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
// 
func (px *Paxos) Min() int {
  // You code here.
  px.mu.Lock()
  defer px.mu.Unlock()

  min_v := px.done_index[px.me]

  for _,val := range px.done_index{
    if val < min_v{
      min_v = val
    }
  } 

  for i:=0 ; i <= min_v; i++{
    delete(px.instance_map, i)
  }
  return min_v + 1
  //return 0
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (bool, interface{}) {
  // Your code here.
  px.mu.Lock()
  px.mu.Unlock()
  instance, ok := px.instance_map[seq]
  if !ok{
    return false, nil
  }
  return instance.is_decided, instance.va
  //
}


//
// tell the peer to shut itself down.
// for testing.
// please do not change this function.
//
func (px *Paxos) Kill() {
  px.dead = true
  if px.l != nil {
    px.l.Close()
  }
}

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
  px := &Paxos{}
  px.peers = peers
  px.me = me
  px.instance_map = make(map[int]*Instance)
  px.seq_max = -100
  px.done_index = make([]int, len(peers))
  for i:=0 ; i < len(peers); i++{
    px.done_index[i] = -1
  } 
  // Your initialization code here.

  if rpcs != nil {
    // caller will create socket &c
    rpcs.Register(px)
  } else {
    rpcs = rpc.NewServer()
    rpcs.Register(px)

    // prepare to receive connections from clients.
    // change "unix" to "tcp" to use over a network.
    os.Remove(peers[me]) // only needed for "unix"
    l, e := net.Listen("unix", peers[me]);
    if e != nil {
      log.Fatal("listen error: ", e);
    }
    px.l = l
    
    // please do not change any of the following code,
    // or do anything to subvert it.
    
    // create a thread to accept RPC connections
    go func() {
      for px.dead == false {
        conn, err := px.l.Accept()
        if err == nil && px.dead == false {
          if px.unreliable && (rand.Int63() % 1000) < 100 {
            // discard the request.
            conn.Close()
          } else if px.unreliable && (rand.Int63() % 1000) < 200 {
            // process the request but force discard of reply.
            c1 := conn.(*net.UnixConn)
            f, _ := c1.File()
            err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
            if err != nil {
              fmt.Printf("shutdown: %v\n", err)
            }
            px.rpcCount++
            go rpcs.ServeConn(conn)
          } else {
            px.rpcCount++
            go rpcs.ServeConn(conn)
          }
        } else if err == nil {
          conn.Close()
        }
        if err != nil && px.dead == false {
          fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
        }
      }
    }()
  }


  return px
}
