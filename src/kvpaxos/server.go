package kvpaxos

import "net"
import "fmt"
import "net/rpc"
import "log"
import "paxos"
import "sync"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "time"
import "strconv"
const Debug=1

func DPrintf(format string, a ...interface{}) (n int, err error) {
  if Debug > 0 {
    log.Printf(format, a...)
  }
  return
}


type Op struct {
  // Your definitions here.
  // Field names must start with capital letters,
  // otherwise RPC will break.
  Key string
  Value string
  Seq int
  RequestID int64
  Op_type string
}

type KVPaxos struct {
  mu sync.Mutex
  l net.Listener
  me int
  dead bool // for testing
  unreliable bool // for testing
  px *paxos.Paxos
  last_seq_done int
  kvstore map[string]string
  idstore map[int64]string
  // Your definitions here.
}


func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
  // Your code here.
  kv.mu.Lock()
  defer kv.mu.Unlock()

  //fmt.Println("Start Get Key", args.Key)
  seq_num := kv.last_seq_done + 1
  op := Op{args.Key, "", seq_num, args.RequestID, "Get"}
  //fmt.Println("Start() seq_num: ", seq_num)
  for {

    kv.px.Start(seq_num, op)
    v1:= kv.Wait(seq_num)

    if v1.RequestID == op.RequestID{
      // The request has been completed
      break
    }

    // Encountered an earlier Put operation from the Paxos log, Apply it
    //fmt.Println("Operation, encountered", v1.Op_type, "Seq", v1.Seq, v1.Key, v1.Value)
    kv.Apply(v1)

    seq_num+=1
    op = Op{args.Key, "", seq_num, args.RequestID, "Get"}
  }

  //fmt.Println("Get consensus completed")

  _, ok := kv.kvstore[args.Key]
  if ok {
    reply.Value = kv.kvstore[args.Key]
    reply.Err = "OK"
  } else {
    reply.Value = ""
    reply.Err = "OK"
  }
  kv.last_seq_done = seq_num
  kv.px.Done(kv.last_seq_done)
  kv.px.Min()

  return nil
}

func (kv *KVPaxos) Wait(seq int) Op {
  to := 10 * time.Millisecond
  for {
   decided, v1 := kv.px.Status(seq)
   if decided {
     return v1.(Op)
   }
   if to < 10 * time.Second {
     //to += 10 * time.Millisecond
      to *= 2
   }
   time.Sleep(to)
   
 }
}

func (kv *KVPaxos) Apply(op Op) string{
  _, done := kv.idstore[op.RequestID]
  //fmt.Println("Inside Apply", op)
  if op.Op_type == "Put" && !done{
    //_, done := kv.idstore[op.RequestID]
      kv.kvstore[op.Key] = op.Value
      //kv.idstore[op.RequestID] = op.Value

      //fmt.Println(op.Key, kv.kvstore[op.Key])
    } else if op.Op_type == "Get" {
      return kv.kvstore[op.Key]
    } else if op.Op_type == "PutHash" && !done{

      new_value, previous_value := kv.compute_hash(op.Key, op.Value)
      kv.kvstore[op.Key] = new_value
      kv.idstore[op.RequestID] = previous_value
      //fmt.Println(op.Key, op.Value, op.Op_type, previous_value, new_value)
      return previous_value
    } 

  return ""
}

func (kv *KVPaxos) compute_hash(key string, value string) (string, string){
  previous_value, ok := kv.kvstore[key] 
  if !ok {
    previous_value = ""
  }
  h := hash(previous_value + value)
  new_value := strconv.Itoa(int(h))
  return new_value, previous_value
}

func (kv *KVPaxos) Put(args *PutArgs, reply *PutReply) error {
  // Your code here.
  kv.mu.Lock()
  defer kv.mu.Unlock()

  //fmt.Println("Put Operation", args.Key, args.Value, args.DoHash)
  // Check if the request is already completed, works for both Put / PutHash()
  _, done := kv.idstore[args.RequestID]
  if done{
    reply.Err = "OK"
    reply.PreviousValue = kv.idstore[args.RequestID]
    return nil
  }

  seq_num := kv.last_seq_done + 1

  if !args.DoHash {
    op := Op{args.Key, args.Value, seq_num, args.RequestID, "Put"}
    for {
      //fmt.Println("Start() seq_num: ", seq_num)
      kv.px.Start(seq_num, op)
      v1 := kv.Wait(seq_num)

      if v1.RequestID == op.RequestID{
        // The request has been completed
        kv.Apply(v1)
        break
      }
      // Apply the operation
      kv.Apply(v1)
      seq_num+=1
      op = Op{args.Key, args.Value, seq_num, args.RequestID, "Put"}
    } 
    
    //fmt.Println("Put consensus completed", seq_num, args.Key, args.Value)
    // kv.kvstore[args.Key] = args.Value
    // kv.idstore[args.RequestID] = args.Value
    
    reply.Err = "OK"
    reply.PreviousValue = args.Value

    kv.last_seq_done = seq_num
    kv.px.Done(seq_num)
    kv.px.Min()
  } else{

    op := Op{args.Key, args.Value, seq_num, args.RequestID, "PutHash"}
    for {
      //fmt.Println("Start() seq_num: ", seq_num)
      kv.px.Start(seq_num, op)
      v1:= kv.Wait(seq_num)

      if v1.RequestID == op.RequestID{
        // The request has been completed
        //fmt.Println("After this 2")
        reply.PreviousValue = kv.Apply(v1)
        break
      }
      // Apply the operation
      //fmt.Println("After this 1")
      kv.Apply(v1)
      seq_num+=1
      op = Op{args.Key, args.Value, seq_num, args.RequestID, "PutHash"}
    } 
    
    //fmt.Println("PutHash consensus completed", seq_num, args.Key, args.Value)
    
    //////////////
    // _, done := kv.idstore[op.RequestID]
    // if !done {
    //   new_value, previous_value := kv.compute_hash(args.Key, args.Value)
    //   kv.kvstore[args.Key] = new_value
    //   kv.idstore[args.RequestID] = previous_value
    //   reply.PreviousValue = previous_value
    // } else {
    //   reply.PreviousValue = kv.idstore[args.RequestID]
    // }
    reply.Err = "OK"
    
    //fmt.Println("Value returned-->", args.Key, args.Value, args.DoHash, reply.PreviousValue)
    kv.last_seq_done = seq_num
    kv.px.Done(seq_num)
    kv.px.Min()

  }

  return nil
}

// tell the server to shut itself down.
// please do not change this function.
func (kv *KVPaxos) kill() {
  DPrintf("Kill(%d): die\n", kv.me)
  kv.dead = true
  kv.l.Close()
  kv.px.Kill()
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// 
func StartServer(servers []string, me int) *KVPaxos {
  // call gob.Register on structures you want
  // Go's RPC library to marshall/unmarshall.
  gob.Register(Op{})

  kv := new(KVPaxos)
  kv.me = me
  kv.kvstore = make(map[string]string)
  kv.idstore = make(map[int64]string)
  // Your initialization code here.

  rpcs := rpc.NewServer()
  rpcs.Register(kv)

  kv.px = paxos.Make(servers, me, rpcs)

  os.Remove(servers[me])
  l, e := net.Listen("unix", servers[me]);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  kv.l = l


  // please do not change any of the following code,
  // or do anything to subvert it.

  go func() {
    for kv.dead == false {
      conn, err := kv.l.Accept()
      if err == nil && kv.dead == false {
        if kv.unreliable && (rand.Int63() % 1000) < 100 {
          // discard the request.
          conn.Close()
        } else if kv.unreliable && (rand.Int63() % 1000) < 200 {
          // process the request but force discard of reply.
          c1 := conn.(*net.UnixConn)
          f, _ := c1.File()
          err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
          if err != nil {
            fmt.Printf("shutdown: %v\n", err)
          }
          go rpcs.ServeConn(conn)
        } else {
          go rpcs.ServeConn(conn)
        }
      } else if err == nil {
        conn.Close()
      }
      if err != nil && kv.dead == false {
        fmt.Printf("KVPaxos(%v) accept: %v\n", me, err.Error())
        kv.kill()
      }
    }
  }()

  return kv
}

