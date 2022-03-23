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

const Debug=0

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
  return nil
}

func (kv *KVPaxos) Wait(seq int) interface{}{
  to := 10 * time.Millisecond
  for {
   decided, v1 := kv.px.Status(seq)
   if decided {
     return v1
   }
   time.Sleep(to)
   if to < 10 * time.Second {
     to *= 2
   }
 }
}


func (kv *KVPaxos) ForwardPut(key string , value string, requestID int64) error {

  kv.mu.Lock()
  defer kv.mu.Unlock()

  if args.DoHash == false{
      kv.kvstore[key] = value
      kv.idstore[requestID] = value
      return nil
  } else if args.DoHash == true{
      // pb.kvstore[args.Key] = args.Value
      // pb.idstore[args.RequestID] = args.PreviousValue
      // reply.Err = "OK"
      fmt.Println("Implement Hash")
      return nil
  }

  return nil
}

func (kv *KVPaxos) Put(args *PutArgs, reply *PutReply) error {
  // Your code here.
  kv.mu.Lock()
  defer kv.mu.Unlock()

  seq_num := kvpaxos.last_seq_done + 1
  op = Op{args.Key, args.Value, seq_num, args.RequestID}
  //decided, op_v1 := px.Status(seq_num)
  // request_done, ok := kv.idstore[args.RequestID]
  // if ok{
  //   if request_done == args.Value{
  //     // Request already completed
  //     reply.Err = "OK"
  //     return nil
  //   }
  // }

  for {

    px.Start(seq_num, op)
    v1:= kv.Wait(seq_num)

    if v1.RequestID == op.RequestID{
      // The request has been completed
      break
    }
    seq_num++
  } 

  ForwardPut(op)
  reply.Err = "OK"
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
  pb.kvstore = make(map[string]string)
  pb.idstore = make(map[int64]*Op)
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

