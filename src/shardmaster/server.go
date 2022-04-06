package shardmaster

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
//import "math/big"
//import "crypto/rand"

const Debug = 1 
func DPrintf(format string, a ...interface{}) (n int, err error) {
  if Debug > 0 {
    log.Printf(format, a...)
  }
  return
}

type ShardMaster struct {
  mu sync.Mutex
  l net.Listener
  me int
  dead bool // for testing
  unreliable bool // for testing
  px *paxos.Paxos
  globalSeq int 
  configs []Config // indexed by config num
}


type Op struct {
  // Your data here.
  Op_type string
  Num int
  Gid int64
  Servers []string
  Shard int
  ID int64
}

// func nrand() int64 {
//   max := big.NewInt(int64(1) << 62)
//   bigx, _ := rand.Int(rand.Reader, max)
//   x := bigx.Int64()
//   return x
// }


func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
  // Your code here.
  sm.mu.Lock()
  defer sm.mu.Unlock()

  op := Op{Op_type: "Join", Servers: args.Servers, Gid: args.GID, ID: rand.Int63()}
  sm.makeAgreementAndApplyChange(op)

  return nil
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
  // Your code here.
  sm.mu.Lock()
  defer sm.mu.Unlock()
  op := Op{Op_type: "Leave", Gid: args.GID, ID: rand.Int63()}
  sm.makeAgreementAndApplyChange(op)

  return nil
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
  // Your code here.
  sm.mu.Lock()
  defer sm.mu.Unlock()

  op := Op{Op_type: "Move", Shard: args.Shard, Gid: args.GID, ID: rand.Int63()}
  sm.makeAgreementAndApplyChange(op)

  return nil
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
  // Your code here.
  sm.mu.Lock()
  defer sm.mu.Unlock()

  op := Op{ Op_type: "Query",  Num: args.Num, ID: rand.Int63()}
  config := sm.makeAgreementAndApplyChange(op)  

  reply.Config = config

  return nil
}


func (sm *ShardMaster) applyChange(op Op) Config {

  if op.Op_type == "Query" {

    if op.Num == -1 || op.Num >= len(sm.configs){
      return sm.configs[len(sm.configs)-1]
    } else {
      return sm.configs[op.Num]
    }

  }
  // make new config using the last
  prevConfig := sm.configs[len(sm.configs)-1]
  newConfigNum := prevConfig.Num + 1
  var newShardArr [len(prevConfig.Shards)]int64
  var newGroups = make(map[int64][]string)

  for i := range prevConfig.Shards{
    newShardArr[i] = prevConfig.Shards[i]
  }
  for k,v := range prevConfig.Groups{
    newArr := make([]string, len(v))
    copy(newArr, v)
    newGroups[k] = newArr
  }

  newConfig := Config{Num: newConfigNum, Shards: newShardArr, Groups: newGroups}

  if op.Op_type == "Move" {    
    
    newConfig.Shards[op.Shard] = op.Gid
    
    sm.configs = append(sm.configs, newConfig)

    return newConfig

  } else if op.Op_type == "Join"{

    _, found := prevConfig.Groups[op.Gid]
    if found {
      return prevConfig
    }
    newConfig.Groups[op.Gid] = op.Servers
    numGIDs := len(newConfig.Groups) 

    gidNumMap := make(map[int64]int)
    gidNumMap[op.Gid] = 0 

    minMoves := len(prevConfig.Shards) / numGIDs 
    //fmt.Println("minMoves", minMoves)
    i := 0 
    maxVal := -1
    var maxGID int64
    minVal := 100000
    var minGID int64

    for i < minMoves{
      // GID -> numshards
      /*
      Possible cause of error
      */
      for i:= range newConfig.Shards{
        gidNumMap[newConfig.Shards[i]]++      
      }
      
      // calculate max and min GIDs
      for k,v := range gidNumMap{
        if v > maxVal {
          maxGID = k
          maxVal = v
        }
        if v < minVal{
          minVal = v
          minGID = k
        }
      }
      //fmt.Println("maxGID", maxGID, "minGID", minGID)
      // move one shard from maxGID to minGID
      for j := range newConfig.Shards{
        if newConfig.Shards[j] == maxGID{
          newConfig.Shards[j] = minGID
          break
        }
      }

      i++
    }
    sm.configs = append(sm.configs, newConfig)
    return newConfig
  } else if op.Op_type == "Leave"{
    // remaining GIDs
    _, found := prevConfig.Groups[op.Gid]
    if !found {
      return prevConfig
    }

    delete(newConfig.Groups, op.Gid)
    gidNumMap := make(map[int64]int)
    for k,_ := range newConfig.Groups {
      gidNumMap[k] = 0
      for i := range newConfig.Shards {
        if newConfig.Shards[i] == k {
          gidNumMap[k]++
        }
      }
    }

    gidArr := make([]int64, 0, len(newConfig.Groups))
    for key := range newConfig.Groups {
      gidArr = append(gidArr, key)
    }

    for i := range newConfig.Shards {
      var minGID int64
      minVal := 100000

      for k, v := range gidNumMap {
        if v <= minVal {
          minVal = v
          minGID = k
          }
        }
      
      if newConfig.Shards[i] == op.Gid {
        newConfig.Shards[i] = minGID
        gidNumMap[minGID]++
        }
    }
    sm.configs = append(sm.configs, newConfig)
    return newConfig
  }
  return newConfig
}

func (sm *ShardMaster) makeAgreementAndApplyChange(op Op) Config {

  // increase sequence number.
  seq := sm.globalSeq + 1

  // Retry till this operation is entered into log.
  for {
    //DPrintf("Server %v is proposing seq %v\n", sm.me, seq)
    sm.px.Start(seq, op)
    agreedV := sm.WaitForAgreement(seq)
    if agreedV.(Op).ID != op.ID {
      seq++
      continue
    }
    break
    time.Sleep(10 *time.Millisecond)
  }
  // If [globalSeq+1, seq) is not null, then there is some log
  // beyond our commit, catch up first:
  for idx := sm.globalSeq + 1; idx < seq; idx++ {
    v := sm.WaitForAgreement(idx)
    sm.applyChange(v.(Op))
  }
  // Now we can apply our op, and return the value
  config := sm.applyChange(op)

  // Update global seq
  sm.globalSeq = seq

  // mark seq as done.
  sm.px.Done(seq)
  return config
}

func (sm *ShardMaster) WaitForAgreement(seq int) interface{} {
  to := 10 * time.Millisecond
  for {
    ok, v := sm.px.Status(seq)
    if ok {
      return v
    }
    time.Sleep(to)
    if to < 10*time.Second {
      to += 2
    }
  }
}
// please don't change this function.
func (sm *ShardMaster) Kill() {
  sm.dead = true
  sm.l.Close()
  sm.px.Kill()
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
// 
func StartServer(servers []string, me int) *ShardMaster {
  gob.Register(Op{})

  sm := new(ShardMaster)
  sm.me = me

  sm.configs = make([]Config, 1)
  sm.configs[0].Groups = map[int64][]string{}

  rpcs := rpc.NewServer()
  rpcs.Register(sm)

  sm.px = paxos.Make(servers, me, rpcs)

  os.Remove(servers[me])
  l, e := net.Listen("unix", servers[me]);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  sm.l = l

  // please do not change any of the following code,
  // or do anything to subvert it.

  go func() {
    for sm.dead == false {
      conn, err := sm.l.Accept()
      if err == nil && sm.dead == false {
        if sm.unreliable && (rand.Int63() % 1000) < 100 {
          // discard the request.
          conn.Close()
        } else if sm.unreliable && (rand.Int63() % 1000) < 200 {
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
      if err != nil && sm.dead == false {
        fmt.Printf("ShardMaster(%v) accept: %v\n", me, err.Error())
        sm.Kill()
      }
    }
  }()

  return sm
}
