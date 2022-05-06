package shardkv

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "paxos"
import "sync"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "shardmaster"
import "strconv"
const Debug=0

func DPrintf(format string, a ...interface{}) (n int, err error) {
        if Debug > 0 {
                log.Printf(format, a...)
        }
        return
}


type Op struct {
  // Your definitions here.
  ID int64
  Op_type string
  DoHash bool
  newConfig shardmaster.Config
  Key string
  Value string
  Shard int
  KVstore map[string]string
  IDstore map[int64]string
}


type ShardKV struct {
  mu sync.Mutex
  l net.Listener
  me int
  dead bool // for testing
  unreliable bool // for testing
  sm *shardmaster.Clerk
  px *paxos.Paxos

  gid int64 // my replica group ID

  globalSeq int
  config shardmaster.Config

  kvstore map[int]map[string]string
  visitedRequests map[int]map[int64]string
  // Your definitions here.
}

// applyChange Applies the operation (Op) to the server database.
// the caller should hold the lock
func (kv *ShardKV) applyChange(op Op) (Err, string) {
  if op.Op_type == "Put" {
    // Case 1: Put

    // check if already applied?
    fmt.Println("Apply Put", op.Key, ":", op.Value, "DoHash:", op.DoHash)
    shardNum := key2shard(op.Key)
    _, found := kv.visitedRequests[shardNum]
    if found {
      prevV, ok := kv.visitedRequests[shardNum][op.ID]
      if ok {
        return OK, prevV  
      } 
      
    }


    // get the old value and new value based on Hash/noHash.

    oldValue := kv.kvstore[shardNum][op.Key]
    newValue := ""
    
    if op.DoHash {
      newValue = strconv.Itoa(int(hash(oldValue + op.Value)))
    } else {
      newValue = op.Value
    }

    //DPrintf("Server %v, apply op: %v, old: %v, new: %v\n", kv.me, op, oldValue, newValue)
    //fmt.Println("Here")
    // update db with new value.
    _, found = kv.kvstore[shardNum]
    if found{
      kv.kvstore[shardNum][op.Key] = newValue
    } else{
      kv.kvstore[shardNum] = make(map[string]string)
      kv.kvstore[shardNum][op.Key] = op.Value
    }
    

    // Only PutHash needs old value:
    if !op.DoHash {
      // Discard to save memory
      oldValue = ""
    }

    // update request reply DB.
    _, found = kv.visitedRequests[shardNum]
    if found{
      kv.visitedRequests[shardNum][op.ID] = oldValue
    } else{
      kv.visitedRequests[shardNum] = make(map[int64]string)
      kv.visitedRequests[shardNum][op.ID] = oldValue
    }
    //kv.visitedRequests[shardNum][op.ID] = oldValue
    //fmt.Println(kv.kvstore)
    return OK, oldValue

  } else if op.Op_type == "Get" {
    // Case 2: Get, If key present in DB return value else return ErrNoKey.
    shardNum := key2shard(op.Key)
    fmt.Println("Apply Get", op.Key, "Shard", shardNum)
    
    value, found := kv.kvstore[shardNum][op.Key]
    //fmt.Println(kv.kvstore)
    if found {
      return OK, value
    } else {
      return ErrNoKey, ""
    }
  } else if op.Op_type == "Reconf"{
    // add update functioanlity

    new_conf := op.newConfig
    old_conf := kv.config
    curr_gid := kv.gid

    if old_conf.Num == 0 {
      kv.config = new_conf
      return "", ""
    }
    if new_conf.Num <= old_conf.Num {
      return "", ""
    }
    fmt.Println("Apply Reconf", old_conf.Shards, old_conf.Num, "-->", new_conf.Shards, new_conf.Num)
    shards_to_move := make([]bool, len(old_conf.Shards))

    // get current shards
    // for i,_ := range old_conf.Shards{
    //   if old_conf.Shards[i] == kv.gid{
    //     shards_in_old[i] = 1
    //   }
    // }
    // //fmt.Println("Shards to shift", shards_in_old)
    // // get shards that changed from the current
    for i:=0; i < len(new_conf.Shards); i++{
      shards_to_move[i] = (new_conf.Shards[i]==curr_gid) && !(old_conf.Shards[i]==curr_gid)
    }
    // send the data to the respective GIDs
    gid_shard_map := make(map[int64][]int)
    for i,_ := range new_conf.Shards{
      if shards_to_move[i] {
        // shard data needs to be transferred
        gid_shard_map[new_conf.Shards[i]] = append(gid_shard_map[new_conf.Shards[i]],i)
      }
    }
    //fmt.Println("Shards to move", shards_to_move)
    fmt.Println("Apply Reconf, Ready to send", gid_shard_map)

    for ngid, shards := range gid_shard_map{

      // get all servers of the gid
      servers, ok := new_conf.Groups[ngid]
      if ok {
        for _, srv := range servers {

          for shard,_ := range shards{
            args := &ReconfArgs{}
            args.KVstore = make(map[string]string)
            args.IDstore = make(map[int64]string)
            _, ok := kv.kvstore[shard]
            if ok {
              for k,v := range kv.kvstore[shard]{
                args.KVstore[k] = v
              }
              
              for k,v := range kv.visitedRequests[shard]{
                args.IDstore[k] = v
              }
            }
            fmt.Println("Calling Send..with", args.KVstore)
            args.ID = rand.Int63()
            var reply ReconfReply
            done:= call(srv, "ShardKV.Update", args, &reply)
            if done {
              break
            }
          } 
          
        } 
      }
    }
    
    fmt.Println("Return after Apply Reconf")
    kv.config = new_conf

    return "", ""
  } else if op.Op_type == "Update"{

    fmt.Println("Apply Update", op.KVstore)
    kv.kvstore[op.Shard] = make(map[string]string)
    kv.visitedRequests[op.Shard] = make(map[int64]string)

    for k,v := range op.KVstore{
      kv.kvstore[op.Shard][k] = v
    }

    for k,v := range op.IDstore{
      kv.visitedRequests[op.Shard][k] = v
    }

  }
  return "", ""
}

// the caller should hold the lock
func (kv *ShardKV) makeAgreementAndApplyChange(op Op) (Err, string) {

  // increase sequence number.
  fmt.Println("Make Agreement for Op: ", op.Op_type)
  seq := kv.globalSeq + 1

  // Retry till this operation is entered into log.
  for {
    //DPrintf("Server %v is proposing seq %v\n", kv.me, seq)
    kv.px.Start(seq, op)
    agreedV := kv.WaitForAgreement(seq)
    if agreedV.(Op).ID != op.ID {
      seq++
      continue
    }
    //DPrintf("Server %v propose seq %v OK\n", kv.me, seq)
    // OK.
    break
  }
  // If [globalSeq+1, seq) is not null, then there is some log
  // beyond our commit, catch up first:
  for idx := kv.globalSeq + 1; idx < seq; idx++ {
    v := kv.WaitForAgreement(idx)
    kv.applyChange(v.(Op))
  }
  // Now we can apply our op, and return the value
  fmt.Println("Apply after Agreement for Op: ", op.Op_type)
  ret1, ret2 := kv.applyChange(op)

  // Update global seq
  kv.globalSeq = seq

  // mark seq as done.
  kv.px.Done(seq)
  return ret1, ret2
}

// WaitForAgreement this function waits for seq to be decided by paxos, and returns the decided operation.
func (kv *ShardKV) WaitForAgreement(seq int) interface{} {
  to := 10 * time.Millisecond
  for {
    ok, v := kv.px.Status(seq)
    if ok {
      return v
    }
    time.Sleep(to)
    if to < 10*time.Second {
      to += 2
    }
  }
}

func (kv *ShardKV) Update(args *ReconfArgs, reply *ReconfArgs) error {

  // kv.mu.Lock()
  // defer kv.mu.Unlock()
  fmt.Println("RPC Received data as", args.KVstore)
  op := Op{Op_type: "Update", Shard: args.Shard, KVstore: args.KVstore, IDstore: args.IDstore, ID: rand.Int63()} 
  
  
  kv.makeAgreementAndApplyChange(op)

  return nil
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
  // Your code here.
  kv.mu.Lock()
  defer kv.mu.Unlock()

  shardNum := key2shard(args.Key)
  keyGID := kv.config.Shards[shardNum]
  if keyGID != kv.gid{
    reply.Err = "ErrWrongGroup"
    return nil
  }
  op:= Op{
    Key: args.Key,
    Op_type: "Get",
    ID: args.ID,
  }

  err, ret2 := kv.makeAgreementAndApplyChange(op)
  reply.Err = err
  reply.Value = ret2

  return nil
}

func (kv *ShardKV) Put(args *PutArgs, reply *PutReply) error {
  // Your code here.
  kv.mu.Lock()
  defer kv.mu.Unlock()

  fmt.Println("Inside Put for Key", args.Key, "Value", args.Value)
  shardNum := key2shard(args.Key)
  keyGID := kv.config.Shards[shardNum]
  if keyGID != kv.gid{
    reply.Err = "ErrWrongGroup"
    return nil
  }

  op:= Op{
    Key: args.Key,
    Value: args.Value,
    Op_type: "Put",
    ID: args.ID,
    DoHash: args.DoHash,
  }

  err, ret2 := kv.makeAgreementAndApplyChange(op)
  reply.Err = err
  reply.PreviousValue = ret2

  return nil
}

//
// Ask the shardmaster if there's a new configuration;
// if so, re-configure.
//
func (kv *ShardKV) tick() {

  kv.mu.Lock()
  defer kv.mu.Unlock()
  getConfig := kv.sm.Query(-1)

  if getConfig.Num != kv.config.Num{
    fmt.Println("Config Changed, old: ",kv.config.Num, "New:", getConfig.Num, kv.me)
    if getConfig.Num != kv.config.Num+1{
      fmt.Println("Config Changed, old: ", kv.config.Num, "New:", getConfig.Num)
    //fmt.Println("Config received as new", getConfig.Shards)
      getConfig = kv.sm.Query(kv.config.Num +1)
      }
    op := Op{
    ID: rand.Int63(),
    Op_type: "Reconf",
    newConfig: getConfig,
    }

    kv.makeAgreementAndApplyChange(op)
  }
}


// tell the server to shut itself down.
func (kv *ShardKV) kill() {
  kv.dead = true
  kv.l.Close()
  kv.px.Kill()
}

//
// Start a shardkv server.
// gid is the ID of the server's replica group.
// shardmasters[] contains the ports of the
//   servers that implement the shardmaster.
// servers[] contains the ports of the servers
//   in this replica group.
// Me is the index of this server in servers[].
//
func StartServer(gid int64, shardmasters []string,
                 servers []string, me int) *ShardKV {
  gob.Register(Op{})

  kv := new(ShardKV)
  kv.me = me
  kv.gid = gid
  kv.sm = shardmaster.MakeClerk(shardmasters)

  kv.kvstore = make(map[int]map[string]string)
  kv.visitedRequests = make(map[int]map[int64]string)

  kv.config = shardmaster.Config{Num: 0, Groups: map[int64][]string{}}
  //kv.config.Shards = make([shardmaster.NShards]int64)
  // Your initialization code here.
  // Don't call Join().

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
        fmt.Printf("ShardKV(%v) accept: %v\n", me, err.Error())
        kv.kill()
      }
    }
  }()

  go func() {
    for kv.dead == false {
      kv.tick()
      time.Sleep(250 * time.Millisecond)
    }
  }()

  return kv
}
