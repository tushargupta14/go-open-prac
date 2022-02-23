package pbservice

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "viewservice"
import "os"
import "syscall"
import "math/rand"
import "sync"

import "strconv"

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
  if Debug > 0 {
    n, err = fmt.Printf(format, a...)
  }
  return
}

type PBServer struct {
  l net.Listener
  dead bool // for testing
  unreliable bool // for testing
  me string
  vs *viewservice.Clerk
  done sync.WaitGroup
  finish chan interface{}
  view viewservice.View
  mu sync.Mutex
  kvstore map[string]string
  idstore map[int64]string
  // Your declarations here.
}

func (pb *PBServer) ForwardPut(args *ForwardArgs, reply *ForwardReply) error {

  pb.mu.Lock()
  defer pb.mu.Unlock()

  if pb.view.Backup == pb.me && pb.view.Backup != ""{
    if args.DoHash == false{
      pb.kvstore[args.Key] = args.Value
      reply.Err = "OK"
      return nil
    } else if args.DoHash == true{
      pb.kvstore[args.Key] = args.Value
      pb.idstore[args.RequestID] = args.PreviousValue
      reply.Err = "OK"
      return nil
    }

  } else {
    reply.Err = "ErrWrongServer"
    return nil
  }

  return nil
}

func (pb *PBServer) compute_hash(key string, value string) (string, string){
  previous_value, ok := pb.kvstore[key] 
  if !ok {
    previous_value = ""
  }
  h := hash(previous_value + value)
  new_value := strconv.Itoa(int(h))
  return new_value, previous_value
}

func (pb *PBServer) Put(args *PutArgs, reply *PutReply) error {
  // Your code here.
  pb.mu.Lock()
  defer pb.mu.Unlock()

  if pb.me == pb.view.Primary && pb.view.Primary != ""{
    //fmt.Println("Inside Put")
    if args.DoHash == false{
      // Normal Put function
      if pb.view.Backup != ""{
        //fmt.Println("Backup Found, Forward Put call")
        fargs := ForwardArgs{args.Key, args.Value, args.Value, false, args.RequestID}
        var freply ForwardReply

        ok := call(pb.view.Backup, "PBServer.ForwardPut", &fargs, &freply)
        if ok==false || freply.Err != OK{
          reply.Err = "ErrWrongServer"
          return nil
        }
      }
      pb.kvstore[args.Key] = args.Value
      reply.Err = "OK"
      return nil

    } else if args.DoHash == true{
      
      _, done := pb.idstore[args.RequestID]

      if done{
        // request completed
        //fmt.Println("Request already completed")
        reply.Err = "OK"
        reply.PreviousValue = pb.idstore[args.RequestID]
        return nil
      }

      new_value, previous_value := pb.compute_hash(args.Key, args.Value)
      if pb.view.Backup != ""{
        //fmt.Println("Backup Found, Forward Put call")
        fargs := ForwardArgs{args.Key, previous_value, new_value, true, args.RequestID}
        var freply ForwardReply

        ok := call(pb.view.Backup, "PBServer.ForwardPut", &fargs, &freply)
        if ok==false || freply.Err != OK{
          reply.Err = "ErrWrongServer"
          return nil
        }
      }
      
      //fmt.Println("New", new_value, "Prev",previous_value)
      pb.kvstore[args.Key] = new_value
      reply.Err = "OK"
      reply.PreviousValue = previous_value
      pb.idstore[args.RequestID] = previous_value
      return nil
    }
    } else {
      // The current server is Backup
      reply.Err = "ErrWrongServer"
      return nil 
    }
  return nil
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
  // Your code here.
  pb.mu.Lock()
  defer pb.mu.Unlock()

  if pb.me == pb.view.Primary && pb.view.Primary != ""{

    // if pb.view.Backup != ""{
    //   // Forward request
    //   // Set the Err
    //   return nil
    // }

    if val, ok := pb.kvstore[args.Key]; ok{
      reply.Err = "OK"
      reply.Value = val
      return nil
    } else {
      reply.Err = "ErrNoKey"
      return nil
    }

  }

  return nil
}

func (pb *PBServer) CopyDB(args *CopyArgs, reply *CopyReply) error {
  // Your code here.
  pb.mu.Lock()
  defer pb.mu.Unlock()

  v, _ := pb.vs.Ping(pb.view.Viewnum)
  pb.view = v

  if v.Backup == pb.me && v.Backup != ""{
    // Current Backup
    pb.kvstore = args.KVstore
    pb.idstore = args.IDstore
    reply.Err = "OK"
    return nil
  } else {
    reply.Err = "ErrWrongServer"
    return nil
  }
  return nil
}


// ping the viewserver periodically.
func (pb *PBServer) tick() {
  // Your code here.
  pb.mu.Lock()
  defer pb.mu.Unlock()

  v, _ := pb.vs.Ping(pb.view.Viewnum)

  //pb.view = v 
  if v.Primary == pb.me{
    curr_backup := pb.view.Backup
    if curr_backup != v.Backup && v.Backup !=""{
      // Sync both the servers
      copyargs := CopyArgs{pb.kvstore, pb.idstore}
      var copyreply CopyReply
      //fmt.Println("New backup found")
      ok := call(v.Backup, "PBServer.CopyDB", &copyargs, &copyreply)
      for copyreply.Err != OK || ok == false {
        copyreply.Err = ""
        ok = call(v.Backup, "PBServer.CopyDB", &copyargs, &copyreply)
      }
      // if copyreply.Err != OK || !ok{
      //   fmt.Println("Copy Failed, Forward to Wrong server")
     
    }
    pb.view = v
    return 
  }
  
  pb.view = v 
  return
  
}

// tell the server to shut itself down.
// please do not change this function.
func (pb *PBServer) kill() {
  pb.dead = true
  pb.l.Close()
}


func StartServer(vshost string, me string) *PBServer {
  pb := new(PBServer)
  pb.me = me
  pb.vs = viewservice.MakeClerk(me, vshost)
  pb.finish = make(chan interface{})
  // Your pb.* initializations here.
  pb.view.Viewnum = 0
  pb.view.Primary = ""
  pb.view.Backup = ""
  pb.kvstore = make(map[string]string)
  pb.idstore = make(map[int64]string)

  rpcs := rpc.NewServer()
  rpcs.Register(pb)

  os.Remove(pb.me)
  l, e := net.Listen("unix", pb.me);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  pb.l = l

  // please do not change any of the following code,
  // or do anything to subvert it.

  go func() {
    for pb.dead == false {
      conn, err := pb.l.Accept()
      if err == nil && pb.dead == false {
        if pb.unreliable && (rand.Int63() % 1000) < 100 {
          // discard the request.
          conn.Close()
        } else if pb.unreliable && (rand.Int63() % 1000) < 200 {
          // process the request but force discard of reply.
          c1 := conn.(*net.UnixConn)
          f, _ := c1.File()
          err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
          if err != nil {
            fmt.Printf("shutdown: %v\n", err)
          }
          pb.done.Add(1)
          go func() {
            rpcs.ServeConn(conn)
            pb.done.Done()
          }()
        } else {
          pb.done.Add(1)
          go func() {
            rpcs.ServeConn(conn)
            pb.done.Done()
          }()
        }
      } else if err == nil {
        conn.Close()
      }
      if err != nil && pb.dead == false {
        fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
        pb.kill()
      }
    }
    DPrintf("%s: wait until all request are done\n", pb.me)
    pb.done.Wait() 
    // If you have an additional thread in your solution, you could
    // have it read to the finish channel to hear when to terminate.
    close(pb.finish)
  }()

  pb.done.Add(1)
  go func() {
    for pb.dead == false {
      pb.tick()
      time.Sleep(viewservice.PingInterval)
    }
    pb.done.Done()
  }()

  return pb
}
