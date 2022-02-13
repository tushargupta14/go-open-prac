package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"

type ViewServer struct {
  mu           sync.Mutex
  l            net.Listener
  dead         bool
  me           string
  curr_view    uint
  is_ack       bool
  curr_primary string
  curr_backup  string
  time_map     map[string]time.Time
  idle_server  string
  // Your declarations here.
}

//
// server Ping RPC handler.
//

func (self ViewServer) increment() {
  // https://go.dev/play/
  self.curr_view++
}

func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {

  // Your code here.
  vs.mu.Lock()
  defer vs.mu.Unlock()

  viewnum := args.Viewnum
  client := args.Me
  vs.time_map[client] = time.Now()

  //fmt.Println("Pinged By", client, viewnum, "ACKED BY PRIMARY", vs.is_ack)
  if client == vs.curr_primary {
    //fmt.Println("Inside Primary Viewnum:", viewnum, "Curr view", vs.curr_view)
    if viewnum == vs.curr_view {
      vs.is_ack = true
      if vs.curr_backup == "" && vs.idle_server!="" {
        vs.curr_backup = vs.idle_server
        vs.idle_server = ""
        vs.curr_view++
        vs.is_ack = false
        //fmt.Println("Backup server", vs.curr_backup)
      }
      reply.View = View{vs.curr_view, vs.curr_primary, vs.curr_backup}
      return nil
    } else if viewnum!=0 && viewnum < vs.curr_view{
        //fmt.Println("Here")
        reply.View = View{vs.curr_view, vs.curr_primary, vs.curr_backup}
        vs.is_ack = false
        return nil
    } else if viewnum == 0 {
      if vs.is_ack {
        vs.curr_primary = vs.curr_backup
        vs.curr_backup = ""
        vs.is_ack = false
        vs.curr_view++
        reply.View = View{vs.curr_view, vs.curr_primary, vs.curr_backup}
        return nil
      }
    }
  } else if client == vs.curr_backup {
    //fmt.Println("Backup server", vs.curr_backup)
    reply.View = View{vs.curr_view, vs.curr_primary, vs.curr_backup}
    return nil
  } else if viewnum == 0 {
    // Idle server
    if vs.curr_primary == "" {
      vs.curr_primary = client
      vs.is_ack = false
      vs.curr_view++
      //fmt.Printf("Here")
      reply.View = View{vs.curr_view, vs.curr_primary, vs.curr_backup}
    } else {
      vs.idle_server = client
      //fmt.Println("Idle server", vs.idle_server)
      reply.View = View{vs.curr_view, vs.curr_primary, vs.curr_backup}
      return nil
    }
  }
  return nil
}

// 
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {

  // Your code here.
  reply.View = View{vs.curr_view, vs.curr_primary, vs.curr_backup}

  return nil
}

//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {

  vs.mu.Lock()
  defer vs.mu.Unlock()

  if vs.curr_primary!= "" && time.Since(vs.time_map[vs.curr_primary]) > DeadPings * PingInterval {
    // Primary Dead
    //fmt.Println("Primary Failed")
    if vs.is_ack{
      vs.curr_primary = vs.curr_backup
      vs.curr_backup = func() (string){
      if vs.idle_server != ""{
        idle := vs.idle_server
        vs.idle_server = ""
        return idle
      } else{
        return ""
      }
    }()
    vs.curr_view++
    vs.is_ack = false
    }
    
  }
  //print("At Backup")
  if vs.curr_backup!= "" && time.Since(vs.time_map[vs.curr_backup]) > DeadPings * PingInterval {
    //fmt.Println("Backup Failed")
    if vs.is_ack{
      //vs.curr_primary = vs.curr_backup
      vs.curr_backup = func() (string){
      if vs.idle_server != ""{
        idle := vs.idle_server
        vs.idle_server = ""
        return idle
      } else{
        return ""
      }
    }()
    vs.curr_view++
    }
    
  }

  if vs.idle_server!= "" && time.Since(vs.time_map[vs.idle_server]) > DeadPings * PingInterval {
    //fmt.Println("Idle server failed")    
    vs.idle_server = ""
  }  
}

//
// tell the server to shut itself down.
// for testing.
// please don't change this function.
//
func (vs *ViewServer) Kill() {
  vs.dead = true
  vs.l.Close()
}

func StartServer(me string) *ViewServer {
  vs := new(ViewServer)
  vs.me = me
  // Your vs.* initializations here.
  vs.curr_view = 0
  vs.curr_primary = ""
  vs.curr_backup = ""
  vs.is_ack = false
  vs.time_map = make(map[string]time.Time)
  // tell net/rpc about our RPC server and handlers.
  rpcs := rpc.NewServer()
  rpcs.Register(vs)

  // prepare to receive connections from clients.
  // change "unix" to "tcp" to use over a network.
  os.Remove(vs.me) // only needed for "unix"
  l, e := net.Listen("unix", vs.me)
  if e != nil {
    log.Fatal("listen error: ", e)
  }
  vs.l = l

  // please don't change any of the following code,
  // or do anything to subvert it.

  // create a thread to accept RPC connections from clients.
  go func() {
    for vs.dead == false {
      conn, err := vs.l.Accept()
      if err == nil && vs.dead == false {
        go rpcs.ServeConn(conn)
      } else if err == nil {
        conn.Close()
      }
      if err != nil && vs.dead == false {
        fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
        vs.Kill()
      }
    }
  }()

  // create a thread to call tick() periodically.
  go func() {
    for vs.dead == false {
      vs.tick()
      time.Sleep(PingInterval)
    }
  }()

  return vs
}
