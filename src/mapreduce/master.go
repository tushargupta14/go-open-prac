package mapreduce
import "container/list"
import "fmt"
type WorkerInfo struct {
  address string
  // You can add definitions here.
}


// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
  l := list.New()
  for _, w := range mr.Workers {
    DPrintf("DoWork: shutdown %s\n", w.address)
    args := &ShutdownArgs{}
    var reply ShutdownReply;
    ok := call(w.address, "Worker.Shutdown", args, &reply)
    if ok == false {
      fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
    } else {
      l.PushBack(reply.Njobs)
    }
  }
  return l
}

func DoTask(jobNum int, mr *MapReduce, jobType string){

  var doJobargs DoJobArgs
  var doJobReply DoJobReply

  switch jobType{
    case Map: 
      doJobargs = DoJobArgs{mr.file, "Map", jobNum, mr.nReduce}
    case Reduce: 
      doJobargs = DoJobArgs{mr.file, "Reduce", jobNum, mr.nMap}
  }
  
  worker := <- mr.registerChannel
  status:= call(worker,"Worker.DoJob", &doJobargs, &doJobReply)
  // for status == false{
  //   fmt.Println("Worker failed")
  //   worker = <- mr.registerChannel
  //   status= call(worker,"Worker.DoJob", &doJobargs, &doJobReply)
  //   if status{
  //     break
  //   }
  // }
  if status{
    mr.TaskChannel <- true
    mr.registerChannel <- worker  
  }
  
}

func (mr *MapReduce) RunMaster() *list.List {
  // Your code here

  // Distribute map jobs

  for i:=0 ; i < mr.nMap; i++ {
    go DoTask(i, mr, "Map")
  }

  mapJobs:=mr.nMap

  for mapJobs > 0{
    //fmt.Println("mapJobs", mapJobs)
    <- mr.TaskChannel
    mapJobs-=1
  }

  //fmt.Println("Starting the Reduce Phase")
  for j:=0 ; j < mr.nReduce; j++{
    go DoTask(j, mr, "Reduce")
  }
  
  reduceJobs:=mr.nReduce

  for reduceJobs > 0{
    //fmt.Println("reduceJobs", reduceJobs)
    <- mr.TaskChannel
    reduceJobs--
  }
  //mr.DoneChannel <- true
  return mr.KillWorkers()
}
