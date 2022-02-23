package pbservice

import "hash/fnv"

const (
  OK = "OK"
  ErrNoKey = "ErrNoKey"
  ErrWrongServer = "ErrWrongServer"
)
type Err string

type PutArgs struct {
  Key string
  Value string
  DoHash bool // For PutHash
  RequestID int64
  // You'll have to add definitions here.

  // Field names must start with capital letters,
  // otherwise RPC will break.
}

type ForwardArgs struct {
  Key string
  PreviousValue string
  Value string
  DoHash bool // For PutHash
  RequestID int64
}

type ForwardReply struct {
  Err Err
  PreviousValue string
}

type CopyArgs struct {
  KVstore map[string]string
  IDstore map[int64]string
}

type CopyReply struct {
  Err Err
}

type PutReply struct {
  Err Err
  PreviousValue string // For PutHash
}

type GetArgs struct {
  Key string
  // You'll have to add definitions here.
}

type GetReply struct {
  Err Err
  Value string
}


// Your RPC definitions here.

func hash(s string) uint32 {
  h := fnv.New32a()
  h.Write([]byte(s))
  return h.Sum32()
}

// Completed commit