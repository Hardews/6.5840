package mr

import (
	"os"
	"strconv"
)

//
// RPC definitions.
//
// remember to capitalize all names.
//

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type NullArgs struct{}

type NullReply struct{}

type DisReply struct {
	WorkerSeq int
	NReduce   int
	Filename  string
	Content   string
}

type FinishArgs struct {
	WorkerSeq int
}

type DoneReply struct {
	IsDone bool
}

type PingArgs struct {
	WorkerType int
	WorkerSeq  int
}

func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
