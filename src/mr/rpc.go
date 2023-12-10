package mr

import (
	"os"
	"strconv"
)

// your RPC definitions here.

type NullArgs struct{}

type NullReply struct{}

type DisArgs struct {
	JobType int
}

type DisReply struct {
	JobSeq   int
	NReduce  int
	Filename string
	Content  string
}

type DoneReply struct {
	IsDone bool
}

type SeqArgs struct {
	JobSeq int
}

func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
