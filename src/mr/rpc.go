package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

// Add your RPC definitions here.
const (
	Map     = "Map"
	Reduce  = "Reduce"
	None    = "None"
	AllDone = "AllDone"
)

// client register for a worker id
type RegisterWorkerIdArgs struct {
	Msg string
}

type RegisterWorkerIdReply struct {
	WorkerId int
}

// client sent a request to fetch a job
type RequestJobArgs struct {
	Msg      string
	WorkerId int
}

type RequestJobReply struct {
	JobType    string   // mapper job or reducer job
	JobId      int      // mapper job or reducer job id
	Filenames  []string // the input filename to mapper job or reducer
	NumReducer int      // number of reducer in total
}

type FinishJobArgs struct {
	JobType   string // mapper job or reducer job
	JobId     int
	WorkerId  int      // worker id
	Filenames []string // filename that is done
}

type FinishJobReply struct {
	Msg string
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
