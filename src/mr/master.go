package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	Done         = "Done"
	Pending      = "Pending"
	NotScheduled = "Not Scheduled"
)

type JobStatus struct {
	Status   string
	WorkerId int
}

type Master struct {
	// Your definitions here.
	// need to lock the object in concurrent env
	mu sync.Mutex
	// use an array to record all workers, we need to use this to reject
	// reply from workers that dead or straggle
	workers []int
	// use a array here to store the mapper input filenames, this is
	// used to generate the mapper task id
	mapperFilenames []string
	// use a dict here to store the status of mapper job
	mapperStatus map[int]JobStatus
	// use a map to store reducer input filenames
	reducerFilenames map[int][]string
	// use a dict here to store the status of reducer job
	reducerStatus  map[int]JobStatus
	numReducer     int
	mapperAllDone  bool
	reducerAllDone bool
}

// Your code here -- RPC handlers for the worker to call.

func (m *Master) RegisterWorkerId(args *RegisterWorkerIdArgs, reply *RegisterWorkerIdReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	workerId := len(m.workers)
	m.workers = append(m.workers, workerId)

	reply.WorkerId = workerId
	return nil
}

func (m *Master) CheckStatus(jobType string, jobId int) {
	time.Sleep(time.Second * 10)
	m.mu.Lock()
	defer m.mu.Unlock()
	if jobType == Map {
		jobStatus := m.mapperStatus[jobId]
		if jobStatus.Status != Done {
			m.mapperStatus[jobId] = JobStatus{Status: NotScheduled}
		}
	} else {
		jobStatus := m.reducerStatus[jobId]
		if jobStatus.Status != Done {
			m.reducerStatus[jobId] = JobStatus{Status: NotScheduled}
		}
	}
}

func (m *Master) RequestJob(args *RequestJobArgs, reply *RequestJobReply) error {
	// Lock the data and return a file that is not done yet
	m.mu.Lock()
	defer m.mu.Unlock()
	defer fmt.Println(m.mapperStatus)
	defer fmt.Println(m.reducerStatus)

	if m.reducerAllDone {
		reply.JobType = AllDone
		return nil
	}

	allDone := true

	// Iter through all Map job and see if there is any not finished yet
	if m.mapperAllDone == false {
		for i, filename := range m.mapperFilenames {
			jobStatus, _ := m.mapperStatus[i]
			switch jobStatus.Status {
			case Done:
				continue
			case Pending:
				allDone = false
			case NotScheduled:
				allDone = false
				reply.JobType = Map
				reply.JobId = i
				reply.Filenames = []string{filename}
				reply.NumReducer = m.numReducer
				m.mapperStatus[i] = JobStatus{Status: Pending, WorkerId: args.WorkerId}
				// kick off a goroutines to check if the job is timeout
				go m.CheckStatus(Map, i)
				return nil
			}
		}
	}

	// After check we found that we are waiting for map job to done
	if allDone == false {
		reply.JobType = None
		return nil
	}
	// We have done all mapper jobs, mark it as all done to avoid duplicated check
	m.mapperAllDone = true

	allDone = true
	// Iter through all Reduce job and see if there is any not finished yet
	if m.reducerAllDone == false {
		for i, filenames := range m.reducerFilenames {
			jobStatus, _ := m.reducerStatus[i]
			switch jobStatus.Status {
			case Done:
				continue
			case Pending:
				allDone = false
			case NotScheduled:
				allDone = false
				reply.JobType = Reduce
				reply.JobId = i
				reply.Filenames = filenames
				reply.NumReducer = m.numReducer
				m.reducerStatus[i] = JobStatus{Status: Pending, WorkerId: args.WorkerId}
				// kick off a goroutines to check if the job is timeout
				go m.CheckStatus(Reduce, i)
				return nil
			}
		}
	}

	if allDone == false {
		reply.JobType = None
		return nil
	} else {
		m.reducerAllDone = true
		reply.JobType = AllDone
	}

	return nil
}

func (m *Master) FinishJob(args *FinishJobArgs, reply *FinishJobReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	switch args.JobType {
	case Map:
		// Mark the corresponding Map job finished, and flash the intermediate file
		jobId := args.JobId
		filenames := args.Filenames
		fmt.Println("Receive all files ", filenames, " from ", args.WorkerId)
		// There might be duplicated job due to straggler, check the status to see if
		// the reply matches our record
		jobStatus, _ := m.mapperStatus[jobId]
		if jobStatus.Status == Pending && jobStatus.WorkerId == args.WorkerId {
			m.mapperStatus[jobId] = JobStatus{Status: Done, WorkerId: -1}
			for _, filename := range filenames {
				// fmt.Println("Start to process ", filename)
				token := strings.Split(filename, "-")
				reducerId, _ := strconv.ParseInt(token[len(token)-1], 10, 64)
				m.reducerFilenames[int(reducerId)] = append(m.reducerFilenames[int(reducerId)], filename)
			}
		}
	case Reduce:
		jobId := args.JobId
		filenames := args.Filenames
		jobStatus, _ := m.reducerStatus[jobId]
		if jobStatus.Status == Pending && jobStatus.WorkerId == args.WorkerId {
			fmt.Println("Received successfully finished reducer task ", jobId, ", result in file", filenames)
			m.reducerStatus[jobId] = JobStatus{Status: Done, WorkerId: -1}
		}
	}

	reply.Msg = "Good job"
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.reducerAllDone
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	m.mu.Lock()
	m.mapperFilenames = files
	m.mapperStatus = make(map[int]JobStatus)
	for i, _ := range files {
		m.mapperStatus[i] = JobStatus{Status: NotScheduled}
	}
	m.reducerFilenames = make(map[int][]string)
	m.reducerStatus = make(map[int]JobStatus)
	for i := 0; i < nReduce; i++ {
		m.reducerStatus[i] = JobStatus{Status: NotScheduled}
	}
	m.numReducer = nReduce
	m.mu.Unlock()

	m.server()
	return &m
}
