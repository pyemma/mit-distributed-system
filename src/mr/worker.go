package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	workerId := RegisterWorkerId()
	fmt.Println("My worker id is ", workerId)

	// uncomment to send the Example RPC to the master.
	for {
		intermediate := []string{}
		reply := GetJob(workerId)
		numReducer := reply.NumReducer
		switch reply.JobType {
		case AllDone:
			fmt.Println("Worker ", workerId, " exits as all job id done")
			return
		case None:
			time.Sleep(time.Second * 2)
			continue
		case Map:
			filename := reply.Filenames[0]
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open %v", filename)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", filename)
			}
			file.Close()
			kva := mapf(filename, string(content))
			fmt.Println("Total parsed key %d", len(kva))

			// shard the kv into nReducer splits
			kvSharded := make(map[int][]KeyValue)
			for _, kv := range kva {
				shard := ihash(kv.Key) % numReducer
				kvSharded[shard] = append(kvSharded[shard], kv)
			}
			// Flash the result to files
			for shard, kva := range kvSharded {
				tempfile, err := ioutil.TempFile("", "mr-tempfile")
				if err != nil {
					log.Fatalf("cannot create tempfile")
				}

				enc := json.NewEncoder(tempfile)
				err = enc.Encode(&kva)
				if err != nil {
					log.Fatalf("fail to write to tempfile")
				}
				tempfile.Close()
				interFilename := "mr-" + strconv.Itoa(reply.JobId) + "-" + strconv.Itoa(shard)
				os.Rename(tempfile.Name(), interFilename)
				intermediate = append(intermediate, interFilename)

			}
			FinishJob(workerId, reply.JobType, reply.JobId, intermediate)
		case Reduce:
			// Read values from all intermediate file
			fmt.Println("Receive intermediate file ", reply.Filenames)
			kva := make([]KeyValue, 0)
			for _, filename := range reply.Filenames {
				file, err := os.Open(filename)
				if err != nil {
					log.Fatalf("cannot open intermediate file %v", filename)
				}
				dec := json.NewDecoder(file)
				kvs := make([]KeyValue, 0)
				if err := dec.Decode(&kvs); err != nil {
					log.Fatalf("fail to read intermediate file")
				}
				kva = append(kva, kvs...)
			}

			sort.Sort(ByKey(kva))

			oname := "mr-out-" + strconv.Itoa(reply.JobId)
			tempfile, err := ioutil.TempFile("", "mr-tempfile")
			if err != nil {
				log.Fatalf("cannot create tempfile")
			}
			i := 0
			for i < len(kva) {
				j := i + 1
				for j < len(kva) && kva[j].Key == kva[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, kva[k].Value)
				}
				output := reducef(kva[i].Key, values)

				fmt.Fprintf(tempfile, "%v %v\n", kva[i].Key, output)

				i = j
			}
			tempfile.Close()
			os.Rename(tempfile.Name(), oname)
			intermediate = append(intermediate, oname)

			FinishJob(workerId, reply.JobType, reply.JobId, intermediate)
		}

	}
	fmt.Println("Worker ", workerId, " exits as all job id done")
}

// RegisterWorkerId makes a RPC call to master to register a id
func RegisterWorkerId() int {
	args := RegisterWorkerIdArgs{}
	args.Msg = "Register a worker id on master"

	reply := RegisterWorkerIdReply{}
	call("Master.RegisterWorkerId", &args, &reply)
	return reply.WorkerId
}

// GetJob makes a RPC call to master to request a job.
func GetJob(workerId int) GetJobReply {
	args := GetJobArgs{}
	args.Msg = "This is from a worker " + strconv.Itoa(workerId)
	args.WorkerId = workerId

	reply := GetJobReply{}
	call("Master.GetJob", &args, &reply)
	fmt.Println(reply.JobType)
	fmt.Println(reply.Filenames)
	return reply
}

// FinishJob makes a RPC call to master to notify that a job is done
func FinishJob(workerId int, jobType string, jobId int, filenames []string) {
	args := FinishJobArgs{}
	args.Filenames = filenames
	args.JobType = jobType
	args.JobId = jobId
	args.WorkerId = workerId

	reply := FinishJobReply{}
	call("Master.FinishJob", &args, &reply)
	// fmt.Println(reply.Msg)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
