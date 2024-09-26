package mr

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

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
	alive := true
	for alive {
		task := GetTask()
		switch task.TaskType {
		case MapTask:
			{
				DealMapTask(&task, mapf)
				CallDone(&task)
			}
		case ReduceTask:
			{
				//reduceIdx := CallReduceIdx(&task)
				DealReduceTask(&task, reducef)
				CallDone(&task)
			}
		case WaittingTask:
			{
				time.Sleep(time.Second)
				//fmt.Println("In waitingTask ...")
			}
		case ExitTask:
			{
				//fmt.Println("[INFO] exit")
				alive = false
			}
		}
	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func CallReduceIdx(task *Task) int {
	args := task
	var reply int
	ok := call("Coordinator.SetReduceIdx", &args, &reply)
	if ok {
		//fmt.Printf("[INFO] GetReduceIdx:%d ", reply)
	} else {
		fmt.Println("CallReduceIdx failed")
	}
	return reply
}

func CallReleaseReduceIdx(task Task) {

}

func GetTask() Task {
	args := TaskArgs{}
	reply := Task{}
	ok := call("Coordinator.DistributeTask", &args, &reply)
	if !ok {
		fmt.Printf("[error]call failed\n")
	}
	return reply
}

func CallDone(finishedTask *Task) Task {
	args := finishedTask
	reply := Task{}
	ok := call("Coordinator.SetTaskDone", &args, &reply)
	if ok {
		//fmt.Println("[INFO] CallDone finish: ", finishedTask)
	} else {
		fmt.Println("callDone failed")
	}
	return reply
}

func DealMapTask(task *Task, mapf func(string, string) []KeyValue) {
	intermediate := []KeyValue{}
	filename := task.FileNames[0]
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	defer file.Close()
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}

	intermediate = mapf(filename, string(content))

	Hashkva := make([][]KeyValue, task.ReduceNum)
	for _, kv := range intermediate {
		hIndex := ihash(kv.Key) % task.ReduceNum
		Hashkva[hIndex] = append(Hashkva[hIndex], kv)
	}

	for i := 0; i < task.ReduceNum; i++ {
		oname := fmt.Sprintf("mr-tmp-%d-%d", task.TaskId, i)
		ofile, _ := os.Create(oname)

		enc := json.NewEncoder(ofile)
		for _, kv := range Hashkva[i] {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalf("encode failed:%v", err)
			}
		}
		ofile.Close()
	}

}

func shuffle(filenames []string) []KeyValue {
	kva := []KeyValue{}
	for _, filename := range filenames {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatal("can not open file %v", filename)
		}

		dec := json.NewDecoder(file)
		for {
			kv := KeyValue{}
			if err := dec.Decode(&kv); err != nil {
				if err == io.EOF {
					break
				}
				log.Fatalf("decode error:%v", err)
			}
			kva = append(kva, kv)
		}
		file.Close()

	}
	sort.Sort(ByKey(kva))
	return kva
}

func getIntermediateFilenames(reduceIdx int) []string {
	pattern := fmt.Sprintf("mr-*-%d", reduceIdx)
	filenames, err := filepath.Glob(pattern)
	if err != nil {
		log.Fatalf("error getting files:%v", err)
	}
	return filenames
}

func DealReduceTask(task *Task, reducef func(string, []string) string) {
	reducefileName := task.TaskId
	intermediate := shuffle(task.FileNames)

	//先命名临时的文件
	dir, _ := os.Getwd()
	//mapperNum := len(task.FileNames)
	//reduceTaskIdx := task.TaskId - mapperNum
	//tname := fmt.Sprintf("mr-tmp-%v", task.ReduceIdx)
	tempFile, err := ioutil.TempFile(dir, "mr-tmp-*")
	if err != nil {
		log.Fatal("[ERROR] Failed to create temp file", err)
	}

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	tempFile.Close()

	//完全写入后，再重命名
	fn := fmt.Sprintf("mr-out-%d", reducefileName)
	os.Rename(tempFile.Name(), fn)
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
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
