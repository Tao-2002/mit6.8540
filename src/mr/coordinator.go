package mr

import (
	"fmt"
	"io/ioutil"
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

var mu sync.Mutex

type Task struct {
	FileNames []string  //输入文件
	TaskId    int       //任务id
	TaskState TaskState //任务状态(working,waiting,done)
	ReduceNum int       //需要用于Hash...
	ReduceIdx int       //指定reduce任务的Idx
	TaskType  TaskType  //任务类型 MapTask/ReduceTask/WaitingTask/ExitTask
	StartTime time.Time // 任务开始时间
}

type TaskArgs struct {
}

type TaskType int

//任务类型 TaskType
const (
	MapTask TaskType = iota
	ReduceTask
	WaittingTask
	ExitTask
)

type Phase int

//阶段类型 PhaseType
const (
	MapPhase Phase = iota
	ReducePhase
	DonePhase
)

type TaskState int

//任务状态类型
const (
	working TaskState = iota
	waiting
	done
)

type TaskMetaInfo struct {
	TaskAddr *Task
}
type TaskMetaHolder struct {
	MetaMap map[int]*TaskMetaInfo
}
type Coordinator struct {
	// Your definitions here.
	PhaseState     Phase //MapPhase,ReducePhase 阶段类型
	MapChan        chan *Task
	ReduceChan     chan *Task
	MapNum         int
	ReduceNum      int
	ReduceState    []bool
	TaskMetaHolder TaskMetaHolder
	files          []string
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}
func (c *Coordinator) judgeState(taskId int) bool {
	task, ok := c.TaskMetaHolder.MetaMap[taskId]
	if !ok || task.TaskAddr.TaskState != waiting {
		return false
	}
	task.TaskAddr.StartTime = time.Now()
	task.TaskAddr.TaskState = working
	return true
}

func (c *Coordinator) DistributeTask(args *TaskArgs, reply *Task) error {
	mu.Lock()
	defer mu.Unlock()
	switch c.PhaseState {
	case MapPhase:
		{
			if len(c.MapChan) > 0 {
				*reply = *<-c.MapChan
				if !c.judgeState(reply.TaskId) {
					// task 不处于 Waiting 状态
					fmt.Printf("Map-taskid[ %d ] is running\n", reply.TaskId)
				}
			} else {
				reply.TaskType = WaittingTask
				if c.TaskMetaHolder.CheckAll(MapPhase) {
					c.toNextPhase()
				}
				return nil
			}
		}
	case ReducePhase:
		{
			if len(c.ReduceChan) > 0 {
				*reply = *<-c.ReduceChan
				if !c.judgeState(reply.TaskId) {
					fmt.Println("Reduce-task is running", reply)
				}
			} else {
				reply.TaskType = WaittingTask
				if c.TaskMetaHolder.CheckAll(ReducePhase) {
					c.toNextPhase()
				}
				return nil
			}
		}
	case DonePhase:
		reply.TaskType = ExitTask
	default:
		fmt.Printf("undefined Phase...")
	}
	return nil
}
func (c *Coordinator) SetReduceIdx(args *Task, reply *int) error {
	mu.Lock()
	defer mu.Unlock()

	for i := 0; i < c.ReduceNum; i++ {
		if !c.ReduceState[i] { // 找到第一个未分配的索引
			c.ReduceState[i] = true // 标记为已分配
			args.ReduceIdx = i
			*reply = i // 返回索引
			return nil
		}
	}

	*reply = -1 // 如果没有可用的索引，返回 -1
	return nil
}

func (c *Coordinator) SetTaskDone(args *Task, reply *Task) error {
	mu.Lock()
	defer mu.Unlock()

	switch args.TaskType {
	case MapTask:
		{
			task, ok := c.TaskMetaHolder.MetaMap[args.TaskId]
			if ok && task.TaskAddr.TaskState == working {
				task.TaskAddr.TaskState = done
			} else {
				//fmt.Printf("[INFO] Map task Id[ %d ] is already finished.\n", args.TaskId)
			}
		}
	case ReduceTask:
		{
			task, ok := c.TaskMetaHolder.MetaMap[args.TaskId]
			if ok && task.TaskAddr.TaskState == working {
				task.TaskAddr.TaskState = done
			} else {
				//fmt.Printf("[INFO] Reduce task Id[ %d ] is already finished.\n", args.TaskId)
			}
		}
	default:
		panic("[ERROR] TaskType undefined.")
	}
	return nil
}

func (c *Coordinator) toNextPhase() {
	if c.PhaseState == MapPhase {
		c.MakeReduceTasks()
		c.PhaseState = ReducePhase
	} else if c.PhaseState == ReducePhase {
		c.PhaseState = DonePhase
	}
}

func (tmh *TaskMetaHolder) CheckAll(Phasestate Phase) bool {
	mapUndoneNum, mapDoneNum := 0, 0
	reduceUndoneNum, reduceDoneNum := 0, 0
	if Phasestate == MapPhase {
		for _, v := range tmh.MetaMap {
			if v.TaskAddr.TaskState == working {
				mapUndoneNum++
			} else if v.TaskAddr.TaskState == done {
				mapDoneNum++
			}
		}
	} else if Phasestate == ReducePhase {
		for _, v := range tmh.MetaMap {
			if v.TaskAddr.TaskState == working {
				reduceUndoneNum++
			} else if v.TaskAddr.TaskState == done {
				reduceDoneNum++
			}
		}
	}
	// map阶段的全部完成,reduce阶段的全部都还没开始
	if (mapDoneNum > 0 && mapUndoneNum == 0) && (reduceDoneNum == 0 && reduceUndoneNum == 0) {
		return true
	} else if reduceDoneNum > 0 && reduceUndoneNum == 0 {
		// reduce阶段的全部完成
		return true
	}
	return false
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

func (c *Coordinator) MakeMapTasks(files []string) {
	for i, file := range files {
		task := &Task{
			FileNames: []string{file},
			TaskId:    i,
			TaskState: waiting,
			ReduceNum: c.ReduceNum,
			TaskType:  MapTask,
		}
		c.MapChan <- task
		c.TaskMetaHolder.MetaMap[task.TaskId] = &TaskMetaInfo{TaskAddr: task}
	}
}

func (c *Coordinator) nextPhase() {
	if c.PhaseState == MapPhase {
		c.MakeReduceTasks()
		c.PhaseState = ReducePhase
	} else if c.PhaseState == ReducePhase {
		c.PhaseState = DonePhase
	}
}

func (c *Coordinator) MakeReduceTasks() {
	i := 0
	fileLen := len(c.TaskMetaHolder.MetaMap)
	for ; i < c.ReduceNum; i++ {
		task := &Task{
			FileNames: c.SelectReduceFile(i),
			TaskId:    i + fileLen,
			TaskState: waiting,
			ReduceNum: c.ReduceNum,
			ReduceIdx: i,
			TaskType:  ReduceTask,
		}
		c.ReduceChan <- task
		c.TaskMetaHolder.MetaMap[task.TaskId] = &TaskMetaInfo{TaskAddr: task}
	}
}

func (c *Coordinator) SelectReduceFile(reduceIdx int) []string {
	var s []string
	path, _ := os.Getwd()
	files, _ := ioutil.ReadDir(path)
	for _, fi := range files {
		if strings.HasPrefix(fi.Name(), "mr-tmp") && strings.HasSuffix(fi.Name(), strconv.Itoa(reduceIdx)) {
			s = append(s, fi.Name())
		}
	}
	return s
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// Your code here.
	c := Coordinator{
		PhaseState:     MapPhase,
		MapNum:         len(files),
		MapChan:        make(chan *Task, len(files)),
		ReduceChan:     make(chan *Task, nReduce),
		ReduceNum:      nReduce,
		ReduceState:    make([]bool, nReduce),
		TaskMetaHolder: TaskMetaHolder{MetaMap: make(map[int]*TaskMetaInfo)},
		files:          files,
	}
	c.MakeMapTasks(files)

	c.server()
	go c.CrashDetector()

	return &c
}

func (c *Coordinator) CrashDetector() {
	for {
		//fmt.Println("in CrashDetector ...")
		time.Sleep(2 * time.Second)
		mu.Lock()
		if c.PhaseState == DonePhase {
			mu.Unlock()
			break
		}
		for _, task := range c.TaskMetaHolder.MetaMap {
			if task.TaskAddr.TaskState == working && time.Since(task.TaskAddr.StartTime) > 10*time.Second {
				switch task.TaskAddr.TaskType {
				case MapTask:
					{
						task.TaskAddr.TaskState = waiting
						c.MapChan <- task.TaskAddr
					}
				case ReduceTask:
					{
						task.TaskAddr.TaskState = waiting
						c.ReduceChan <- task.TaskAddr
						//c.ReduceState[task.TaskAddr.ReduceIdx] = false
					}
				}
			}
		}
		mu.Unlock()
	}
}
