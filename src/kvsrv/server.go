package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	data   map[string]string
	reqIDs sync.Map
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.Value = kv.data[args.Key]
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	v, ok := kv.reqIDs.Load(args.ReqId)
	//println("in put:--------:v=", v)
	if ok {
		reply.Value = v.(string)
		return
	}
	old := kv.data[args.Key]
	if old != args.Value {
		reply.Value = old
		kv.data[args.Key] = args.Value
	}
	kv.reqIDs.Store(args.ReqId, old)

}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	v, ok := kv.reqIDs.Load(args.ReqId)
	//println("in Append:--------:v=", v)

	if ok {
		reply.Value = v.(string)
		return
	}
	old := kv.data[args.Key]
	if old == "" {
		kv.data[args.Key] = args.Value
	} else {
		kv.data[args.Key] = old + args.Value
	}
	reply.Value = old
	//if old != args.Value {
	//	reply.Value = old
	//	kv.data[args.Key] += args.Value
	//}
	kv.reqIDs.Store(args.ReqId, old)
}

func (kv *KVServer) Report(args *ReportArgs, reply *ReportReply) {
	kv.reqIDs.Delete(args.ReqId)
	return
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.data = make(map[string]string, 20)
	return kv
}
