package kvsrv

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ReqId int64
}

type PutAppendReply struct {
	Value string
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	//ClientId  int64
	ReqId int64
}

type GetReply struct {
	Value string
}

type ReportArgs struct {
	Key string
	//ClientId  int64
	ReqId int64
}
type ReportReply struct {
}
