package paxos

type PrepareR struct {
	Num int
}

type PrepareAck struct {
	AckNum int // -1 表示拒绝
	Value  interface{}
}

type AcceptR struct {
	Num   int
	Value interface{}
}

type AcceptAck struct {
	AckNum int32
}




