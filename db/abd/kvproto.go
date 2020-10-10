package abd

import (
	"fmt"
)

//import "fmt"

type Operation uint8

const (
	NONE Operation = iota
	GET
	PUT
)

var NIL Value
var DUMMYVAL = Value(0)

type Key int64
type Value int64

type Version struct {
	ServerId int32
	ThreadId int32
	Ts       int64
	R        int32 //random value to avoid collision
}

type VersionGenerator struct {
	Counter         int32
	WrapAroundRange int32
}

func (vg *VersionGenerator) GenerateVersion(serverId int32, threadId int32) *Version {
	txnVs := Version{
		ServerId: serverId,
		ThreadId: threadId,
		Ts:       MakeTimestamp(),
		R:        vg.Counter,
	}
	vg.Counter++
	if vg.WrapAroundRange > vg.WrapAroundRange {
		vg.Counter = 0
	}
	return &txnVs
}

//minimum version
var DUMMYVS = Version{
	ServerId: 0,
	ThreadId: 0,
	Ts:       0,
	R:        0,
}

type Command struct {
	Op  Operation
	K   Key
	Val Value
}

type Transaction struct {
	Commands []Command
	Ts       int64
}

type TransactionResp struct {
	Vals    []int64
	QueLat  int64
	ProcLat int64
	ExecLat int64
}

func (lhs *Version) LargerThan(rhs *Version) bool {
	l1 := lhs.Ts == rhs.Ts
	l2 := lhs.R == rhs.R
	l3 := lhs.ServerId == rhs.ServerId

	t1 := lhs.Ts > rhs.Ts
	t2 := l1 && lhs.R > rhs.R
	t3 := l1 && l2 && lhs.ServerId > rhs.ServerId
	t4 := l1 && l2 && l3 && lhs.ThreadId > lhs.ThreadId

	return t1 || t2 || t3 || t4
}

func (lhs *Version) Equal(rhs *Version) bool {
	return lhs.Ts == rhs.Ts && lhs.R == rhs.R && lhs.ServerId == rhs.ServerId && lhs.ThreadId == lhs.ThreadId
}

//Copy what on right hand side to left hand side
func (lhs *Version) Copy(rhs *Version) {
	lhs.ServerId = rhs.ServerId
	lhs.ThreadId = rhs.ThreadId
	lhs.Ts = rhs.Ts
	lhs.R = rhs.R
}

func (lhs *Version) Print() {
	Printf("sId: %d, tId: %d, ts: %d, R: %d", lhs.ServerId, lhs.ThreadId, lhs.Ts, lhs.R)
}

func (lhs *Version) StrPrint() string {
	return fmt.Sprintf("sId: %d, tId: %d, ts: %d, R: %d", lhs.ServerId, lhs.ThreadId, lhs.Ts, lhs.R)
}
