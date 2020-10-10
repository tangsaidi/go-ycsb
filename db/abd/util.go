package abd

import (
	"bufio"
	"bytes"
	"encoding/gob"
	"hash/fnv"
	"sync"
	"time"
)

const CHAN_BUFFER_SIZE = 10000

func Hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

func MakeTimestamp() int64 {
	return time.Now().UnixNano() / int64(time.Millisecond)
}

//send a single object
func SendObject(writer *bufio.Writer, object interface{}) {
	var bbuf bytes.Buffer
	var byteMsg []byte
	encoder := gob.NewEncoder(&bbuf)
	encoder.Encode(object)
	byteMsg = bbuf.Bytes()
	writer.Write(byteMsg)
	writer.Flush()
}

//write a concurrent map
var lock sync.Mutex

func SyncSendObject(writer *bufio.Writer, object interface{}) {
	lock.Lock()
	defer lock.Unlock()
	var bbuf bytes.Buffer
	var byteMsg []byte
	encoder := gob.NewEncoder(&bbuf)
	encoder.Encode(object)
	byteMsg = bbuf.Bytes()
	writer.Write(byteMsg)
	writer.Flush()
}
