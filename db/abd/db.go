package abd

import (
	"bufio"
	"context"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"runtime"
	"sort"
	"strings"

	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
)

type abdCreator struct {
}

type kvTuple struct {
	key   string
	value map[string][]byte
}

func zip(keys []string, values []map[string][]byte) ([]kvTuple, error) {
	if len(keys) != len(values) {
		return nil, fmt.Errorf("zip: arguments must be of same length")
	}

	r := make([]kvTuple, len(keys), len(values))

	for i, e := range keys {
		r[i] = kvTuple{e, values[i]}
	}

	return r, nil
}

func init() {
	ycsb.RegisterDBCreator("abd", abdCreator{})
}

type abd struct {
	// reader *bufio.Reader
	// writer *bufio.Writer
	port int
	// mu   sync.Mutex
}

func (r *abd) Close() error {
	return nil
	// return r.client.Close()
}

func (r *abd) InitThread(ctx context.Context, _ int, _ int) context.Context {
	server, err := net.Dial("tcp", fmt.Sprintf(":%d", r.port))
	if err != nil {
		log.Printf("Error connecting to replica \n")
	}
	conn := &server

	ctx = context.WithValue(ctx, "reader", bufio.NewReader(*conn))
	ctx = context.WithValue(ctx, "writer", bufio.NewWriter(*conn))
	return ctx
}

func (r *abd) CleanupThread(_ context.Context) {
}

func (r *abd) Read(ctx context.Context, table string, key string, fields []string) (map[string][]byte, error) {
	k := Key(key)
	cmd := Command{GET, k, "default"}
	var txn Transaction
	txn.Commands = append(txn.Commands, cmd)
	txn.Ts = MakeTimestamp()
	SendObject(ctx.Value("writer").(*bufio.Writer), txn)
	gobReader := gob.NewDecoder(ctx.Value("reader").(*bufio.Reader))
	var response []Value

	if err := gobReader.Decode(&response); err != nil {
		fmt.Println("Error when reading:", err)
	}
	return nil, nil // we don't care about the return value for benchmark purposes
}

func (r *abd) Scan(ctx context.Context, table string, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	return nil, fmt.Errorf("scan is not supported")
}

func (r *abd) Update(ctx context.Context, table string, key string, values map[string][]byte) error {
	return fmt.Errorf("update is not supported")
}

func (r *abd) Insert(ctx context.Context, table string, key string, values map[string][]byte) error {
	data, err := json.Marshal(values)
	if err != nil {
		return err
	}

	k := Key(key)
	cmd := Command{PUT, k, Value(data)}
	var txn Transaction
	txn.Commands = append(txn.Commands, cmd)
	txn.Ts = MakeTimestamp()
	SendObject(ctx.Value("writer").(*bufio.Writer), txn)
	gobReader := gob.NewDecoder(ctx.Value("reader").(*bufio.Reader))
	var response []Value

	if err := gobReader.Decode(&response); err != nil {
		fmt.Println("Error when inserting:", err)
	}

	return nil
}

func (r *abd) BatchInsert(ctx context.Context, table string, keys []string, values []map[string][]byte) error {
	var txn Transaction
	combined, err := zip(keys, values)
	if err != nil {
		return err
	}
	sort.Slice(combined, func(i, j int) bool {
		switch strings.Compare(combined[i].key, combined[j].key) {
		case -1:
			return true
		case 1:
			return false
		}
		return true
	})

	for _, key := range combined {
		data, err := json.Marshal(key.value)
		if err != nil {
			return err
		}

		k := Key(key.key)
		cmd := Command{PUT, k, Value(data)}
		txn.Commands = append(txn.Commands, cmd)
	}

	txn.Ts = MakeTimestamp()

	SendObject(ctx.Value("writer").(*bufio.Writer), txn)

	gobReader := gob.NewDecoder(ctx.Value("reader").(*bufio.Reader))
	var response []Value
	if err := gobReader.Decode(&response); err != nil {
		fmt.Println("Error when inserting:", err)
	}
	return nil
}

func (r *abd) BatchRead(ctx context.Context, table string, keys []string, fields []string) ([]map[string][]byte, error) {
	var txn Transaction

	sort.Slice(keys, func(i, j int) bool {
		switch strings.Compare(keys[i], keys[j]) {
		case -1:
			return true
		case 1:
			return false
		}
		return true
	})

	for _, key := range keys {
		k := Key(key)
		cmd := Command{GET, k, "default"}
		txn.Commands = append(txn.Commands, cmd)
	}

	txn.Ts = MakeTimestamp()
	SendObject(ctx.Value("writer").(*bufio.Writer), txn)

	gobReader := gob.NewDecoder(ctx.Value("reader").(*bufio.Reader))
	var response []Value
	if err := gobReader.Decode(&response); err != nil {
		fmt.Println("Error when reading:", err)
	}
	return nil, nil
}

func (r *abd) Delete(ctx context.Context, table string, key string) error {
	return fmt.Errorf("delete is not supported")
}

func (db *abd) BatchUpdate(ctx context.Context, table string, keys []string, values []map[string][]byte) error {
	panic("The abdDB has not implemented the batch operation")
}

func (db *abd) BatchDelete(ctx context.Context, table string, keys []string) error {
	panic("The abdDB has not implemented the batch operation")
}

func (r abdCreator) Create(p *properties.Properties) (ycsb.DB, error) {
	abdClient := &abd{}
	procs := p.GetInt(maxProcs, 2)
	runtime.GOMAXPROCS(procs)

	abdClient.port = p.GetInt(serverPort, 7070)
	// server, err := net.Dial("tcp", fmt.Sprintf(":%d", port))
	// if err != nil {
	// 	log.Printf("Error connecting to replica \n")
	// }
	// abdClient.conn = &server

	// abdClient.reader = bufio.NewReader(server)
	// abdClient.writer = bufio.NewWriter(server)

	return abdClient, nil
}

const (
	serverPort = "abd.server_port"
	maxProcs   = "abd.max_procs" // GOMAXPROCS
	// batchSize  = "abd.batchsize"
	// masterAddr = "abd.master_addr"
	// masterPort = "abd.master_port"
	// onceCheck  = "abd.once_check" // Check that every expected reply was receiving exactly once
	// conflicts  = "abd.conflict"   // Percentage of conflicts
	// read       = "abd.read"       // Percentage of read
)
