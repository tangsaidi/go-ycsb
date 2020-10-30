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

	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
)

type abdCreator struct {
}

func init() {
	ycsb.RegisterDBCreator("abd", abdCreator{})
}

type abd struct {
	reader *bufio.Reader
	writer *bufio.Writer
}

func (r *abd) Close() error {
	return nil
	// return r.client.Close()
}

func (r *abd) InitThread(ctx context.Context, _ int, _ int) context.Context {
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
	SendObject(r.writer, txn)
	gobReader := gob.NewDecoder(r.reader)
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
	SendObject(r.writer, txn)

	gobReader := gob.NewDecoder(r.reader)
	var response []Value
	if err := gobReader.Decode(&response); err != nil {
		fmt.Println("Error when inserting:", err)
	}

	return nil
}

func (r *abd) BatchInsert(ctx context.Context, table string, keys []string, values []map[string][]byte) error {
	var txn Transaction
	for i, key := range keys {
		data, err := json.Marshal(values[i])
		if err != nil {
			return err
		}

		k := Key(key)
		cmd := Command{PUT, k, Value(data)}
		txn.Commands = append(txn.Commands, cmd)
	}

	txn.Ts = MakeTimestamp()
	SendObject(r.writer, txn)

	gobReader := gob.NewDecoder(r.reader)
	var response []Value
	if err := gobReader.Decode(&response); err != nil {
		fmt.Println("Error when inserting:", err)
	}
	return nil
}

func (r *abd) BatchRead(ctx context.Context, table string, keys []string, fields []string) ([]map[string][]byte, error) {
	var txn Transaction
	for _, key := range keys {
		k := Key(key)
		cmd := Command{GET, k, "default"}
		txn.Commands = append(txn.Commands, cmd)
	}

	txn.Ts = MakeTimestamp()
	SendObject(r.writer, txn)

	gobReader := gob.NewDecoder(r.reader)
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
	port := p.GetInt(serverPort, 7070)
	server, err := net.Dial("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		log.Printf("Error connecting to replica \n")
	}
	abdClient.reader = bufio.NewReader(server)
	abdClient.writer = bufio.NewWriter(server)

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
