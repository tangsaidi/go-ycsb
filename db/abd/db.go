package abd

import (
	"github.com/pingcap/go-ycsb/pkg/ycsb"
)

type abdCreator struct {
}

func init() {
	ycsb.RegisterDBCreator("abd", abdCreator{})
}
