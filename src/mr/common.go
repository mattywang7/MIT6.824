package mr

import (
	"fmt"
	"log"
)

type TaskPhase int

const (
	MapPhase    TaskPhase = 0
	ReducePhase TaskPhase = 1
)

const Debug = false

func DPrintf(format string, v ...interface{}) {
	if Debug {
		log.Printf(format + "\n", v...)
	}
}

type Task struct {
	FileName string
	NReduce  int
	NMap     int
	Seq      int
	Phase    TaskPhase
	Alive    bool // worker will exit when Alive becomes false
}

func reduceName(mapId, reduceId int) string {
	return fmt.Sprintf("mr-%d-%d", mapId, reduceId)
}

func mergeName(reduceId int) string {
	return fmt.Sprintf("mr-out-%d", reduceId)
}