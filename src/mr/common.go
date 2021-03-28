package mr

import "fmt"

type TaskPhase int

const (
	MapPhase    TaskPhase = 0
	ReducePhase TaskPhase = 1
)

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