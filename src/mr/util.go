package mr

type TaskType int

const (
	Map = iota
	Reduce
)

type CoordinatorPhase TaskType

type Task struct {
	Type     TaskType
	Id       int
	Filename string
}
