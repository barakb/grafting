package grafting

type StateMachine map[string]interface{}

func NewStateMachine() StateMachine {
	return make(StateMachine)
}

type StateMachineCommand interface {
	Execute(sm StateMachine) interface{}
}

type SetValue struct {
	Key   string
	Value interface{}
}

func (v SetValue) Execute(sm StateMachine) interface{} {
	old := sm[v.Key]
	sm[v.Key] = v.Value
	return old
}

type StateMachineCommandRequest struct {
	message
	Command StateMachineCommand
}
