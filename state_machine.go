package grafting

import (
	"github.com/nu7hatch/gouuid"
)

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
	Uid     *uuid.UUID
	Command StateMachineCommand
}

type StateMachineCommandResponse struct {
	message
	Uid         *uuid.UUID
	ReturnValue interface{}
}
