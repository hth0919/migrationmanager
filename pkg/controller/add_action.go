package controller

import (
	"github.com/hth0919/migrationmanager/pkg/controller/action"
)

func init() {
	// AddToManagerFuncs is a list of functions to create controllers and add them to a manager.
	AddToManagerFuncs = append(AddToManagerFuncs, action.Add)
}
