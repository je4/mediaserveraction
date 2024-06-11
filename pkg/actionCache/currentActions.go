package actionCache

import (
	"emperror.dev/errors"
	"sync"
)

func NewCurrentActions() *CurrentActions {
	return &CurrentActions{
		Mutex:   sync.Mutex{},
		actions: map[string][]chan<- *ActionResult{},
	}
}

type CurrentActions struct {
	sync.Mutex
	actions map[string][]chan<- *ActionResult
}

func (ca *CurrentActions) HasAction(id string) bool {
	ca.Lock()
	defer ca.Unlock()
	_, ok := ca.actions[id]
	return ok
}

func (ca *CurrentActions) AddAction(id string) error {
	ca.Lock()
	defer ca.Unlock()
	if _, ok := ca.actions[id]; !ok {
		return errors.Errorf("action %s already running", id)
	}
	ca.actions[id] = []chan<- *ActionResult{}
	return nil
}

func (ca *CurrentActions) AddWaiter(id string, ch chan<- *ActionResult) error {
	ca.Lock()
	defer ca.Unlock()
	if _, ok := ca.actions[id]; !ok {
		return errors.Errorf("current action %s not found", id)
	}
	ca.actions[id] = append(ca.actions[id], ch)
	return nil
}

func (ca *CurrentActions) RemoveAction(id string) error {
	ca.Lock()
	defer ca.Unlock()
	delete(ca.actions, id)
	return nil
}

func (ca *CurrentActions) GetWaiters(id string) []chan<- *ActionResult {
	ca.Lock()
	defer func() {
		delete(ca.actions, id)
		ca.Unlock()
	}()
	if waiters, ok := ca.actions[id]; !ok {
		return nil
	} else {
		return waiters
	}
}
