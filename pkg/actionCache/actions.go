package actionCache

import (
	"context"
	"crypto/sha1"
	"emperror.dev/errors"
	"fmt"
	"github.com/google/uuid"
	mediaserverproto "github.com/je4/mediaserverproto/v2/pkg/mediaserver/proto"
	"github.com/je4/utils/v2/pkg/zLogger"
	"sync"
	"time"
)

func NewActions(mediaType string, action []string, logger zLogger.ZLogger) *Actions {
	return &Actions{
		client:         map[string]*ClientEntry{},
		mediaType:      mediaType,
		action:         action,
		actionJobChan:  make(chan *ActionJob),
		currentActions: map[string][]chan<- *ActionResult{},
		currentMutex:   sync.Mutex{},
		logger:         logger,
	}
}

type ActionResult struct {
	err    error
	result *mediaserverproto.Cache
}

type ActionJob struct {
	id         string
	ap         *mediaserverproto.ActionParam
	resultChan chan<- *ActionResult
}

func (aj *ActionJob) String() string {
	item := aj.ap.GetItem()
	return fmt.Sprintf("ActionJob{id: %s, action: %s/%s/%s/%s}", aj.id, item.GetIdentifier().GetCollection(), item.GetIdentifier().GetSignature(), aj.ap.GetAction(), ActionParams(aj.ap.GetParams()).String())
}

type Actions struct {
	client         map[string]*ClientEntry
	mediaType      string
	action         []string
	actionJobChan  chan *ActionJob
	currentActions map[string][]chan<- *ActionResult
	currentMutex   sync.Mutex
	logger         zLogger.ZLogger
}

func (a *Actions) AddClient(name string, client *ClientEntry) {
	a.client[name] = client
	client.setJobChannel(a.actionJobChan)
}

func (a *Actions) Action(ap *mediaserverproto.ActionParam, actionTimeout time.Duration) (*mediaserverproto.Cache, error) {
	item := ap.GetItem()
	var params ActionParams = ap.GetParams()
	actionStr := fmt.Sprintf("%s/%s/%s/%s", item.GetIdentifier().GetCollection(), item.GetIdentifier().GetSignature(), ap.GetAction(), params.String())
	actionIDBytes := sha1.Sum([]byte(actionStr))
	actionID := fmt.Sprintf("%x", actionIDBytes)
	a.currentMutex.Lock()
	if _, ok := a.currentActions[actionID]; ok {
		a.logger.Debug().Msgf("action %s already running - waiting", actionStr)
		waitFor := make(chan *ActionResult)
		a.currentActions[actionID] = append(a.currentActions[actionID], waitFor)
		a.currentMutex.Unlock()
		defer func() {
			close(waitFor)
		}()
		select {
		case <-time.After(actionTimeout):
			a.logger.Debug().Msgf("running action %s timed out", actionStr)
			return nil, errors.Errorf("action end timeout for %s/%s/%s/%s", item.GetIdentifier().GetCollection(), item.GetIdentifier().GetSignature(), ap.GetAction(), params.String())
		case result := <-waitFor:
			a.logger.Debug().Msgf("running action %s finished", actionStr)
			if result.err != nil {
				return nil, errors.Wrapf(result.err, "action %s/%s/%s/%s failed", item.GetIdentifier().GetCollection(), item.GetIdentifier().GetSignature(), ap.GetAction(), params.String())
			}
			return result.result, nil
		}
	}
	a.currentMutex.Unlock()
	resultChan := make(chan *ActionResult)
	a.logger.Debug().Msgf("running action %s", actionStr)
	select {
	case a.actionJobChan <- &ActionJob{
		id:         uuid.NewString(),
		ap:         ap,
		resultChan: resultChan,
	}:
		a.currentMutex.Lock()
		a.currentActions[actionID] = []chan<- *ActionResult{}
		a.currentMutex.Unlock()
	case <-time.After(actionTimeout):
		return nil, errors.Errorf("action start timeout for  %s/%s/%s/%s", item.GetIdentifier().GetCollection(), item.GetIdentifier().GetSignature(), ap.GetAction(), params.String())
	}
	select {
	case <-time.After(actionTimeout):
		return nil, errors.Errorf("action end timeout for %s/%s/%s/%s", item.GetIdentifier().GetCollection(), item.GetIdentifier().GetSignature(), ap.GetAction(), params.String())
	case result := <-resultChan:
		a.currentMutex.Lock()
		channels, ok := a.currentActions[actionID]
		a.currentMutex.Unlock()
		defer func() {
			a.currentMutex.Lock()
			delete(a.currentActions, actionID)
			a.currentMutex.Unlock()
		}()
		if ok {
			for _, c := range channels {
				c <- result
			}
		}
		if result.err != nil {
			return nil, errors.Wrapf(result.err, "action %s/%s/%s/%s failed", item.GetIdentifier().GetCollection(), item.GetIdentifier().GetSignature(), ap.GetAction(), params.String())
		}
		return result.result, nil
	}
}

func (a *Actions) GetClient(name string) (*ClientEntry, bool) {
	client, ok := a.client[name]
	return client, ok
}

func (a *Actions) GetParams(action string) ([]string, error) {
	for address, client := range a.client {
		resp, err := client.client.GetParams(context.Background(), &mediaserverproto.ParamsParam{
			Type:   a.mediaType,
			Action: action,
		})
		if err != nil {
			return nil, errors.Wrapf(err, "cannot get params for %s::%s from %s", a.mediaType, action, address)
		}
		return resp.GetValues(), nil
	}
	return nil, errors.Errorf("no client found for %s::%s", a.mediaType, action)
}

func (a *Actions) RemoveClient(name string) {
	delete(a.client, name)
}
