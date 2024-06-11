package actionCache

import (
	"context"
	"crypto/sha1"
	"emperror.dev/errors"
	"fmt"
	"github.com/google/uuid"
	mediaserverproto "github.com/je4/mediaserverproto/v2/pkg/mediaserver/proto"
	"github.com/je4/utils/v2/pkg/zLogger"
	"time"
)

func actionID(action string) string {
	actionIDBytes := sha1.Sum([]byte(action))
	return fmt.Sprintf("%x", actionIDBytes)
}

func actionStr(collection, signature, action string, params ActionParams) string {
	return fmt.Sprintf("%s/%s/%s/%s", collection, signature, action, params.String())
}

func NewActions(mediaType string, action []string, logger zLogger.ZLogger) *Actions {
	return &Actions{
		client:         map[string]*ClientEntry{},
		mediaType:      mediaType,
		action:         action,
		actionJobChan:  make(chan *ActionJob),
		currentActions: NewCurrentActions(),
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
	currentActions *CurrentActions
	logger         zLogger.ZLogger
}

func (a *Actions) AddClient(name string, client *ClientEntry) {
	a.client[name] = client
	client.setJobChannel(a.actionJobChan)
}

func (a *Actions) Action(ap *mediaserverproto.ActionParam, actionTimeout time.Duration) (*mediaserverproto.Cache, error) {
	item := ap.GetItem()
	var params ActionParams = ap.GetParams()
	actionString := actionStr(item.GetIdentifier().GetCollection(), item.GetIdentifier().GetSignature(), ap.GetAction(), params)
	id := actionID(actionString)
	if a.currentActions.HasAction(id) {
		a.logger.Debug().Msgf("action %s already running - waiting", actionString)
		waitFor := make(chan *ActionResult)
		a.currentActions.AddWaiter(id, waitFor)
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
	resultChan := make(chan *ActionResult)
	a.logger.Debug().Msgf("running action %s", actionStr)
	select {
	case a.actionJobChan <- &ActionJob{
		id:         uuid.NewString(),
		ap:         ap,
		resultChan: resultChan,
	}:
		a.currentActions.AddAction(id)
	case <-time.After(actionTimeout):
		return nil, errors.Errorf("action start timeout for  %s/%s/%s/%s", item.GetIdentifier().GetCollection(), item.GetIdentifier().GetSignature(), ap.GetAction(), params.String())
	}
	select {
	case <-time.After(actionTimeout):
		return nil, errors.Errorf("action end timeout for %s/%s/%s/%s", item.GetIdentifier().GetCollection(), item.GetIdentifier().GetSignature(), ap.GetAction(), params.String())
	case result := <-resultChan:
		channels := a.currentActions.GetWaiters(id)
		for _, c := range channels {
			select {
			case c <- result:
			default:
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
