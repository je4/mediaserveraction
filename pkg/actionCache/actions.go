package actionCache

import (
	"context"
	"crypto/sha1"
	"emperror.dev/errors"
	"fmt"
	"github.com/google/uuid"
	actionParams "github.com/je4/mediaserverhelper/v2/pkg/actionParams"
	mediaserverproto "github.com/je4/mediaserverproto/v2/pkg/mediaserver/proto"
	"github.com/je4/utils/v2/pkg/zLogger"
	"golang.org/x/exp/maps"
	"google.golang.org/grpc/metadata"
	"time"
)

func actionID(action string) string {
	actionIDBytes := sha1.Sum([]byte(action))
	return fmt.Sprintf("%x", actionIDBytes)
}

func actionStr(collection, signature, action string, params actionParams.ActionParams) string {
	return fmt.Sprintf("%s/%s/%s/%s", collection, signature, action, params.String())
}

func NewActions(actions map[string][]string, logger zLogger.ZLogger) *Actions {
	l0 := logger.With().Strs("mediaTypes", maps.Keys(actions)).Logger()
	return &Actions{
		done:           make(chan bool),
		client:         map[string]*ClientEntry{},
		actions:        actions,
		actionBuffer:   NewQueue[*ActionJob](0, &l0),
		currentActions: NewCurrentActions(),
		logger:         &l0,
	}
}

type ActionResult struct {
	err    error
	result *mediaserverproto.Cache
}

type ActionJob struct {
	id         string
	ap         *mediaserverproto.ActionParam
	domain     string
	resultChan chan<- *ActionResult
}

func (aj *ActionJob) String() string {
	item := aj.ap.GetItem()
	return fmt.Sprintf("ActionJob{id: %s, action: %s/%s/%s/%s}", aj.id, item.GetIdentifier().GetCollection(), item.GetIdentifier().GetSignature(), aj.ap.GetAction(), actionParams.ActionParams(aj.ap.GetParams()).String())
}

type Actions struct {
	client  map[string]*ClientEntry
	actions map[string][]string
	//actionJobChan  chan *ActionJob
	currentActions *CurrentActions
	logger         zLogger.ZLogger
	actionBuffer   *Queue[*ActionJob]
	done           chan bool
}

func (a *Actions) Start() error {
	a.logger.Debug().Msg("starting actions")
	a.actionBuffer.Start()
	go func() {
		for {
			select {
			case <-time.After(1 * time.Second):
				now := time.Now()
				for _, client := range a.client {
					if client.clientTimeout.Before(now) {
						if err := a.RemoveClient(client.name); err != nil {
							a.logger.Error().Err(err).Msgf("cannot remove client %s", client.name)
						}
					}
				}
			case <-a.done:
				a.logger.Debug().Msg("client timeout check stopped")
				return
			}
		}
	}()
	return nil
}

func (a *Actions) Stop() error {
	var errs = []error{}
	a.done <- true
	a.logger.Debug().Msg("stopping actions")
	a.actionBuffer.Stop()
	for _, client := range a.client {
		if err := a.RemoveClient(client.name); err != nil {
			errs = append(errs, errors.Wrapf(err, "cannot remove client %s", client.name))
		}
	}
	return errors.Combine(errs...)
}

func (a *Actions) AddClient(name string, client *ClientEntry) {
	a.client[name] = client
	a.actionBuffer.AddSize(client.queueSize)
	client.setJobQueue(a.actionBuffer)
}

func (a *Actions) Action(ap *mediaserverproto.ActionParam, domain string, actionTimeout time.Duration) (*mediaserverproto.Cache, error) {
	item := ap.GetItem()
	var params actionParams.ActionParams = ap.GetParams()
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
	a.logger.Debug().Msgf("running action %s", actionString)
	if a.actionBuffer.Push(&ActionJob{
		id:         uuid.NewString(),
		domain:     domain,
		ap:         ap,
		resultChan: resultChan,
	}) == false {
		return nil, errors.Errorf("action buffer full for %s/%s/%s/%s", item.GetIdentifier().GetCollection(), item.GetIdentifier().GetSignature(), ap.GetAction(), params.String())
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

func (a *Actions) GetParams(_type, action, domain string) ([]string, error) {
	md := metadata.New(nil)
	md.Set("domain", domain)
	ctx := metadata.NewOutgoingContext(context.Background(), md)

	for address, client := range a.client {
		resp, err := client.client.GetParams(ctx, &mediaserverproto.ParamsParam{
			Type:   _type,
			Action: action,
		})
		if err != nil {
			return nil, errors.Wrapf(err, "cannot get params for %s::%s from %s", _type, action, address)
		}
		return resp.GetValues(), nil
	}
	return nil, errors.Errorf("no client found for %s::%s", _type, action)
}

func (a *Actions) RemoveClient(name string) error {
	var errs []error
	if client, ok := a.client[name]; ok {
		if err := client.Close(); err != nil {
			errs = append(errs, errors.Wrapf(err, "cannot close client %s", name))
		}
		a.actionBuffer.SetSize(-client.queueSize)
		delete(a.client, name)
	}
	if len(errs) > 0 {
		return errors.Combine(errs...)
	}
	return nil
}

func (a *Actions) IsEmpty() bool {
	return len(a.client) == 0
}
