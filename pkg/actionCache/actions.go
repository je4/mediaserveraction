package actionCache

import (
	"context"
	"emperror.dev/errors"
	"fmt"
	"github.com/google/uuid"
	mediaserverationactionproto "github.com/je4/mediaserverproto/v2/pkg/mediaserveraction/proto"
	pb "github.com/je4/mediaserverproto/v2/pkg/mediaserveraction/proto"
	mediaserverdbproto "github.com/je4/mediaserverproto/v2/pkg/mediaserverdb/proto"
	"time"
)

func NewActions(mediaType string, action []string) *Actions {
	return &Actions{
		client:        map[string]*ClientEntry{},
		mediaType:     mediaType,
		action:        action,
		actionJobChan: make(chan *ActionJob),
	}
}

type ActionResult struct {
	err    error
	result *mediaserverdbproto.Cache
}

type ActionJob struct {
	id         string
	ap         *mediaserverationactionproto.ActionParam
	resultChan chan<- *ActionResult
}

func (aj *ActionJob) String() string {
	item := aj.ap.GetItem()
	return fmt.Sprintf("ActionJob{id: %s, action: %s/%s/%s/%s}", aj.id, item.GetIdentifier().GetCollection(), item.GetIdentifier().GetSignature(), aj.ap.GetAction(), ActionParams(aj.ap.GetParams()).String())
}

type Actions struct {
	client        map[string]*ClientEntry
	mediaType     string
	action        []string
	actionJobChan chan *ActionJob
}

func (a *Actions) AddClient(name string, client *ClientEntry) {
	a.client[name] = client
	client.setJobChannel(a.actionJobChan)
}

func (a *Actions) Action(ap *mediaserverationactionproto.ActionParam, actionTimeout time.Duration) (*mediaserverdbproto.Cache, error) {
	item := ap.GetItem()
	resultChan := make(chan *ActionResult)
	select {
	case a.actionJobChan <- &ActionJob{
		id:         uuid.NewString(),
		ap:         ap,
		resultChan: resultChan,
	}:
	case <-time.After(actionTimeout):
		return nil, errors.Errorf("action %s/%s/%s/%s timeout", item.GetIdentifier().GetCollection(), item.GetIdentifier().GetSignature(), ap.GetAction(), ap.GetParams())
	}
	result := <-resultChan
	if result.err != nil {
		return nil, errors.Wrapf(result.err, "action %s/%s/%s/%s failed", item.GetIdentifier().GetCollection(), item.GetIdentifier().GetSignature(), ap.GetAction(), ap.GetParams())
	}
	return result.result, nil
}

func (a *Actions) GetClient(name string) (*ClientEntry, bool) {
	client, ok := a.client[name]
	return client, ok
}

func (a *Actions) GetParams(action string) ([]string, error) {
	for address, client := range a.client {
		resp, err := client.client.GetParams(context.Background(), &pb.ParamsParam{
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
