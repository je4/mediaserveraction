package actionCache

import (
	"context"
	"emperror.dev/errors"
	"github.com/google/uuid"
	pb "github.com/je4/mediaserverproto/v2/pkg/mediaserveraction/proto"
)

func NewActions(mediaType string, action []string) *Actions {
	return &Actions{
		client:        map[string]*ClientEntry{},
		mediaType:     mediaType,
		action:        action,
		actionJobChan: make(chan *ActionJob),
	}
}

type ActionJob struct {
	id         string
	collection string
	signature  string
	action     string
	params     ActionParams
	resultChan chan<- error
}

type Actions struct {
	client        map[string]*ClientEntry
	mediaType     string
	action        []string
	actionJobChan chan *ActionJob
}

func (a *Actions) AddClient(name string, client *ClientEntry) {
	a.client[name] = client
}

func (a *Actions) Action(collection, signature, action string, params ActionParams) error {
	resultChan := make(chan error)
	a.actionJobChan <- &ActionJob{
		id:         uuid.NewString(),
		collection: collection,
		signature:  signature,
		action:     action,
		params:     params,
		resultChan: resultChan,
	}
	err := <-resultChan
	if err != nil {
		return errors.Wrapf(err, "action %s::%s %v failed", a.mediaType, action, params)
	}
	return nil
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
