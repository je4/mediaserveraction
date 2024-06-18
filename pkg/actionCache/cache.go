package actionCache

import (
	"emperror.dev/errors"
	"fmt"
	mediaserverproto "github.com/je4/mediaserverproto/v2/pkg/mediaserver/proto"
	"github.com/je4/utils/v2/pkg/zLogger"
	"golang.org/x/exp/maps"
	"slices"
	"time"
)

var coreActions = []string{"item", "metadata"}

func NewCache(actionTimeout time.Duration, db mediaserverproto.DatabaseClient, logger zLogger.ZLogger) *Cache {
	cache := &Cache{
		cache:         map[string]*Actions{},
		actionTimeout: actionTimeout,
		logger:        logger,
		actionParams:  map[string][]string{},
		db:            db,
	}
	return cache
}

type Cache struct {
	cache         map[string]*Actions
	actionTimeout time.Duration
	db            mediaserverproto.DatabaseClient
	logger        zLogger.ZLogger
	actionParams  map[string][]string
}

func (c *Cache) Action(ap *mediaserverproto.ActionParam) (*mediaserverproto.Cache, error) {
	item := ap.GetItem()
	actions, ok := c.GetActions(item.GetMetadata().GetType(), ap.GetAction())
	if !ok {
		return nil, errors.Errorf("actions %s::%s not found", item.GetMetadata().GetType(), ap.GetAction())
	}
	return actions.Action(ap, c.actionTimeout)
}

func (c *Cache) GetParams(mediaType string, action string) ([]string, error) {
	if slices.Contains(coreActions, action) {
		return []string{}, nil
	}
	params, ok := c.actionParams[fmt.Sprintf("%s::%s", mediaType, action)]
	if !ok {
		return nil, errors.Errorf("action %s::%s not found", mediaType, action)
	}
	return params, nil
}

func (c *Cache) SetAction(mediaType string, action string, actions *Actions) {
	sig := fmt.Sprintf("%s::%s", mediaType, action)
	c.cache[sig] = actions
}

func (c *Cache) GetAllActionParam() map[string][]string {
	return c.actionParams
}

func (c *Cache) AddActions(mediaType string, actionParams map[string][]string) error {
	actions := maps.Keys(actionParams)
	for action, params := range actionParams {
		key := fmt.Sprintf("%s::%s", mediaType, action)
		if cParams, ok := c.actionParams[key]; ok {
			if slices.Compare(params, cParams) != 0 {
				return errors.Errorf("action %s::%s already defined with different params", mediaType, action)
			}
		} else {
			c.actionParams[key] = params
		}
	}
	cd, ok := c.GetActions(mediaType, actions[0])
	if !ok {
		// create empty cache entry
		cd = NewActions(mediaType, actions, c.logger)
		// add it for all actions
		for _, a := range actions {
			c.SetAction(mediaType, a, cd)
		}
	}
	return nil
}

func (c *Cache) GetActions(mediaType, action string) (*Actions, bool) {
	sig := fmt.Sprintf("%s::%s", mediaType, action)
	actions, ok := c.cache[sig]
	return actions, ok
}

func (c *Cache) GetClientEntry(mediaType, action, address string) (*ClientEntry, bool) {
	actions, ok := c.GetActions(mediaType, action)
	if !ok {
		return nil, false
	}
	return actions.GetClient(address)
}

func (c *Cache) RemoveClientEntry(mediaType, action, address string) error {
	actions, ok := c.GetActions(mediaType, action)
	if !ok {
		return nil
	}
	var errs []error
	if err := actions.RemoveClient(address); err != nil {
		errs = append(errs, err)
	}
	if actions.IsEmpty() {
		delete(c.cache, fmt.Sprintf("%s::%s", mediaType, action))
	}
	return errors.Combine(errs...)
}

func (c *Cache) AddClientEntry(mediaType, action, address string, client *ClientEntry) {
	actions, ok := c.GetActions(mediaType, action)
	if !ok {
		actions = NewActions(mediaType, []string{action}, c.logger)
		c.SetAction(mediaType, action, actions)
	}
	actions.AddClient(address, client)
}
