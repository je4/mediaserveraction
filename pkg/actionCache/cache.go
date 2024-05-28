package actionCache

import (
	"emperror.dev/errors"
	"fmt"
	mediaserverproto "github.com/je4/mediaserverproto/v2/pkg/mediaserver/proto"
	"github.com/je4/utils/v2/pkg/zLogger"
	"time"
)

func NewCache(actionTimeout time.Duration, db mediaserverproto.DatabaseClient, logger zLogger.ZLogger) *Cache {
	return &Cache{
		cache:         map[string]*Actions{},
		actionTimeout: actionTimeout,
		logger:        logger,
		db:            db,
	}
}

type Cache struct {
	cache         map[string]*Actions
	actionTimeout time.Duration
	db            mediaserverproto.DatabaseClient
	logger        zLogger.ZLogger
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
	actions, ok := c.GetActions(mediaType, action)
	if !ok {
		return nil, fmt.Errorf("actions %s::%s not found", mediaType, action)
	}
	return actions.GetParams(action)
}

func (c *Cache) SetAction(mediaType string, action string, actions *Actions) {
	sig := fmt.Sprintf("%s::%s", mediaType, action)
	c.cache[sig] = actions
}

func (c *Cache) AddActions(mediaType string, actions []string) {
	cd, ok := c.GetActions(mediaType, actions[0])
	if !ok {
		// create empty cache entry
		cd = NewActions(mediaType, actions)
		// add it for all actions
		for _, a := range actions {
			c.SetAction(mediaType, a, cd)
		}
	}
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

func (c *Cache) AddClientEntry(mediaType, action, address string, client *ClientEntry) {
	actions, ok := c.GetActions(mediaType, action)
	if !ok {
		actions = NewActions(mediaType, []string{action})
		c.SetAction(mediaType, action, actions)
	}
	actions.AddClient(address, client)
}
