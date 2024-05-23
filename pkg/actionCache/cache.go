package actionCache

import (
	"context"
	"emperror.dev/errors"
	"fmt"
	mediaserverdbproto "github.com/je4/mediaserverproto/v2/pkg/mediaserverdb/proto"
	"github.com/je4/utils/v2/pkg/zLogger"
)

func NewCache(db mediaserverdbproto.DBControllerClient, logger zLogger.ZLogger) *Cache {
	return &Cache{
		cache:  map[string]*Actions{},
		logger: logger,
		db:     db,
	}
}

type Cache struct {
	cache  map[string]*Actions
	db     mediaserverdbproto.DBControllerClient
	logger zLogger.ZLogger
}

func (c *Cache) Action(collection, signature, action string, params map[string]string) error {
	it, err := c.db.GetItem(context.Background(), &mediaserverdbproto.ItemIdentifier{
		Collection: collection,
		Signature:  signature,
	})
	if err != nil {
		return errors.Wrapf(err, "cannot get item %s/%s", collection, signature)

	}
	actions, ok := c.GetActions(it.GetMetadata().GetType(), action)
	if !ok {
		return errors.Errorf("actions %s::%s not found", it.GetMetadata().GetType(), action)
	}
	return actions.Action(collection, signature, action, params)
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
