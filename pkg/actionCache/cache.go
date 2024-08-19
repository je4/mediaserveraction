package actionCache

import (
	"emperror.dev/errors"
	"fmt"
	mediaserverproto "github.com/je4/mediaserverproto/v2/pkg/mediaserver/proto"
	"github.com/je4/utils/v2/pkg/zLogger"
	"golang.org/x/exp/maps"
	"slices"
	"strings"
	"time"
)

var coreActions = []string{"item", "master", "metadata"}

func NewCache(actionTimeout time.Duration, dbs map[string]mediaserverproto.DatabaseClient, allDomains []string, logger zLogger.ZLogger) *Cache {
	cache := &Cache{
		cache:         map[string]*Actions{},
		allDomains:    allDomains,
		actionTimeout: actionTimeout,
		logger:        logger,
		actionParams:  map[string][]string{},
		dbs:           dbs,
	}
	return cache
}

type Cache struct {
	cache         map[string]*Actions
	actionTimeout time.Duration
	dbs           map[string]mediaserverproto.DatabaseClient
	logger        zLogger.ZLogger
	actionParams  map[string][]string
	allDomains    []string
}

func (c *Cache) Action(ap *mediaserverproto.ActionParam, domain string) (*mediaserverproto.Cache, error) {
	item := ap.GetItem()
	actions, ok := c.GetAction(item.GetMetadata().GetType(), ap.GetAction(), domain)
	if !ok {
		return nil, errors.Errorf("actions %s::%s not found", item.GetMetadata().GetType(), ap.GetAction())
	}
	return actions.Action(ap, domain, c.actionTimeout)
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

func (c *Cache) SetAction(mediaType string, actions, domains []string, actionsObject *Actions) {
	for _, domain := range domains {
		for _, action := range actions {
			sig := fmt.Sprintf("%s::%s::%s", domain, mediaType, action)
			c.cache[sig] = actionsObject
		}
	}
}

func (c *Cache) GetAllActionParam() map[string][]string {
	return c.actionParams
}

func (c *Cache) AddActions(mediaType string, actionParams map[string][]string, domains []string) error {
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
	cd, ok := c.GetActions(mediaType, actions, domains)
	if !ok {
		// create empty cache entry
		cd = NewActions(mediaType, actions, c.logger)
		if err := cd.Start(); err != nil {
			return errors.Wrapf(err, "cannot start actions %s::%v", mediaType, actions)
		}
		// add it for all actions
		c.SetAction(mediaType, actions, domains, cd)
	}
	return nil
}

func (c *Cache) GetAction(mediaType, action, domain string) (*Actions, bool) {
	sig := fmt.Sprintf("%s::%s::%s", domain, mediaType, action)
	as, ok := c.cache[sig]
	return as, ok
}
func (c *Cache) GetActions(mediaType string, actions, domains []string) (resultActions *Actions, resultOK bool) {
	for _, domain := range domains {
		for _, action := range actions {
			if as, ok := c.GetAction(mediaType, action, domain); !ok {
				return nil, false
			} else {
				if resultActions != nil && resultActions != as {
					c.logger.Error().Msgf("different actions for %v::%s::%v found", domains, mediaType, actions)
					return nil, false
				}
				resultActions = as
				resultOK = true
			}
		}
	}
	return
}

func (c *Cache) GetClientEntryByName(name string) (resultClient *ClientEntry, resultDomains []string, resultMediatype string, resultActions []string, rok bool) {
	var keys = []string{}
	for key, actions := range c.cache {
		if client, ok := actions.GetClient(name); ok {
			if resultClient != nil && resultClient != client {
				c.logger.Error().Msgf("client %s found multiple times", name)
				return nil, nil, "", nil, false
			}
			resultClient = client
			keys = append(keys, key)
			rok = true
		}
	}
	for _, key := range keys {
		parts := strings.Split(key, "::")
		if len(parts) != 3 {
			c.logger.Error().Msgf("invalid key %s", key)
			return nil, nil, "", nil, false
		}
		resultDomains = append(resultDomains, parts[0])
		if resultMediatype != "" && resultMediatype != parts[1] {
			c.logger.Error().Msgf("multiple media types %s - %s found for %s", resultMediatype, parts[1], name)
			return nil, nil, "", nil, false
		} else {
			resultMediatype = parts[1]
		}
		resultActions = append(resultActions, parts[2])
	}
	slices.Sort(resultDomains)
	slices.Sort(resultActions)
	slices.Compact(resultDomains)
	slices.Compact(resultActions)
	return
}

func (c *Cache) GetClientEntry(mediaType, action, domain, address string) (*ClientEntry, bool) {
	actions, ok := c.GetAction(mediaType, action, domain)
	if !ok {
		return nil, false
	}
	return actions.GetClient(address)
}

func (c *Cache) RemoveClientEntry(address string) error {
	var errs []error
	keys := maps.Keys(c.cache)
	for _, key := range keys {
		actions := c.cache[key]
		if err := actions.RemoveClient(address); err != nil {
			errs = append(errs, err)
		}
		if actions.IsEmpty() {
			delete(c.cache, key)
		}
	}
	return errors.Combine(errs...)
}

func (c *Cache) AddClientEntry(mediaType string, actions, domains []string, name string, client *ClientEntry) {
	ractions, ok := c.GetActions(mediaType, actions, domains)
	if !ok {
		ractions = NewActions(mediaType, actions, c.logger)
		if err := ractions.Start(); err != nil {
			c.logger.Error().Err(err).Msgf("cannot start actions %s::%v", mediaType, actions)
			return
		}
		c.SetAction(mediaType, actions, domains, ractions)
	}
	ractions.AddClient(name, client)
}

func (c *Cache) Close() error {
	var errs []error
	for _, actions := range c.cache {
		if err := actions.Stop(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Combine(errs...)
}
