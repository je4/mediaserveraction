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

type TypeAction map[string][]string

func (ta TypeAction) String() string {
	mediaTypes := []string{}
	actions := []string{}

	for mediaType, as := range ta {
		mediaTypes = append(mediaTypes, mediaType)
		actions = append(actions, as...)
	}
	slices.Sort(actions)
	actions = slices.Compact(actions)
	return fmt.Sprintf("[%v]::[%v]", strings.Join(mediaTypes, ","), strings.Join(actions, ","))
}

func (ta TypeAction) Equals(ta2 TypeAction) bool {
	if len(ta) != len(ta2) {
		return false
	}
	for key, actions := range ta {
		if actions2, ok := ta2[key]; !ok {
			return false
		} else {
			for _, action := range actions {
				if !slices.Contains(actions2, action) {
					return false
				}
			}
		}
	}
	return true
}

type TypeActionParams map[string]map[string][]string

func (tap TypeActionParams) GetTypeActions() TypeAction {
	result := TypeAction{}
	for mediaType, actions := range tap {
		result[mediaType] = []string{}
		for action := range actions {
			result[mediaType] = append(result[mediaType], action)
		}
	}
	return result
}

func (tap TypeActionParams) Equals(tap2 TypeActionParams) bool {
	if len(tap) != len(tap2) {
		return false
	}
	for key, actions := range tap {
		if actions2, ok := tap2[key]; !ok {
			return false
		} else {
			for action, params := range actions {
				if params2, ok := actions2[action]; !ok {
					return false
				} else {
					for _, param := range params {
						if !slices.Contains(params2, param) {
							return false
						}
					}
				}
			}
		}
	}
	return true
}

func NewCache(actionTimeout time.Duration, dbs map[string]mediaserverproto.DatabaseClient, allDomains []string, logger zLogger.ZLogger) *Cache {
	cache := &Cache{
		cache:            map[string]*Actions{},
		allDomains:       allDomains,
		actionTimeout:    actionTimeout,
		logger:           logger,
		typeActionParams: TypeActionParams{},
		dbs:              dbs,
	}
	return cache
}

type Cache struct {
	cache            map[string]*Actions
	actionTimeout    time.Duration
	dbs              map[string]mediaserverproto.DatabaseClient
	logger           zLogger.ZLogger
	typeActionParams TypeActionParams
	allDomains       []string
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
	if actions, ok := c.typeActionParams[mediaType]; ok {
		if params, ok := actions[action]; ok {
			return params, nil
		} else {
			return nil, errors.Errorf("action %s::%s not found", mediaType, action)
		}
	} else {
		return nil, errors.Errorf("mediaType %s not found", mediaType)
	}
}

func (c *Cache) SetAction(typeActionsParam TypeActionParams, domains []string, actionsObject *Actions) {
	c.typeActionParams = typeActionsParam
	for _, domain := range domains {
		for mediaType, actions := range typeActionsParam {
			for action, _ := range actions {
				sig := fmt.Sprintf("%s::%s::%s", domain, mediaType, action)
				c.cache[sig] = actionsObject
			}
		}
	}
}

func (c *Cache) GetAllActionParam() TypeActionParams {

	return c.typeActionParams
}

func (c *Cache) AddActions(typeActionsParams TypeActionParams, domains []string) error {
	c.logger.Debug().Msgf("adding actions %v", typeActionsParams)
	var typeAction = typeActionsParams.GetTypeActions()
	cd, ok := c.GetActions(typeAction, domains)
	if !ok {
		// create empty cache entry
		cd = NewActions(typeAction, c.logger)
		if err := cd.Start(); err != nil {
			return errors.Wrapf(err, "cannot start actions %v", typeAction)
		}
		// add it for all actions
		c.SetAction(typeActionsParams, domains, cd)
	}

	return nil
}

func (c *Cache) GetAction(mediaType, action, domain string) (*Actions, bool) {
	sig := fmt.Sprintf("%s::%s::%s", domain, mediaType, action)
	as, ok := c.cache[sig]
	return as, ok
}
func (c *Cache) GetActions(typeActions TypeAction, domains []string) (resultActions *Actions, resultOK bool) {
	for _, domain := range domains {
		for mediaType, actions := range typeActions {
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
	}
	return
}

func (c *Cache) GetClientEntryByName(name string) (resultClient *ClientEntry, resultDomains []string, resultTypeActions TypeAction, rok bool) {
	var keys = []string{}
	for key, actions := range c.cache {
		if client, ok := actions.GetClient(name); ok {
			if resultClient != nil && resultClient != client {
				c.logger.Error().Msgf("client %s found multiple times", name)
				return nil, nil, nil, false
			}
			resultClient = client
			keys = append(keys, key)
			rok = true
		}
	}
	resultTypeActions = TypeAction{}
	for _, key := range keys {
		parts := strings.Split(key, "::")
		if len(parts) != 3 {
			c.logger.Error().Msgf("invalid key %s", key)
			return nil, nil, nil, false
		}
		resultDomains = append(resultDomains, parts[0])
		mediaType := parts[1]
		action := parts[2]
		if _, ok := resultTypeActions[mediaType]; !ok {
			resultTypeActions[mediaType] = []string{}
		}
		resultTypeActions[mediaType] = append(resultTypeActions[mediaType], action)
	}
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

func (c *Cache) AddClientEntry(typeActionsParams TypeActionParams, domains []string, name string, client *ClientEntry) {
	typeActions := typeActionsParams.GetTypeActions()
	ractions, ok := c.GetActions(typeActions, domains)
	if !ok {
		ractions = NewActions(typeActions, c.logger)
		if err := ractions.Start(); err != nil {
			c.logger.Error().Err(err).Msgf("cannot start actions %v", typeActions)
			return
		}
		c.SetAction(typeActionsParams, domains, ractions)
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
