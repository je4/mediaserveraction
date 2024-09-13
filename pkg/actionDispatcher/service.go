package actionDispatcher

import (
	"context"
	"crypto/tls"
	"fmt"
	pbgeneric "github.com/je4/genericproto/v2/pkg/generic/proto"
	"github.com/je4/mediaserveraction/v2/pkg/actionCache"
	mediaserverproto "github.com/je4/mediaserverproto/v2/pkg/mediaserver/proto"
	"github.com/je4/miniresolver/v2/pkg/resolver"
	"github.com/je4/utils/v2/pkg/zLogger"
	"golang.org/x/exp/maps"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"time"
)

func NewActionDispatcher(cache *actionCache.Cache, clientTLS *tls.Config, resolverClient *resolver.MiniResolver, refreshInterval time.Duration, dbs map[string]mediaserverproto.DatabaseClient, domains []string, logger zLogger.ZLogger) (*mediaserverActionDispatcher, error) {
	_logger := logger.With().Str("rpcService", "mediaserverActionDispatcher").Logger()
	return &mediaserverActionDispatcher{
		logger:          &_logger,
		resolverClient:  resolverClient,
		cache:           cache,
		clientTLS:       clientTLS,
		refreshInterval: refreshInterval,
		dbs:             dbs,
		domains:         domains,
	}, nil
}

type mediaserverActionDispatcher struct {
	mediaserverproto.UnimplementedActionDispatcherServer
	logger          zLogger.ZLogger
	cache           *actionCache.Cache
	clientTLS       *tls.Config
	refreshInterval time.Duration
	dbs             map[string]mediaserverproto.DatabaseClient
	resolverClient  *resolver.MiniResolver
	domains         []string
}

func (d *mediaserverActionDispatcher) Ping(context.Context, *emptypb.Empty) (*pbgeneric.DefaultResponse, error) {
	return &pbgeneric.DefaultResponse{
		Status:  pbgeneric.ResultStatus_OK,
		Message: "pong",
		Data:    nil,
	}, nil
}

// AddController adds a controller to the dispatcher
// Caveat: different services sharing an action MUST share all actions (no partial intersection of actions allowed)
func (d *mediaserverActionDispatcher) AddController(ctx context.Context, param *mediaserverproto.ActionDispatcherParam) (*mediaserverproto.ActionDispatcherDefaultResponse, error) {
	actionParams := param.GetActions()
	instance := param.GetName()
	if instance == "" {
		return nil, status.Errorf(codes.InvalidArgument, "no instance defined")
	}
	instance = "instance_" + instance
	domains := param.GetDomains()
	if len(domains) == 0 {
		domains = d.domains
	}

	aParams := actionCache.TypeActionParams{}
	for _type, actions := range actionParams {
		aParams[_type] = actionCache.TypeAction{}
		for action, params := range actions.GetValues() {
			aParams[_type][action] = params.GetValues()
		}
	}
	if err := d.cache.AddActions(aParams, domains); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "cannot add actions: %v", err)
	}
	name := fmt.Sprintf("%s.%s", instance, mediaserverproto.Action_ServiceDesc.ServiceName)

	ctrlName := aParams.GetTypeActions().String()
	clientEntry, _, ractions, ok := d.cache.GetClientEntryByName(name)
	if ok {
		if !aParams.GetTypeActions().Equals(ractions) {
			return nil, status.Errorf(codes.InvalidArgument, "controller %s already defined with different type/actions/domains", name)
		}
		// if client already exists, refresh timeout
		clientEntry.SetTimeout(time.Now().Add(d.refreshInterval))
	} else {
		// create new client
		c, closer, err := resolver.NewClientCloser[mediaserverproto.ActionClient](d.resolverClient, mediaserverproto.NewActionClient, mediaserverproto.Action_ServiceDesc.ServiceName, instance)
		if err != nil {
			d.logger.Error().Msgf("cannot create mediaserveraction grpc client '%s': %v", name, err)
			return nil, status.Errorf(codes.Internal, "cannot create client: %v", err)
		}
		resolver.DoPing(c, d.logger)

		queueSize := int(param.GetQueueSize())
		if queueSize == 0 {
			queueSize = int(2*param.GetConcurrency() + 1)
		}
		clientEntry = actionCache.NewClientEntry(ctrlName, c, closer, d.refreshInterval, d.dbs, queueSize)
		d.cache.AddClientEntry(aParams, domains, name, clientEntry)
		if err := clientEntry.Start(param.GetConcurrency(), d.logger); err != nil {
			return nil, status.Errorf(codes.Internal, "cannot start client %s: %v", name, err)
		}
	}
	return &mediaserverproto.ActionDispatcherDefaultResponse{
		Response: &pbgeneric.DefaultResponse{
			Status:  pbgeneric.ResultStatus_OK,
			Message: fmt.Sprintf("controller for domains %v %s added to %s", domains, name, ctrlName),
		},
		NextCallWait: int64(d.refreshInterval.Seconds()),
	}, nil
}
func (d *mediaserverActionDispatcher) RemoveController(ctx context.Context, param *mediaserverproto.ActionDispatcherParam) (*pbgeneric.DefaultResponse, error) {
	typeActionParams := param.GetActions()
	mediaTypes := maps.Keys(typeActionParams)
	if len(mediaTypes) == 0 {
		return nil, status.Errorf(codes.InvalidArgument, "no mediaTypes defined")
	}
	name := fmt.Sprintf("%s.%s", param.GetName(), mediaserverproto.Action_ServiceDesc.ServiceName)
	if err := d.cache.RemoveClientEntry(name); err != nil {
		return &pbgeneric.DefaultResponse{
			Status:  pbgeneric.ResultStatus_OK,
			Message: fmt.Sprintf("cannot remove controller %s: %v", name, err),
		}, nil
	}
	return &pbgeneric.DefaultResponse{
		Status:  pbgeneric.ResultStatus_OK,
		Message: fmt.Sprintf("controller %s removed from %s", name, typeActionParams),
	}, nil
}

func (d *mediaserverActionDispatcher) GetActions(context.Context, *emptypb.Empty) (*mediaserverproto.ActionMap, error) {
	typeActions := d.cache.GetAllActionParam()
	res := &mediaserverproto.ActionMap{
		Actions: map[string]*mediaserverproto.StringListMap{},
	}
	for mediaType, actions := range typeActions {
		res.Actions[mediaType] = &mediaserverproto.StringListMap{
			Values: map[string]*pbgeneric.StringList{},
		}
		for action, params := range actions {
			res.Actions[mediaType].Values[action] = &pbgeneric.StringList{
				Values: params,
			}
		}
	}
	return res, nil
}
