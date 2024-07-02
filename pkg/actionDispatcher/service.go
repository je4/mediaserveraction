package actionDispatcher

import (
	"context"
	"crypto/tls"
	"fmt"
	pbgeneric "github.com/je4/genericproto/v2/pkg/generic/proto"
	"github.com/je4/mediaserveraction/v2/pkg/actionCache"
	"github.com/je4/mediaserverproto/v2/pkg/mediaserver/client"
	mediaserverproto "github.com/je4/mediaserverproto/v2/pkg/mediaserver/proto"
	"github.com/je4/utils/v2/pkg/zLogger"
	"golang.org/x/exp/maps"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"net"
	"time"
)

func NewActionDispatcher(cache *actionCache.Cache, clientTLS *tls.Config, refreshInterval time.Duration, db mediaserverproto.DatabaseClient, logger zLogger.ZLogger) (*mediaserverActionDispatcher, error) {
	_logger := logger.With().Str("rpcService", "mediaserverActionDispatcher").Logger()
	return &mediaserverActionDispatcher{
		logger:          &_logger,
		cache:           cache,
		clientTLS:       clientTLS,
		refreshInterval: refreshInterval,
		db:              db,
	}, nil
}

type mediaserverActionDispatcher struct {
	mediaserverproto.UnimplementedActionDispatcherServer
	logger          zLogger.ZLogger
	cache           *actionCache.Cache
	clientTLS       *tls.Config
	refreshInterval time.Duration
	db              mediaserverproto.DatabaseClient
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
	actions := maps.Keys(actionParams)
	if len(actions) == 0 {
		return nil, status.Errorf(codes.InvalidArgument, "no actions defined")
	}
	port := param.GetPort()
	host := param.GetHost()
	if host == "" {
		// detect host from peer
		var err error
		p, ok := peer.FromContext(ctx)
		if !ok {
			return nil, fmt.Errorf("cannot get peer")
		}
		peerAddr := p.Addr.String()
		host, _, err = net.SplitHostPort(peerAddr)
		if err != nil {
			return nil, fmt.Errorf("cannot split host port of '%s': %v", peerAddr, err)
		}
		ip := net.ParseIP(host)
		if ip.To4() == nil {
			// IPv6
			host = fmt.Sprintf("[%s]", host)
		}
	}

	aParams := map[string][]string{}
	for action, params := range actionParams {
		aParams[action] = params.GetValues()
	}
	queueSize := int(param.GetQueueSize())
	if queueSize == 0 {
		queueSize = int(2*param.GetConcurrency() + 1)
	}
	if err := d.cache.AddActions(param.GetType(), aParams, queueSize); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "cannot add actions: %v", err)
	}
	address := fmt.Sprintf("%s:%d", host, port)
	clientEntry, ok := d.cache.GetClientEntry(param.GetType(), actions[0], address)
	if ok {
		// if client already exists, refresh timeout
		clientEntry.SetTimeout(time.Now().Add(d.refreshInterval))
	} else {
		// create new client
		c, closer, err := client.NewActionClient(address, d.clientTLS)
		if err != nil {
			return nil, status.Errorf(500, "cannot create client: %v", err)
		}
		queueSize = int(param.GetQueueSize())
		if queueSize == 0 {
			queueSize = int(2*param.GetConcurrency() + 1)
		}
		clientEntry = actionCache.NewClientEntry(fmt.Sprintf("%s::%v/%s", param.GetType(), actions, address), c, closer, d.refreshInterval, d.db, queueSize)
		d.cache.AddClientEntry(param.GetType(), actions[0], address, queueSize, clientEntry)
		if err := clientEntry.Start(param.GetConcurrency(), d.logger); err != nil {
			return nil, status.Errorf(codes.Internal, "cannot start client %s: %v", address, err)
		}
	}
	return &mediaserverproto.ActionDispatcherDefaultResponse{
		Response: &pbgeneric.DefaultResponse{
			Status:  pbgeneric.ResultStatus_OK,
			Message: fmt.Sprintf("controller %s added to %s::%v", address, param.GetType(), actions),
		},
		NextCallWait: int64(d.refreshInterval.Seconds()),
	}, nil
}
func (d *mediaserverActionDispatcher) RemoveController(ctx context.Context, param *mediaserverproto.ActionDispatcherParam) (*pbgeneric.DefaultResponse, error) {
	actionParams := param.GetActions()
	actions := maps.Keys(actionParams)
	if len(actions) == 0 {
		return nil, status.Errorf(codes.InvalidArgument, "no actions defined")
	}
	port := param.GetPort()
	host := param.GetHost()
	if host == "" {
		// detect host from peer
		var err error
		p, ok := peer.FromContext(ctx)
		if !ok {
			return nil, fmt.Errorf("cannot get peer")
		}
		peerAddr := p.Addr.String()
		host, _, err = net.SplitHostPort(peerAddr)
		if err != nil {
			return nil, fmt.Errorf("cannot split host port of '%s': %v", peerAddr, err)
		}
		ip := net.ParseIP(host)
		if ip.To4() == nil {
			// IPv6
			host = fmt.Sprintf("[%s]", host)
		}
	}
	// gettin the cache
	address := fmt.Sprintf("%s:%d", host, port)
	if err := d.cache.RemoveClientEntry(param.GetType(), actions[0], address); err != nil {
		return &pbgeneric.DefaultResponse{
			Status:  pbgeneric.ResultStatus_OK,
			Message: fmt.Sprintf("cannot remove controller %s: %v", address, err),
		}, nil
	}
	return &pbgeneric.DefaultResponse{
		Status:  pbgeneric.ResultStatus_OK,
		Message: fmt.Sprintf("controller %s removed from %s::%v", address, param.GetType(), actions),
	}, nil
}

func (d *mediaserverActionDispatcher) GetActions(context.Context, *emptypb.Empty) (*mediaserverproto.ActionMap, error) {
	actions := d.cache.GetAllActionParam()
	res := &mediaserverproto.ActionMap{
		Actions: map[string]*pbgeneric.StringList{},
	}
	for action, params := range actions {
		res.Actions[action] = &pbgeneric.StringList{
			Values: params,
		}
	}
	return res, nil
}
