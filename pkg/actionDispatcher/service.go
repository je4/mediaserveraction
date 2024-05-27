package actionDispatcher

import (
	"context"
	"crypto/tls"
	"fmt"
	pbgeneric "github.com/je4/genericproto/v2/pkg/generic/proto"
	"github.com/je4/mediaserveraction/v2/pkg/actionCache"
	"github.com/je4/mediaserverproto/v2/pkg/mediaserveraction/client"
	pb "github.com/je4/mediaserverproto/v2/pkg/mediaserveraction/proto"
	pbdb "github.com/je4/mediaserverproto/v2/pkg/mediaserverdb/proto"
	"github.com/je4/utils/v2/pkg/zLogger"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"net"
	"time"
)

func NewActionDispatcher(cache *actionCache.Cache, clientTLS *tls.Config, refreshInterval time.Duration, db pbdb.DBControllerClient, logger zLogger.ZLogger) (*mediaserverActionDispatcher, error) {
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
	pb.UnimplementedActionDispatcherServer
	logger          zLogger.ZLogger
	cache           *actionCache.Cache
	clientTLS       *tls.Config
	refreshInterval time.Duration
	db              pbdb.DBControllerClient
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
func (d *mediaserverActionDispatcher) AddController(ctx context.Context, param *pb.ActionDispatcherParam) (*pb.DispatcherDefaultResponse, error) {
	actions := param.GetAction()
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

	d.cache.AddActions(param.GetType(), actions)
	address := fmt.Sprintf("%s:%d", host, port)
	clientEntry, ok := d.cache.GetClientEntry(param.GetType(), actions[0], address)
	if ok {
		// if client already exists, refresh timeout
		clientEntry.SetTimeout(time.Now().Add(d.refreshInterval))
	} else {
		// create new client
		c, closer, err := client.CreateControllerClient(address, d.clientTLS)
		if err != nil {
			return nil, status.Errorf(500, "cannot create client: %v", err)
		}
		clientEntry = actionCache.NewClientEntry(fmt.Sprintf("%s::%v/%s", param.GetType(), actions, address), c, closer, d.refreshInterval, d.db)
		d.cache.AddClientEntry(param.GetType(), actions[0], address, clientEntry)
		if err := clientEntry.Start(param.GetConcurrency(), d.logger); err != nil {
			return nil, status.Errorf(codes.Internal, "cannot start client %s: %v", address, err)
		}
	}
	return &pb.DispatcherDefaultResponse{
		Response: &pbgeneric.DefaultResponse{
			Status:  pbgeneric.ResultStatus_OK,
			Message: fmt.Sprintf("controller %s added to %s::%v", address, param.GetType(), actions),
		},
		NextCallWait: int64(d.refreshInterval.Seconds()),
	}, nil
}
func (d *mediaserverActionDispatcher) RemoveController(ctx context.Context, param *pb.ActionDispatcherParam) (*pbgeneric.DefaultResponse, error) {
	actions := param.GetAction()
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
	cd, ok := d.cache.GetActions(param.GetType(), actions[0])
	if !ok {
		return &pbgeneric.DefaultResponse{
			Status:  pbgeneric.ResultStatus_OK,
			Message: fmt.Sprintf("no controller found for %s::%s", param.GetType(), actions[0]),
		}, nil
	}
	address := fmt.Sprintf("%s:%d", host, port)
	clientEntry, ok := cd.GetClient(address)
	if !ok {
		return &pbgeneric.DefaultResponse{
			Status:  pbgeneric.ResultStatus_OK,
			Message: fmt.Sprintf("controller %s:%d not found in %s::%s", host, port, param.GetType(), actions[0]),
		}, nil
	}
	if err := clientEntry.Close(); err != nil {
		return nil, status.Errorf(codes.Internal, "cannot close client: %v", err)
	}
	cd.RemoveClient(address)
	return &pbgeneric.DefaultResponse{
		Status:  pbgeneric.ResultStatus_OK,
		Message: fmt.Sprintf("controller %s removed from %s::%v", address, param.GetType(), actions),
	}, nil
}
