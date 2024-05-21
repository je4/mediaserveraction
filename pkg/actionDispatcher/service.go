package actionDispatcher

import (
	"context"
	"crypto/tls"
	"fmt"
	pbgeneric "github.com/je4/genericproto/v2/pkg/generic/proto"
	"github.com/je4/mediaserverproto/v2/pkg/mediaserveraction/client"
	pb "github.com/je4/mediaserverproto/v2/pkg/mediaserveraction/proto"
	"github.com/je4/utils/v2/pkg/zLogger"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"io"
)

type CacheEntry struct {
	client       pb.ActionControllerClient
	clientCloser io.Closer
	mediaType    string
	action       []string
}

func NewActionDispatcher(clientTLS *tls.Config, logger zLogger.ZLogger) (*mediaserverActionDispatcher, error) {
	return &mediaserverActionDispatcher{
		logger:    logger,
		cache:     make(map[string][]*CacheEntry),
		clientTLS: clientTLS,
	}, nil
}

type mediaserverActionDispatcher struct {
	pb.UnimplementedActionDispatcherServer
	logger    zLogger.ZLogger
	cache     map[string][]*CacheEntry
	clientTLS *tls.Config
}

func (d *mediaserverActionDispatcher) Ping(context.Context, *emptypb.Empty) (*pbgeneric.DefaultResponse, error) {
	return &pbgeneric.DefaultResponse{
		Status:  pbgeneric.ResultStatus_OK,
		Message: "pong",
		Data:    nil,
	}, nil
}

func (d *mediaserverActionDispatcher) AddController(ctx context.Context, param *pb.ActionDispatcherParam) (*pb.DispatcherDefaultResponse, error) {
	c, closer, err := client.CreateControllerClient(fmt.Sprintf("%s:%d", param.GetHost(), param.GetPort()), d.clientTLS)
	if err != nil {
		return nil, status.Errorf(500, "cannot create client: %v", err)
	}
	cd := &CacheEntry{
		client:       c,
		clientCloser: closer,
		mediaType:    param.Type,
		action:       param.Action,
	}
	for _, a := range param.GetAction() {
		sig := fmt.Sprintf("%s::%s", param.GetType(), a)
		if _, ok := d.cache[sig]; !ok {
			d.cache[sig] = make([]*CacheEntry, 0, 1)
		}
		d.cache[sig] = append(d.cache[sig], cd)
	}
}
func (d *mediaserverActionDispatcher) RemoveController(context.Context, *pb.ActionDispatcherParam) (*pbgeneric.DefaultResponse, error) {

}
