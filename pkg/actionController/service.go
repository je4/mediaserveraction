package actionController

import (
	"context"
	pbgeneric "github.com/je4/genericproto/v2/pkg/generic/proto"
	"github.com/je4/mediaserveraction/v2/pkg/actionCache"
	pbdb "github.com/je4/mediaserverproto/v2/pkg/mediaserverdb/proto"
	"github.com/je4/utils/v2/pkg/zLogger"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	pb "github.com/je4/mediaserverproto/v2/pkg/mediaserveraction/proto"
)

func NewActionController(cache *actionCache.Cache, db pbdb.DBControllerClient, logger zLogger.ZLogger) (*mediaserverAction, error) {
	return &mediaserverAction{
		cache:  cache,
		db:     db,
		logger: logger,
	}, nil
}

type mediaserverAction struct {
	pb.UnimplementedActionControllerServer
	db     pbdb.DBControllerClient
	cache  *actionCache.Cache
	logger zLogger.ZLogger
}

func (d *mediaserverAction) Ping(context.Context, *emptypb.Empty) (*pbgeneric.DefaultResponse, error) {
	return &pbgeneric.DefaultResponse{
		Status:  pbgeneric.ResultStatus_OK,
		Message: "pong",
		Data:    nil,
	}, nil
}

func (d *mediaserverAction) GetParams(ctx context.Context, param *pb.ParamsParam) (*pbgeneric.StringList, error) {
	params, err := d.cache.GetParams(param.GetType(), param.GetAction())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "error getting params for %s::%s not found: %v", param.GetType(), param.GetAction(), err)
	}
	return &pbgeneric.StringList{
		Values: params,
	}, nil
}
func (d *mediaserverAction) Action(context.Context, *pb.ActionParam) (*pb.ActionResponse, error) {
	return nil, status.Errorf(codes.NotFound, "not implemented")
}
