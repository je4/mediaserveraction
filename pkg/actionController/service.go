package actionController

import (
	"context"
	pbgeneric "github.com/je4/genericproto/v2/pkg/generic/proto"
	"github.com/je4/utils/v2/pkg/zLogger"
	"google.golang.org/protobuf/types/known/emptypb"

	pb "github.com/je4/mediaserverproto/v2/pkg/mediaserveraction/proto"
)

func NewActionController(logger zLogger.ZLogger) (*mediaserverAction, error) {
	return &mediaserverAction{
		logger: logger,
	}, nil
}

type mediaserverAction struct {
	pb.UnimplementedActionControllerServer
	logger zLogger.ZLogger
}

func (d *mediaserverAction) Ping(context.Context, *emptypb.Empty) (*pbgeneric.DefaultResponse, error) {
	return &pbgeneric.DefaultResponse{
		Status:  pbgeneric.ResultStatus_OK,
		Message: "pong",
		Data:    nil,
	}, nil
}
