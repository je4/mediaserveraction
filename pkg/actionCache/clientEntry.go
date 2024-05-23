package actionCache

import (
	"context"
	"emperror.dev/errors"
	pbgeneric "github.com/je4/genericproto/v2/pkg/generic/proto"
	pb "github.com/je4/mediaserverproto/v2/pkg/mediaserveraction/proto"
	pbdb "github.com/je4/mediaserverproto/v2/pkg/mediaserverdb/proto"
	"github.com/je4/utils/v2/pkg/zLogger"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"io"
	"sync"
	"time"
)

func NewClientEntry(client pb.ActionControllerClient, closer io.Closer, interval time.Duration, db pbdb.DBControllerClient) *ClientEntry {
	ce := &ClientEntry{
		Mutex:        sync.Mutex{},
		db:           db,
		client:       client,
		clientCloser: closer,
		clientDone:   make(chan bool),
		wg:           sync.WaitGroup{},
	}
	ce.RefreshTimeout(interval)
	return ce
}

type ClientEntry struct {
	sync.Mutex
	name          string
	db            pbdb.DBControllerClient
	client        pb.ActionControllerClient
	clientDone    chan bool
	clientCloser  io.Closer
	clientTimeout time.Time
	concurrency   uint32
	workersDone   map[uint32]chan bool
	jobChan       <-chan *ActionJob
	wg            sync.WaitGroup
}

func (c *ClientEntry) setJobChannel(jobChan <-chan *ActionJob) {
	c.Lock()
	defer c.Unlock()
	c.jobChan = jobChan
}

func (c *ClientEntry) doIt(job *ActionJob) error {
	resp, err := c.client.Action(context.Background(), &pb.ActionParam{
		Item: &pbdb.ItemIdentifier{
			Collection: job.collection,
			Signature:  job.signature,
		},
		Action: job.action,
		Params: job.params,
	})
	if err != nil {
		return errors.Wrapf(err, "job %v failed", job)
	}
	actionResponse := resp.GetResponse()
	if actionResponse.GetStatus() != pbgeneric.ResultStatus_OK {
		return status.Errorf(codes.Internal, "job %v failed: %s", job, actionResponse.GetMessage())
	}
	cache := resp.GetCache()
	if cache == nil {
		return status.Errorf(codes.Internal, "job %v failed: no cache", job)
	}
	c.db.SetCache(context.Background(), cache)
	return nil
}

func (c *ClientEntry) Start(workers uint32, logger zLogger.ZLogger) error {
	for workerNum := range workers {
		c.wg.Add(1)
		go func() {
			thisWorkerNum := workerNum
			c.workersDone[thisWorkerNum] = make(chan bool)
			defer c.wg.Done()
			logger.Info().Str("client", c.name).Uint32("worker", thisWorkerNum).Msg("worker started")
			for {
				select {
				case job := <-c.jobChan:
					err := c.doIt(job)
					if err != nil {
						errCode := status.Code(err)
						if errCode == codes.Unavailable {
							// if we cannot connect do some panic stuff

						}
						logger.Error().Err(err).Str("client", c.name).Uint32("worker", thisWorkerNum).Msgf("error processing job %v", job)
					}
					job.resultChan <- err
				case <-c.workersDone[thisWorkerNum]:
					logger.Info().Str("client", c.name).Uint32("worker", thisWorkerNum).Msg("worker done")
					return
				}
			}
		}()
	}
	return nil
}

func (c *ClientEntry) SetTimeout(expiration time.Time) {
	c.Lock()
	defer c.Unlock()
	c.clientTimeout = expiration
}

func (c *ClientEntry) Close() error {
	for workerDone := range c.workersDone {
		close(c.workersDone[workerDone])
	}
	done := make(chan bool)
	go func() {
		defer close(done)
		c.wg.Wait()
	}()
	select {
	case <-done:
		return nil
	case <-time.After(60 * time.Second):
		return errors.New("timeout waiting for workers to finish")
	}
}

func (c *ClientEntry) RefreshTimeout(interval time.Duration) {
	c.Lock()
	defer c.Unlock()
	c.clientTimeout = time.Now().Add(interval)
}
