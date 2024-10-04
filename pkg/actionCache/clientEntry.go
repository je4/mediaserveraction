package actionCache

import (
	"context"
	"emperror.dev/errors"
	pbgeneric "github.com/je4/genericproto/v2/pkg/generic/proto"
	mediaserverproto "github.com/je4/mediaserverproto/v2/pkg/mediaserver/proto"
	"github.com/je4/utils/v2/pkg/zLogger"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"io"
	"sync"
	"time"
)

func NewClientEntry(name string, client mediaserverproto.ActionClient, closer io.Closer, interval time.Duration, dbs map[string]mediaserverproto.DatabaseClient, queueSize int) *ClientEntry {
	ce := &ClientEntry{
		Mutex:        sync.Mutex{},
		name:         name,
		dbs:          dbs,
		client:       client,
		clientCloser: closer,
		clientDone:   make(chan bool),
		wg:           sync.WaitGroup{},
		workersDone:  map[uint32]chan bool{},
		queueSize:    queueSize,
	}
	ce.RefreshTimeout(interval)
	return ce
}

type ClientEntry struct {
	sync.Mutex
	name          string
	dbs           map[string]mediaserverproto.DatabaseClient
	client        mediaserverproto.ActionClient
	clientDone    chan bool
	clientCloser  io.Closer
	clientTimeout time.Time
	concurrency   uint32
	workersDone   map[uint32]chan bool
	//jobChan       <-chan *ActionJob
	wg        sync.WaitGroup
	jobQueue  *Queue[*ActionJob]
	queueSize int
}

func (c *ClientEntry) setJobQueue(jobQueue *Queue[*ActionJob]) {
	c.Lock()
	defer c.Unlock()
	c.jobQueue = jobQueue
}

func (c *ClientEntry) doIt(job *ActionJob) (*mediaserverproto.Cache, error) {
	md := metadata.New(nil)
	md.Set("domain", job.domain)
	ctx := metadata.NewOutgoingContext(context.Background(), md)
	cache, err := c.client.Action(ctx, job.ap)
	if err != nil {
		return nil, errors.Wrapf(err, "job %v failed", job)
	}
	if cache.GetIdentifier() == nil {
		return cache, nil
	}
	db, ok := c.dbs[job.domain]
	if !ok {
		return nil, errors.Errorf("job %s failed: database for domain %s not found", job.id, job.domain)
	}
	resp2, err := db.InsertCache(context.Background(), cache)
	if err != nil {
		return nil, errors.Wrapf(err, "job %s failed: cannot store cache", job.id)
	}
	if resp2.GetStatus() != pbgeneric.ResultStatus_OK {
		return nil, errors.Errorf("job %s failed: cannot store cache: %s", job.id, resp2.GetMessage())
	}
	return cache, nil
}

func (c *ClientEntry) Start(workers uint32, logger zLogger.ZLogger) error {
	for workerNum := range workers {
		c.wg.Add(1)
		go func(thisWorkerNum uint32) {
			done := make(chan bool)
			c.Lock()
			c.workersDone[thisWorkerNum] = done
			c.Unlock()
			defer c.wg.Done()
			logger.Info().Str("client", c.name).Uint32("worker", thisWorkerNum).Msg("worker started")
			for {
				select {
				case job := <-c.jobQueue.Out():
					logger.Info().Str("job", job.id).Str("client", c.name).Uint32("worker", thisWorkerNum).Msgf("job %v", job)
					cache, err := c.doIt(job)
					if err != nil {
						errCode := status.Code(err)
						if errCode == codes.Unavailable {
							// todo: if we cannot connect do some panic stuff

						}
						logger.Error().Err(err).Str("job", job.id).Str("client", c.name).Uint32("worker", thisWorkerNum).Msgf("error processing job %v", job)
					}
					job.resultChan <- &ActionResult{err: err, result: cache}
					logger.Info().Str("job", job.id).Str("client", c.name).Uint32("worker", thisWorkerNum).Msgf("job done %v", job)
				case <-done:
					logger.Info().Str("client", c.name).Uint32("worker", thisWorkerNum).Msg("worker done")
					return
				}
			}
		}(workerNum)
	}
	return nil
}

func (c *ClientEntry) SetTimeout(expiration time.Time) {
	c.Lock()
	defer c.Unlock()
	c.clientTimeout = expiration
}

func (c *ClientEntry) Close() error {
	c.Lock()
	for workerDone, _ := range c.workersDone {
		c.workersDone[workerDone] <- true
		close(c.workersDone[workerDone])
	}
	clear(c.workersDone)
	c.Unlock()
	done := make(chan bool)
	go func() {
		c.wg.Wait()
		done <- true
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-time.After(60 * time.Second):
		return errors.New("timeout waiting for workers to finish")
	}
}

func (c *ClientEntry) RefreshTimeout(interval time.Duration) {
	c.SetTimeout(time.Now().Add(interval))
}
