package main

import (
	"context"
	"flag"
	"fmt"
	genericproto "github.com/je4/genericproto/v2/pkg/generic/proto"
	"github.com/je4/mediaserveraction/v2/config"
	"github.com/je4/mediaserveraction/v2/pkg/actionCache"
	"github.com/je4/mediaserveraction/v2/pkg/actionController"
	"github.com/je4/mediaserveraction/v2/pkg/actionDispatcher"
	pb "github.com/je4/mediaserverproto/v2/pkg/mediaserveraction/proto"
	mediaserverdbClient "github.com/je4/mediaserverproto/v2/pkg/mediaserverdb/client"
	mediaserverdbproto "github.com/je4/mediaserverproto/v2/pkg/mediaserverdb/proto"
	miniresolverClient "github.com/je4/miniresolver/v2/pkg/client"
	"github.com/je4/miniresolver/v2/pkg/grpchelper"
	"github.com/je4/miniresolver/v2/pkg/miniresolverproto"
	"github.com/je4/trustutil/v2/pkg/certutil"
	"github.com/je4/trustutil/v2/pkg/loader"
	configutil "github.com/je4/utils/v2/pkg/config"
	"github.com/je4/utils/v2/pkg/zLogger"
	"github.com/rs/zerolog"
	"google.golang.org/protobuf/types/known/emptypb"
	"io"
	"io/fs"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"
)

var configfile = flag.String("config", "", "location of toml configuration file")

func main() {
	flag.Parse()

	var cfgFS fs.FS
	var cfgFile string
	if *configfile != "" {
		cfgFS = os.DirFS(filepath.Dir(*configfile))
		cfgFile = filepath.Base(*configfile)
	} else {
		cfgFS = config.ConfigFS
		cfgFile = "mediaserveraction.toml"
	}

	conf := &MediaserverActionConfig{
		LocalAddr: "localhost:8443",
		//ResolverTimeout: config.Duration(10 * time.Minute),
		ExternalAddr:            "https://localhost:8443",
		LogLevel:                "DEBUG",
		ResolverTimeout:         configutil.Duration(10 * time.Minute),
		ResolverNotFoundTimeout: configutil.Duration(10 * time.Second),
		ServerTLS: &loader.TLSConfig{
			Type: "DEV",
		},
		ClientTLS: &loader.TLSConfig{
			Type: "DEV",
		},
	}
	if err := LoadMediaserverActionConfig(cfgFS, cfgFile, conf); err != nil {
		log.Fatalf("cannot load toml from [%v] %s: %v", cfgFS, cfgFile, err)
	}
	// create logger instance
	var out io.Writer = os.Stdout
	if conf.LogFile != "" {
		fp, err := os.OpenFile(conf.LogFile, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
		if err != nil {
			log.Fatalf("cannot open logfile %s: %v", conf.LogFile, err)
		}
		defer fp.Close()
		out = fp
	}

	output := zerolog.ConsoleWriter{Out: out, TimeFormat: time.RFC3339}
	_logger := zerolog.New(output).With().Timestamp().Logger()
	_logger.Level(zLogger.LogLevel(conf.LogLevel))
	var logger zLogger.ZLogger = &_logger

	var dbClientAddr string
	if conf.ResolverAddr != "" {
		dbClientAddr = grpchelper.GetAddress(mediaserverdbproto.DBController_Ping_FullMethodName)
	} else {
		if _, ok := conf.GRPCClient["mediaserverdb"]; !ok {
			logger.Fatal().Msg("no mediaserverdb grpc client defined")
		}
		dbClientAddr = conf.GRPCClient["mediaserverdb"]
	}

	clientCert, clientLoader, err := loader.CreateClientLoader(conf.ClientTLS, logger)
	if err != nil {
		logger.Panic().Msgf("cannot create client loader: %v", err)
	}
	defer clientLoader.Close()

	var resolver miniresolverproto.MiniResolverClient
	if conf.ResolverAddr != "" {
		logger.Info().Msgf("resolver address is %s", conf.ResolverAddr)
		miniResolverClient, miniResolverCloser, err := miniresolverClient.CreateClient(conf.ResolverAddr, clientCert)
		if err != nil {
			logger.Fatal().Msgf("cannot create resolver client: %v", err)
		}
		defer miniResolverCloser.Close()
		grpchelper.RegisterResolver(miniResolverClient, time.Duration(conf.ResolverTimeout), time.Duration(conf.ResolverNotFoundTimeout), logger)
		resolver = miniResolverClient
	}

	dbClient, dbClientCloser, err := mediaserverdbClient.CreateClient(dbClientAddr, clientCert)
	if err != nil {
		logger.Panic().Msgf("cannot create mediaserverdb grpc client: %v", err)
	}
	defer dbClientCloser.Close()
	if resp, err := dbClient.Ping(context.Background(), &emptypb.Empty{}); err != nil {
		logger.Error().Msgf("cannot ping mediaserverdb: %v", err)
	} else {
		if resp.GetStatus() != genericproto.ResultStatus_OK {
			logger.Error().Msgf("cannot ping mediaserverdb: %v", resp.GetStatus())
		} else {
			logger.Info().Msgf("mediaserverdb ping response: %s", resp.GetMessage())
		}
	}

	// create TLS Certificate.
	// the certificate MUST contain <package>.<service> as DNS name
	certutil.AddDefaultDNSNames(grpchelper.GetService(pb.ActionDispatcher_Ping_FullMethodName), grpchelper.GetService(pb.ActionController_Ping_FullMethodName))
	serverTLSConfig, serverLoader, err := loader.CreateServerLoader(true, conf.ServerTLS, nil, logger)
	if err != nil {
		logger.Fatal().Err(err).Msg("cannot create server loader")
	}
	defer serverLoader.Close()

	// create grpc server with resolver for name resolution
	grpcServer, err := grpchelper.NewServer(conf.LocalAddr, serverTLSConfig, resolver, logger)
	if err != nil {
		logger.Fatal().Err(err).Msg("cannot create server")
	}

	// register the server

	cache := actionCache.NewCache(time.Duration(conf.ActionTimeout), dbClient, logger)
	adService, err := actionDispatcher.NewActionDispatcher(cache, clientCert, time.Duration(conf.ResolverTimeout), dbClient, logger)
	if err != nil {
		logger.Fatal().Err(err).Msg("cannot create action dispatcher service")
	}
	pb.RegisterActionDispatcherServer(grpcServer, adService)

	acService, err := actionController.NewActionController(cache, dbClient, logger)
	if err != nil {
		logger.Fatal().Err(err).Msg("cannot create action controller service")
	}
	pb.RegisterActionControllerServer(grpcServer, acService)

	grpcServer.Startup()
	done := make(chan os.Signal, 1)
	signal.Notify(done, syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL)
	fmt.Println("press ctrl+c to stop server")
	s := <-done
	fmt.Println("got signal:", s)

	defer grpcServer.GracefulStop()

}
