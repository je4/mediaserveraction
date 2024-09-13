package main

import (
	"crypto/tls"
	"flag"
	"fmt"
	"github.com/je4/certloader/v2/pkg/loader"
	"github.com/je4/mediaserveraction/v2/config"
	"github.com/je4/mediaserveraction/v2/pkg/actionCache"
	"github.com/je4/mediaserveraction/v2/pkg/actionController"
	"github.com/je4/mediaserveraction/v2/pkg/actionDispatcher"
	mediaserverproto "github.com/je4/mediaserverproto/v2/pkg/mediaserver/proto"
	resolver "github.com/je4/miniresolver/v2/pkg/resolver"
	"github.com/je4/trustutil/v2/pkg/certutil"
	configutil "github.com/je4/utils/v2/pkg/config"
	"github.com/je4/utils/v2/pkg/zLogger"
	ublogger "gitlab.switch.ch/ub-unibas/go-ublogger"
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
		ResolverTimeout:         configutil.Duration(10 * time.Minute),
		ResolverNotFoundTimeout: configutil.Duration(10 * time.Second),
		ServerTLS: &loader.Config{
			Type: "DEV",
		},
		ClientTLS: &loader.Config{
			Type: "DEV",
		},
	}
	if err := LoadMediaserverActionConfig(cfgFS, cfgFile, conf); err != nil {
		log.Fatalf("cannot load toml from [%v] %s: %v", cfgFS, cfgFile, err)
	}
	// create logger instance
	hostname, err := os.Hostname()
	if err != nil {
		log.Fatalf("cannot get hostname: %v", err)
	}

	var loggerTLSConfig *tls.Config
	var loggerLoader io.Closer
	if conf.Log.Stash.TLS != nil {
		loggerTLSConfig, loggerLoader, err = loader.CreateClientLoader(conf.Log.Stash.TLS, nil)
		if err != nil {
			log.Fatalf("cannot create client loader: %v", err)
		}
		defer loggerLoader.Close()
	}

	_logger, _logstash, _logfile := ublogger.CreateUbMultiLoggerTLS(conf.Log.Level, conf.Log.File,
		ublogger.SetDataset(conf.Log.Stash.Dataset),
		ublogger.SetLogStash(conf.Log.Stash.LogstashHost, conf.Log.Stash.LogstashPort, conf.Log.Stash.Namespace, conf.Log.Stash.LogstashTraceLevel),
		ublogger.SetTLS(conf.Log.Stash.TLS != nil),
		ublogger.SetTLSConfig(loggerTLSConfig),
	)
	if _logstash != nil {
		defer _logstash.Close()
	}

	if _logfile != nil {
		defer _logfile.Close()
	}

	l2 := _logger.With().Timestamp().Str("host", hostname).Logger() //.Output(output)
	var logger zLogger.ZLogger = &l2

	clientTLSConfig, clientLoader, err := loader.CreateClientLoader(conf.ClientTLS, logger)
	if err != nil {
		logger.Panic().Msgf("cannot create client loader: %v", err)
	}
	defer clientLoader.Close()

	// create TLS Certificate.
	// the certificate MUST contain <package>.<service> as DNS name
	for _, domain := range conf.Domains {
		var domainPrefix string
		if domain != "" {
			domainPrefix = domain + "."
		}
		certutil.AddDefaultDNSNames(domainPrefix+mediaserverproto.Action_ServiceDesc.ServiceName, domainPrefix+mediaserverproto.ActionDispatcher_ServiceDesc.ServiceName)
	}
	serverTLSConfig, serverLoader, err := loader.CreateServerLoader(true, conf.ServerTLS, nil, logger)
	if err != nil {
		logger.Fatal().Err(err).Msg("cannot create server loader")
	}
	defer serverLoader.Close()

	logger.Info().Msgf("resolver address is %s", conf.ResolverAddr)
	resolverClient, err := resolver.NewMiniresolverClient(conf.ResolverAddr, conf.GRPCClient, clientTLSConfig, serverTLSConfig, time.Duration(conf.ResolverTimeout), time.Duration(conf.ResolverNotFoundTimeout), logger)
	if err != nil {
		logger.Fatal().Msgf("cannot create resolver client: %v", err)
	}
	defer resolverClient.Close()

	// create grpc server with resolver for name resolution
	grpcServer, err := resolverClient.NewServer(conf.LocalAddr, conf.Domains, true)
	if err != nil {
		logger.Fatal().Err(err).Msg("cannot create server")
	}
	addr := grpcServer.GetAddr()
	l2 = _logger.With().Timestamp().Str("addr", addr).Logger() //.Output(output)
	logger = &l2

	dbClients, err := resolver.NewClients[mediaserverproto.DatabaseClient](
		resolverClient,
		mediaserverproto.NewDatabaseClient,
		mediaserverproto.Database_ServiceDesc.ServiceName, conf.Domains)
	if err != nil {
		logger.Panic().Msgf("cannot create mediaserverdb grpc client: %v", err)
	}
	for _, dbClient := range dbClients {
		resolver.DoPing(dbClient, logger)
	}

	// register the server

	cache := actionCache.NewCache(time.Duration(conf.ActionTimeout), dbClients, conf.Domains, logger)
	defer func() {
		if err := cache.Close(); err != nil {
			logger.Error().Err(err).Msg("cannot close cache")
		}
	}()
	adService, err := actionDispatcher.NewActionDispatcher(cache, clientTLSConfig, resolverClient, time.Duration(conf.ResolverTimeout), dbClients, conf.Domains, logger)
	if err != nil {
		logger.Fatal().Err(err).Msg("cannot create action dispatcher service")
	}
	mediaserverproto.RegisterActionDispatcherServer(grpcServer, adService)

	acService, err := actionController.NewActionController(cache, dbClients, logger)
	if err != nil {
		logger.Fatal().Err(err).Msg("cannot create action controller service")
	}
	mediaserverproto.RegisterActionServer(grpcServer, acService)

	grpcServer.Startup()
	done := make(chan os.Signal, 1)
	signal.Notify(done, syscall.SIGINT, syscall.SIGTERM, syscall.SIGKILL)
	fmt.Println("press ctrl+c to stop server")
	s := <-done
	fmt.Println("got signal:", s)

	defer grpcServer.GracefulStop()

}
