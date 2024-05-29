package main

import (
	"flag"
	"fmt"
	"github.com/je4/mediaserveraction/v2/config"
	"github.com/je4/mediaserveraction/v2/pkg/actionCache"
	"github.com/je4/mediaserveraction/v2/pkg/actionController"
	"github.com/je4/mediaserveraction/v2/pkg/actionDispatcher"
	mediaserverproto "github.com/je4/mediaserverproto/v2/pkg/mediaserver/proto"
	resolver "github.com/je4/miniresolver/v2/pkg/resolver"
	"github.com/je4/trustutil/v2/pkg/certutil"
	"github.com/je4/trustutil/v2/pkg/loader"
	configutil "github.com/je4/utils/v2/pkg/config"
	"github.com/je4/utils/v2/pkg/zLogger"
	"github.com/rs/zerolog"
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

	hostname, err := os.Hostname()
	if err != nil {
		log.Fatalf("cannot get hostname: %v", err)
	}

	output := zerolog.ConsoleWriter{Out: out, TimeFormat: time.RFC3339}
	_logger := zerolog.New(output).With().Timestamp().Str("service", "mediaserveraction"). /*.Array("addrs", zLogger.StringArray(addrStr))*/ Str("host", hostname).Str("addr", conf.LocalAddr).Logger()
	_logger.Level(zLogger.LogLevel(conf.LogLevel))
	var logger zLogger.ZLogger = &_logger

	clientTLSConfig, clientLoader, err := loader.CreateClientLoader(conf.ClientTLS, logger)
	if err != nil {
		logger.Panic().Msgf("cannot create client loader: %v", err)
	}
	defer clientLoader.Close()

	// create TLS Certificate.
	// the certificate MUST contain <package>.<service> as DNS name
	certutil.AddDefaultDNSNames(mediaserverproto.Action_ServiceDesc.ServiceName, mediaserverproto.ActionDispatcher_ServiceDesc.ServiceName)
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

	dbClient, err := resolver.NewClient[mediaserverproto.DatabaseClient](resolverClient, mediaserverproto.NewDatabaseClient, mediaserverproto.Database_ServiceDesc.ServiceName)
	if err != nil {
		logger.Panic().Msgf("cannot create mediaserverdb grpc client: %v", err)
	}
	resolver.DoPing(dbClient, logger)

	// create grpc server with resolver for name resolution
	grpcServer, err := resolverClient.NewServer(conf.LocalAddr)
	if err != nil {
		logger.Fatal().Err(err).Msg("cannot create server")
	}

	// register the server

	cache := actionCache.NewCache(time.Duration(conf.ActionTimeout), dbClient, logger)
	adService, err := actionDispatcher.NewActionDispatcher(cache, clientTLSConfig, time.Duration(conf.ResolverTimeout), dbClient, logger)
	if err != nil {
		logger.Fatal().Err(err).Msg("cannot create action dispatcher service")
	}
	mediaserverproto.RegisterActionDispatcherServer(grpcServer, adService)

	acService, err := actionController.NewActionController(cache, dbClient, logger)
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
