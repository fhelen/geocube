package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/airbusgeo/godal"
	"github.com/airbusgeo/osio"
	osioGcs "github.com/airbusgeo/osio/gcs"
	"go.uber.org/zap"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"

	geogrpc "github.com/airbusgeo/geocube/internal/grpc"
	"github.com/airbusgeo/geocube/internal/log"
	pb "github.com/airbusgeo/geocube/internal/pb"
	"github.com/airbusgeo/geocube/internal/svc"
)

func main() {
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt)
	ctx, cancel := context.WithCancel(context.Background())
	runerr := make(chan error)

	go func() {
		runerr <- run(ctx)
	}()

	for {
		select {
		case err := <-runerr:
			if err != nil {
				log.Logger(ctx).Fatal("run error", zap.Error(err))
			}
			return
		case <-quit:
			cancel()
			go func() {
				time.Sleep(30 * time.Second)
				runerr <- fmt.Errorf("did not terminate after 30 seconds")
			}()
		}
	}
}

func run(ctx context.Context) error {
	downloaderConfig, err := newDownloaderAppConfig()
	if err != nil {
		return err
	}

	if err := initGDAL(ctx, downloaderConfig); err != nil {
		return fmt.Errorf("init gdal: %w", err)
	}

	// Create Geocube Service
	svc, err := svc.New(ctx, nil, nil, nil, "", "", downloaderConfig.CatalogWorkers)
	if err != nil {
		return fmt.Errorf("svc.new: %w", err)
	}

	grpcServer := newGrpcServer(svc, downloaderConfig.MaxConnectionAge)

	log.Logger(ctx).Info("Geocube v" + geogrpc.GeocubeServerVersion)

	muxHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		grpcServer.ServeHTTP(w, r)
	})

	srv := &http.Server{
		Addr:    fmt.Sprintf(":%s", downloaderConfig.AppPort),
		Handler: h2c.NewHandler(muxHandler, &http2.Server{}),
	}

	go func() {
		var err error
		if downloaderConfig.Local {
			err = srv.ListenAndServe()
		} else {
			err = srv.ListenAndServeTLS("/tls/tls.crt", "/tls/tls.key")
		}
		if err != nil && err != http.ErrServerClosed {
			log.Logger(ctx).Fatal("srv.ListenAndServe", zap.Error(err))
		}
	}()

	<-ctx.Done()
	sctx, cncl := context.WithTimeout(context.Background(), 30*time.Second)
	defer cncl()
	return srv.Shutdown(sctx)
}

func initGDAL(ctx context.Context, serverConfig *serverConfig) error {
	os.Setenv("GDAL_DISABLE_READDIR_ON_OPEN", "EMPTY_DIR")

	godal.RegisterAll()
	if err := godal.RegisterRaster("PNG"); err != nil {
		return err
	}
	osioGCSHandle, err := osioGcs.Handle(ctx)
	if err != nil {
		return err
	}
	gcsa, err := osio.NewAdapter(osioGCSHandle,
		osio.BlockSize(serverConfig.GdalBlockSize),
		osio.NumCachedBlocks(serverConfig.GdalNumCachedBlocks))
	if err != nil {
		return err
	}
	return godal.RegisterVSIAdapter("gs://", gcsa)
}

func getMaxConnectionAge(maxConnectionAge int) int {
	if maxConnectionAge < 60 {
		maxConnectionAge = 15 * 60
	}
	return maxConnectionAge
}

func newGrpcServer(svc geogrpc.GeocubeDownloaderService, maxConnectionAgeValue int) *grpc.Server {
	opts := []grpc.ServerOption{
		grpc.KeepaliveParams(keepalive.ServerParameters{
			MaxConnectionAge:      time.Duration(getMaxConnectionAge(maxConnectionAgeValue)) * time.Second,
			MaxConnectionAgeGrace: time.Minute})}

	grpcServer := grpc.NewServer(opts...)
	pb.RegisterGeocubeDownloaderServer(grpcServer, geogrpc.NewDownloader(svc, getMaxConnectionAge(maxConnectionAgeValue)))
	return grpcServer
}

func newDownloaderAppConfig() (*serverConfig, error) {
	local := flag.Bool("local", false, "execute geocube downloader in local environment")
	listenPort := flag.String("port", "8080", "geocube downloader port to use")
	maxConnectionAge := flag.Int("maxConnectionAge", 0, "grpc max age connection")
	workers := flag.Int("workers", 1, "number of parallel workers per catalog request")
	gdalBlocksize := flag.String("gdalBlockSize", "1Mb", "gdal blocksize value (default 1Mb)")
	gdalNumCachedBlocks := flag.Int("gdalNumCachedBlocks", 500, "gdal blockcache value (default 500)")

	flag.Parse()

	if *listenPort == "" {
		return nil, fmt.Errorf("failed to initialize --port application flag")
	}

	return &serverConfig{
		Local:               *local,
		AppPort:             *listenPort,
		MaxConnectionAge:    *maxConnectionAge,
		CatalogWorkers:      *workers,
		GdalBlockSize:       *gdalBlocksize,
		GdalNumCachedBlocks: *gdalNumCachedBlocks,
	}, nil
}

type serverConfig struct {
	Local               bool
	AppPort             string
	MaxConnectionAge    int
	CatalogWorkers      int
	GdalBlockSize       string
	GdalNumCachedBlocks int
}
