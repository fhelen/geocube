package grpc

import (
	"context"
	"time"

	"github.com/airbusgeo/geocube/internal/geocube"
	"github.com/airbusgeo/geocube/internal/log"
	pb "github.com/airbusgeo/geocube/internal/pb"
	internal "github.com/airbusgeo/geocube/internal/svc"
)

// GeocubeDownloaderService contains the downloader service
type GeocubeDownloaderService interface {
	// GetCubeFromMetadatas requests a cube of data from metadatas generated with a previous call to GetCube()
	GetCubeFromMetadatas(ctx context.Context, metadatas []internal.SliceMeta, grecords [][]*geocube.Record, format string) (internal.CubeInfo, <-chan internal.CubeSlice, error)
}

// DownloaderService is the GRPC service
type DownloaderService struct {
	gdsvc            GeocubeDownloaderService
	maxConnectionAge time.Duration
}

var _ pb.GeocubeDownloaderServer = &DownloaderService{}

// NewDownloader returns a new GRPC DownloaderService connected to an DownloaderService
func NewDownloader(gdsvc GeocubeDownloaderService, maxConnectionAgeSec int) *DownloaderService {
	return &DownloaderService{gdsvc: gdsvc, maxConnectionAge: time.Duration(maxConnectionAgeSec)}
}

// GetCube implements DownloaderService
func (svc *DownloaderService) GetCube(req *pb.GetCubeMetadataRequest, stream pb.GeocubeDownloader_GetCubeServer) error {
	start := time.Now()

	ctx, cancel := context.WithTimeout(stream.Context(), svc.maxConnectionAge*time.Second)
	defer func() {
		cancel()
	}()

	if len(req.GetDatasetsMeta()) != len(req.GetGroupedRecords()) {
		return newValidationError("number of datasetsMeta must be equal to the number of record lists : each datasetMeta is attached to a record list")
	}

	// Read metadatas
	sliceMetas := make([]internal.SliceMeta, 0, len(req.GetDatasetsMeta()))
	for _, metadata := range req.GetDatasetsMeta() {
		sliceMetas = append(sliceMetas, *internal.NewSlideMetaFromProtobuf(metadata))
	}

	// Read grecords
	var err error
	grecords := make([][]*geocube.Record, 0, len(req.GetGroupedRecords()))
	for _, pbgrecords := range req.GetGroupedRecords() {
		records := make([]*geocube.Record, len(pbgrecords.GetRecords()))
		for i, pbrecord := range pbgrecords.GetRecords() {
			if records[i], err = geocube.RecordFromProtobuf(pbrecord); err != nil {
				return formatError("backend.%w", err)
			}
		}
	}

	info, slicesQueue, err := svc.gdsvc.GetCubeFromMetadatas(ctx, sliceMetas, grecords, req.Format.String())
	if err != nil {
		return formatError("backend.%w", err)
	}

	// Return global header
	if err := stream.Send(&pb.GetCubeMetadataResponse{Response: &pb.GetCubeMetadataResponse_GlobalHeader{GlobalHeader: &pb.GetCubeResponseHeader{
		Count:      int64(info.NbImages),
		NbDatasets: int64(info.NbDatasets),
	}}}); err != nil {
		return formatError("backend.GetCube.%w", err)
	}

	log.Logger(ctx).Sugar().Infof("GetCube : %d images from %d datasets (%v)\n", info.NbImages, info.NbDatasets, time.Since(start))

	n := 1
	for slice := range slicesQueue {
		header, chunks := getCubeCreateResponses(&slice)

		getCubeLog(ctx, slice, header, false, n)
		n++

		responses := []*pb.GetCubeMetadataResponse{{Response: &pb.GetCubeMetadataResponse_Header{Header: header}}}
		for _, c := range chunks {
			responses = append(responses, &pb.GetCubeMetadataResponse{Response: &pb.GetCubeMetadataResponse_Chunk{Chunk: c}})
		}

		// Send responses
		for _, r := range responses {
			if err := stream.Send(r); err != nil {
				return formatError("backend.GetCube.%w", err)
			}
		}
	}

	return ctx.Err()
}
