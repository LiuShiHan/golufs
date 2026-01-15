package main

import (
	"context"
	"errors"
	"fmt"
	pb "github.com/LiuShiHan/golufs/pb/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type FileReaderServer struct {
	pb.UnimplementedFileReaderServer
	config    *ServerConfig
	mu        sync.RWMutex
	fileCache map[string]*FileCache
}

type FileCache struct {
	info      os.FileInfo
	lastCheck time.Time
	ttl       time.Duration
}

func NewFileReaderServer(config *ServerConfig) *FileReaderServer {
	return &FileReaderServer{
		config:    config,
		fileCache: make(map[string]*FileCache),
	}
}

func (s *FileReaderServer) ReadFile(req *pb.ReadFileRequest, server pb.FileReader_ReadFileServer) error {
	ctx := server.Context()
	safePath, err := s.validateAndResolvePath(req.Path)
	if err != nil {
		return status.Error(codes.InvalidArgument, fmt.Sprintf("invalid path: %v", err))
	}

	fileInfo, err := os.Stat(safePath)
	if err != nil {
		if os.IsNotExist(err) {
			return status.Error(codes.NotFound, fmt.Sprintf("file %s does not exist", req.Path))
		}
	}

	if fileInfo.IsDir() {
		return status.Error(codes.InvalidArgument, fmt.Sprintf("%s is a directory", req.Path))
	}

	chunkSize := int(req.ChunkSize)
	if chunkSize <= 0 {
		chunkSize = s.config.MaxChunkSize
	}

	file, err := os.Open(safePath)
	if err != nil {
		return status.Error(codes.NotFound, fmt.Sprintf("file %s does not exist", req.Path))
	}
	defer file.Close()

	// 这tm是为了断点续传 不一定云的文件系统有
	offset := req.Offset
	if offset > 0 {
		_, err = file.Seek(offset, io.SeekStart)
		if err != nil {
			return status.Error(codes.Internal, fmt.Sprintf("seek file failed: %v", err))
		}
	}

	var readLength int64 = fileInfo.Size() - offset
	if req.Length > 0 && req.Length < readLength {
		readLength = req.Length
	}

	buffer := make([]byte, chunkSize)
	totalRead := int64(0)
	for totalRead < readLength {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		n, err := file.Read(buffer[:chunkSize])
		if err != nil {
			if err == io.EOF {
				break
			}
			return status.Error(codes.Internal, fmt.Sprintf("read file failed: %v", err))
		}
		if n == 0 {
			break
		}
		totalRead += int64(n)

		resp := &pb.ReadFileResponse{
			Content:     buffer[:n],
			Offset:      offset + totalRead - int64(n), //这个是这个数据的起始点
			TotalSize:   fileInfo.Size(),
			IsLastChunk: totalRead >= readLength,
		}

		if err := server.Send(resp); err != nil {
			return status.Error(codes.Internal, fmt.Sprintf("send response failed: %v", err))
		}

	}
	return nil

}

func (s *FileReaderServer) ListDirectory(ctx context.Context, req *pb.ListDirectoryRequest) (*pb.ListDirectoryResponse, error) {
	safePath, err := s.validateAndResolvePath(req.Path)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, fmt.Sprintf("invalid path: %v", err))
	}

	entries, err := os.ReadDir(safePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, status.Error(codes.NotFound, fmt.Sprintf("directory %s does not exist", req.Path))
		}
		return nil, status.Error(codes.Internal, fmt.Sprintf("read directory failed: %v", err))
	}

	files := make([]*pb.FileInfoResponse, 0, len(entries))
	for _, entry := range entries {
		info, err := entry.Info()
		if err != nil {
			continue
		}
		files = append(files, &pb.FileInfoResponse{
			Name:         info.Name(),
			Size:         info.Size(),
			ModifiedTime: timestamppb.New(info.ModTime()),
			IsDirectory:  info.IsDir(),
			Permissions:  info.Mode().String(),
		})

	}
	return &pb.ListDirectoryResponse{Files: files}, nil
}

func (s *FileReaderServer) HealthCheck(ctx context.Context, req *pb.HealthRequest) (*pb.HealthResponse, error) {
	return &pb.HealthResponse{
		Healthy: true,
		Message: "service is healthy",
	}, nil
}

func (s *FileReaderServer) validateAndResolvePath(requestPath string) (string, error) {
	clearPath := filepath.Clean(requestPath)
	if filepath.IsAbs(clearPath) {
		return "", errors.New("absolute paths are not allowed")
	}

	if strings.HasPrefix(clearPath, "..") {
		return "", errors.New("invalid path")
	}

	resolverPath := filepath.Join(s.config.DataDir, clearPath)

	absResolverPath, err := filepath.Abs(s.config.DataDir)
	if err != nil {
		return "", fmt.Errorf("resolve path error: %v", err)
	}

	if !strings.HasPrefix(resolverPath, absResolverPath) {
		return "", errors.New("path outside of data dir")
	}
	return resolverPath, nil

}
