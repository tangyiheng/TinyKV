package server

import (
	"context"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	reader, err := server.storage.Reader(req.GetContext())
	defer reader.Close()
	resp := kvrpcpb.RawGetResponse{}
	if err != nil {
		resp.Error = err.Error()
		return &resp, err
	}
	value, err := reader.GetCF(req.GetCf(), req.GetKey())
	resp.Value = value
	if value == nil {
		resp.NotFound = true
	}
	return &resp, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	// 创建Modify
	put := storage.Put{
		Key:   req.GetKey(),
		Value: req.GetValue(),
		Cf:    req.GetCf(),
	}
	modify := storage.Modify{Data: put}
	// 调用storage提供的write接口
	err := server.storage.Write(req.GetContext(), []storage.Modify{modify})
	resp := kvrpcpb.RawPutResponse{}
	if err != nil {
		resp.Error = err.Error()
		return &resp, err
	}
	return &resp, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	// 创建Modify
	del := storage.Delete{
		Key: req.GetKey(),
		Cf:  req.GetCf(),
	}
	modify := storage.Modify{Data: del}
	// 调用storage提供的write接口
	err := server.storage.Write(req.GetContext(), []storage.Modify{modify})
	resp := kvrpcpb.RawDeleteResponse{}
	if err != nil {
		resp.Error = err.Error()
		return &resp, err
	}
	return &resp, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	reader, err := server.storage.Reader(req.GetContext())
	defer reader.Close()
	resp := kvrpcpb.RawScanResponse{}
	if err != nil {
		resp.Error = err.Error()
		return &resp, err
	}
	// 创建用于遍历kv对的迭代器
	iter := reader.IterCF(req.GetCf())
	var Kvs []*kvrpcpb.KvPair
	var cnt uint32 = 0
	for iter.Seek(req.StartKey); iter.Valid() && cnt < req.Limit; iter.Next() {
		item := iter.Item()
		var k, v []byte
		k = item.KeyCopy(k)
		v, _ = item.ValueCopy(v)
		Kvs = append(Kvs, &kvrpcpb.KvPair{
			Error: nil, // KeyError?
			Key:   k,
			Value: v,
		})
		cnt++
	}
	iter.Close()
	resp.Kvs = Kvs
	return &resp, nil
}
