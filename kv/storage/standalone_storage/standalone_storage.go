package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
// StandAloneStorage 是单节点 TinyKV 实例的 Storage 实现。它不与其他节点通信，所有数据都存储在本地。
type StandAloneStorage struct {
	// Your Data Here (1).
	db *badger.DB
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	standAloneStorage := StandAloneStorage{}
	options := badger.DefaultOptions
	options.Dir = conf.DBPath
	options.ValueDir = conf.DBPath
	db, err := badger.Open(options)
	if err != nil {
		panic(err)
	}
	standAloneStorage.db = db
	return &standAloneStorage
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	err := s.db.Close()
	if err != nil {
		return err
	}
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	txn := s.db.NewTransaction(false)
	return NewStandAloneStorageReader(txn), nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	txn := s.db.NewTransaction(true)
	// 丢弃已创建的事务
	defer txn.Discard()
	for _, m := range batch {
		switch m.Data.(type) {
		case storage.Put:
			p := m.Data.(storage.Put)
			// 添加一条KV数据到BadgerDB中
			err := txn.Set(engine_util.KeyWithCF(p.Cf, p.Key), p.Value)
			if err != nil {
				return err
			}
		case storage.Delete:
			d := m.Data.(storage.Delete)
			// 从BadgerDB中删除一条KV数据
			err := txn.Delete(engine_util.KeyWithCF(d.Cf, d.Key))
			if err != nil {
				return err
			}
		}
	}
	// 提交事务
	err := txn.Commit()
	if err != nil {
		return err
	}
	return nil
}

// StandAloneStorageReader is a StorageReader which reads from a StandAloneStorage.
type StandAloneStorageReader struct {
	txn *badger.Txn
}

func NewStandAloneStorageReader(txn *badger.Txn) *StandAloneStorageReader {
	return &StandAloneStorageReader{txn: txn}
}

func (r *StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	val, err := engine_util.GetCFFromTxn(r.txn, cf, key)
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	return val, err
}

func (r *StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, r.txn)
}

func (r *StandAloneStorageReader) Close() {
	// 调用 badger.Txn 的 Discard() 并关闭所有迭代器
	r.txn.Discard()
}
