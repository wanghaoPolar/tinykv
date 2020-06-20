package standalone_storage

import (
	"log"

	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	db *badger.DB
}

// NewStandAloneStorage create a new instance of StandAloneStorage and return the pointer
func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	options := badger.DefaultOptions
	options.Dir = conf.DBPath
	options.ValueDir = conf.DBPath
	db, err := badger.Open(options)
	if err != nil {
		log.Fatal(err)
	}
	return &StandAloneStorage{db}
}

// Start init the storage
func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

// Stop close all related data
func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	s.db.Close()
	return nil
}

// Reader return StorageReader{GetCF, IterCF, Close}
func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	txn := s.db.NewTransaction(false)
	reader := &StandAloneStorageReader{txn}
	return reader, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	for _, m := range batch {
		switch m.Data.(type) {
		case storage.Put:
			{
				err := engine_util.PutCF(s.db, m.Cf(), m.Key(), m.Value())
				if err != nil {
					return err
				}
			}
		case storage.Delete:
			{
				err := engine_util.DeleteCF(s.db, m.Cf(), m.Key())
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// StandAloneStorageReader implement StorageReader
type StandAloneStorageReader struct {
	txn *badger.Txn
}

// GetCF return the value of a key
func (sr *StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	value, err := engine_util.GetCFFromTxn(sr.txn, cf, key)
	if err != nil {
		if err.Error() == badger.ErrKeyNotFound.Error() {
			return nil, nil
		}
		return nil, err
	}
	return value, nil
}

// IterCF return a CFIterator
func (sr *StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	iterator := engine_util.NewCFIterator(cf, sr.txn)
	return iterator
}

// Close commit the transaction
func (sr *StandAloneStorageReader) Close() {
	sr.txn.Discard()
	sr.txn.Commit()
}
