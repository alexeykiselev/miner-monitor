package main

import (
	"github.com/dgraph-io/badger/v3"
	"github.com/fxamacker/cbor/v2"
	"github.com/pkg/errors"
)

type storage struct {
	db *badger.DB
}

func newStorage(path string) (*storage, error) {
	opts := badger.DefaultOptions(path)
	opts.Logger = nil
	if len(path) == 0 {
		opts = opts.WithInMemory(true)
	}
	db, err := badger.Open(opts)
	if err != nil {
		return nil, errors.Wrap(err, "failed to open storage")
	}
	return &storage{db: db}, nil
}

func (s *storage) close() error {
	return s.db.Close()
}

func (s *storage) updateAccount(address []byte, balance int64) error {
	txn := s.db.NewTransaction(true)
	defer txn.Discard()

	data, err := cbor.Marshal(balance)
	if err != nil {
		return err
	}
	err = txn.Set(address, data)
	if err != nil {
		return err
	}
	if err = txn.Commit(); err != nil {
		return err
	}
	return nil
}

func (s *storage) setGenerator(h int32, address []byte) error {

}