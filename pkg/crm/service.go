package crm

import (
	"github.com/buger/jsonparser"
	"github.com/deepfabric/busybee/pkg/pb/metapb"
	"github.com/deepfabric/busybee/pkg/storage"
	"github.com/fagongzi/util/hack"
)

// Service crm service
type Service interface {
	UpdateMapping(uint64, ...metapb.IDValue) error
	GetIDValue(uint64, metapb.IDValue, uint32) ([]byte, error)
	UpdateProfile(uint64, uint32, []byte) error
	GetProfileField(uint64, uint32, string) ([]byte, error)
}

// NewService returns crm service
func NewService(store storage.Storage) Service {
	return &service{
		store: store,
	}
}

type service struct {
	store storage.Storage
}

func (s *service) UpdateMapping(tid uint64, values ...metapb.IDValue) error {
	n := len(values)
	for i := 0; i < n; i++ {
		for j := 0; j < n; j++ {
			if i == j {
				continue
			}

			err := s.update(tid, values[i], values[j])
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (s *service) GetIDValue(tid uint64, from metapb.IDValue, to uint32) ([]byte, error) {
	value, err := s.store.Get(storage.MappingKey(tid, from, to))
	if err != nil {
		return nil, err
	}

	return value, nil
}

func (s *service) UpdateProfile(tid uint64, uid uint32, value []byte) error {
	return s.store.Set(storage.ProfileKey(tid, uid), value)
}

func (s *service) GetProfileField(tid uint64, uid uint32, field string) ([]byte, error) {
	value, err := s.store.Get(storage.ProfileKey(tid, uid))
	if err != nil {
		return nil, err
	}

	if field == "" {
		return value, nil
	}

	return extractAttrValue(value, field), nil
}

func (s *service) update(tid uint64, from, to metapb.IDValue) error {
	return s.store.Set(storage.MappingKey(tid, from, to.Type), hack.StringToSlice(to.Value))
}

func extractAttrValue(src []byte, paths ...string) []byte {
	value, _, _, err := jsonparser.Get(src, paths...)
	if err != nil {
		return nil
	}

	return value
}
