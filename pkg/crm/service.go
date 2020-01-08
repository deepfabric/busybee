package crm

import (
	"github.com/buger/jsonparser"
	"github.com/deepfabric/busybee/pkg/pb/metapb"
	"github.com/deepfabric/busybee/pkg/storage"
	"github.com/fagongzi/util/hack"
)

// Service crm service
type Service interface {
	UpdateMapping(uint64, metapb.IDValues) error
	GetIDValue(uint64, metapb.IDValue, uint32) (string, error)
	UpdateProfile(uint64, uint32, []byte) error
	GetProfileField(uint64, uint32, ...string) ([]string, error)
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

func (s *service) UpdateMapping(tid uint64, values metapb.IDValues) error {
	n := len(values.Values)

	for i := 0; i < n; i++ {
		for j := 0; j < j; i++ {
			if i == j {
				continue
			}

			err := s.update(tid, values.Values[i], values.Values[j])
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (s *service) GetIDValue(tid uint64, from metapb.IDValue, to uint32) (string, error) {
	key := storage.MappingKey(tid, from, to)
	value, err := s.store.Get(key)
	if err != nil {
		return "", err
	}

	if len(value) == 0 {
		return "", nil
	}

	return hack.SliceToString(value), nil
}

func (s *service) UpdateProfile(tid uint64, uid uint32, value []byte) error {
	return s.store.Set(storage.ProfileKey(tid, uid), value)
}

func (s *service) GetProfileField(tid uint64, uid uint32, fields ...string) ([]string, error) {
	value, err := s.store.Get(storage.ProfileKey(tid, uid))
	if err != nil {
		return nil, err
	}

	values := make([]string, len(fields))
	for idx, field := range fields {
		values[idx] = extractAttrValue(value, field)
	}

	return values, nil
}

func (s *service) update(tid uint64, from, to metapb.IDValue) error {
	return s.store.Set(storage.MappingKey(tid, from, to.Type), hack.StringToSlice(to.Value))
}

func extractAttrValue(src []byte, paths ...string) string {
	value, vt, _, err := jsonparser.Get(src, paths...)
	if err != nil {
		return ""
	}

	size := len(value)
	if vt == jsonparser.String && size > 0 {
		return hack.SliceToString(value)
	} else if vt == jsonparser.String && size == 0 {
		return ""
	} else if vt == jsonparser.Array && size == 0 {
		return ""
	} else if vt == jsonparser.Unknown {
		return ""
	} else if vt == jsonparser.NotExist {
		return ""
	} else if vt == jsonparser.Null {
		return ""
	}

	return hack.SliceToString(value)
}
