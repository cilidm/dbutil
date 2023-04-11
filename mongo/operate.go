package mongo

import (
	"github.com/cilidm/dbutil/options"

	"github.com/globalsign/mgo/bson"
	"github.com/tal-tech/go-zero/core/stores/mongo"
)

type Model[T any] struct {
	*mongo.Model
}

func MustNew[T any](url, collection string) *Model[T] {
	return &Model[T]{
		Model: mongo.MustNewModel(url, collection),
	}
}

func (m *Model[T]) Query(attr map[string]interface{}) (T, error) {
	var entity T
	session, err := m.TakeSession()
	if err != nil {
		return entity, err
	}

	defer m.PutSession(session)
	err = m.GetCollection(session).Find(attr).One(&entity)

	switch err {
	case nil:
		return entity, nil
	case mongo.ErrNotFound:
		return entity, nil
	default:
		return entity, err
	}
}

func (m *Model[T]) Find(options *options.ListOptions) (ret []T, count int, err error) {
	session, err := m.TakeSession()
	if err != nil {
		return
	}

	defer m.PutSession(session)
	query := m.GetCollection(session).Find(options.FieldMap)
	count, err = query.Count()
	if err != nil {
		return
	}
	err = query.Sort(options.OrderBy).Skip(options.Limit * (options.Page - 1)).Limit(options.Limit).All(&ret)
	switch err {
	case nil:
		return
	case mongo.ErrNotFound:
		err = nil
		return
	default:
		return
	}
}

func (m *Model[T]) Insert(data T) error {
	session, err := m.TakeSession()
	if err != nil {
		return err
	}

	defer m.PutSession(session)
	return m.GetCollection(session).Insert(data)
}

func (m *Model[T]) FindOne(id string) (T, error) {
	var data T

	hexID := bson.ObjectIdHex(id)
	session, err := m.TakeSession()
	if err != nil {
		return data, err
	}

	defer m.PutSession(session)

	err = m.GetCollection(session).FindId(hexID).One(&data)
	switch err {
	case nil:
		return data, nil
	case mongo.ErrNotFound:
		return data, nil
	default:
		return data, err
	}
}

func (m *Model[T]) Update(attr map[string]interface{}, data T) error {
	session, err := m.TakeSession()
	if err != nil {
		return err
	}

	defer m.PutSession(session)

	return m.GetCollection(session).Update(attr, data)
}

func (m *Model[T]) Delete(id string) error {
	session, err := m.TakeSession()
	if err != nil {
		return err
	}

	defer m.PutSession(session)

	return m.GetCollection(session).RemoveId(bson.ObjectIdHex(id))
}
