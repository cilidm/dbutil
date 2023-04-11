package mysql

import (
	"github.com/cilidm/dbutil/options"
	"strings"
	"sync"

	"gorm.io/gorm"
)

type RepoDB[T any] struct {
	opt     options.SearchOptions
	preload string
	orderBy string
	lock    sync.Mutex
	table   string
}

func MustNew[T any]() *RepoDB[T] {
	return &RepoDB[T]{}
}

func (r *RepoDB[T]) SetTable(table string) *RepoDB[T] {
	r.table = table
	return r
}

func (r *RepoDB[T]) SetPreLoad(pre string) *RepoDB[T] {
	r.preload = pre
	return r
}

func (r *RepoDB[T]) SetOptionsByMap(data map[string]interface{}) *RepoDB[T] {
	r.lock.Lock()
	defer r.lock.Unlock()
	r.opt.Fields = data
	return r
}

func (r *RepoDB[T]) SetOptions(key string, val interface{}) *RepoDB[T] {
	r.lock.Lock()
	defer r.lock.Unlock()
	r.opt.Fields[key] = val
	return r
}

func (r *RepoDB[T]) SetOrderBy(orderBy string) *RepoDB[T] {
	r.orderBy = orderBy
	return r
}

func (r *RepoDB[T]) First() (T, error) {
	r.lock.Lock()
	defer r.lock.Unlock()
	var ret T
	query := DB().GetDB().Debug()
	for k, v := range r.opt.Fields {
		query = query.Where(k, v)
	}
	if r.preload != "" {
		query = query.Preload(r.preload)
	}
	if r.orderBy != "" {
		query = query.Order(r.orderBy)
	}
	err := query.First(&ret).Error
	if err != nil {
		if gorm.ErrRecordNotFound == err {
			return ret, nil
		}
		return ret, err
	}
	return ret, nil
}

func (r *RepoDB[T]) Find() ([]T, error) {
	r.lock.Lock()
	defer r.lock.Unlock()
	var ret []T
	query := DB().GetDB().Debug()
	for k, v := range r.opt.Fields {
		query = query.Where(k, v)
	}
	err := query.Find(&ret).Error
	if err != nil {
		if gorm.ErrRecordNotFound == err {
			return ret, nil
		}
		return ret, err
	}
	return ret, nil
}

func (r *RepoDB[T]) Count() (int64, error) {
	r.lock.Lock()
	defer r.lock.Unlock()
	var ret int64
	query := DB().GetDB().Debug().Table(r.table)
	for k, v := range r.opt.Fields {
		query = query.Where(k, v)
	}
	err := query.Count(&ret).Error
	if err != nil {
		if gorm.ErrRecordNotFound == err {
			return 0, nil
		}
		return 0, err
	}
	return ret, nil
}

func (r *RepoDB[T]) FindByPage(options *options.ListOptions) ([]T, int64, error) {
	r.lock.Lock()
	defer r.lock.Unlock()
	var (
		ret   []T
		count int64
	)
	query := DB().GetDB().Table(r.table).Debug()
	if len(options.FieldMap) > 0 {
		for k, v := range options.FieldMap {
			query = query.Where(k, v)
		}
	}

	if len(options.Fields) > 0 {
		var keys []string
		var values []interface{}
		l := len(options.Fields)
		for k := 0; k < l; k += 2 {
			if fieldStr, ok := options.Fields[k].(string); ok {
				keys = append(keys, fieldStr)
			}
			values = append(values, options.Fields[k+1])
		}
		query = query.Where(strings.Join(keys, " AND "), values...)
	}

	offset := (options.Page - 1) * options.Limit
	err := query.Count(&count).Offset(offset).Limit(options.Limit).Order(options.OrderBy).Find(&ret).Error
	if err != nil {
		return ret, count, err
	}
	return ret, count, nil
}

func (r *RepoDB[T]) List(options *options.ListOptions) ([]T, error) {
	r.lock.Lock()
	defer r.lock.Unlock()
	var ret []T
	query := DB().GetDB()
	for k, v := range options.FieldMap {
		query = query.Where(k, v)
	}
	err := query.Offset(options.Page).Limit(options.Limit).Order(options.OrderBy).
		Find(&ret).Error
	if err != nil {
		return ret, err
	}
	return ret, nil
}

func (r *RepoDB[T]) Create(obj T) error {
	r.lock.Lock()
	defer r.lock.Unlock()
	if err := DB().GetDB().Create(obj).Error; err != nil {
		return err
	}
	return nil
}

func (r *RepoDB[T]) Delete(obj T) error {
	r.lock.Lock()
	defer r.lock.Unlock()
	query := DB().GetDB()
	for k, v := range r.opt.Fields {
		query = query.Where(k, v)
	}
	if err := query.Delete(obj).Error; err != nil {
		return err
	}
	return nil
}

func (*RepoDB[T]) DeleteByKV(key string, val interface{}, obj T) error {
	if err := DB().GetDB().Where(key, val).Delete(obj).Error; err != nil {
		return err
	}
	return nil
}

func (r *RepoDB[T]) Update(key, value string, attr map[string]interface{}, obj T) error {
	if err := DB().GetDB().Model(obj).Where(key, value).Updates(attr).Error; err != nil {
		return err
	}
	return nil
}

func (r *RepoDB[T]) Expr(obj T, key, value string, column, expr string, num int) error {
	r.lock.Lock()
	defer r.lock.Unlock()
	if err := DB().GetDB().Model(obj).Where(key, value).UpdateColumn(column, gorm.Expr(expr, num)).
		Error; err != nil {
		return err
	}
	return nil
}

func (r *RepoDB[T]) TxCreate(obj T, txDB *gorm.DB) error {
	if txDB == nil {
		txDB = DB().GetDB()
	}
	if err := txDB.Create(obj).Error; err != nil {
		return err
	}
	return nil
}

func (r *RepoDB[T]) TxUpdate(key, value string, attr map[string]interface{}, obj T, txDB *gorm.DB) error {
	if txDB == nil {
		txDB = DB().GetDB()
	}
	if err := txDB.Model(obj).Where(key, value).Updates(attr).Error; err != nil {
		return err
	}
	return nil
}

func (r *RepoDB[T]) TxDelete(obj T, txDB *gorm.DB) error {
	r.lock.Lock()
	defer r.lock.Unlock()
	if txDB == nil {
		txDB = DB().GetDB()
	}
	query := txDB
	for k, v := range r.opt.Fields {
		query = query.Where(k, v)
	}
	if err := query.Delete(obj).Error; err != nil {
		return err
	}
	return nil
}

func (r *RepoDB[T]) TxExpr(obj T, key, value string, column, expr string, num int, txDB *gorm.DB) error {
	if txDB == nil {
		txDB = DB().GetDB()
	}
	r.lock.Lock()
	defer r.lock.Unlock()
	if err := txDB.Model(obj).Where(key, value).UpdateColumn(column, gorm.Expr(expr, num)).
		Error; err != nil {
		return err
	}
	return nil
}

func (r *RepoDB[T]) FindByRaw(sql string) ([]T, error) {
	var ret []T
	err := DB().GetDB().Raw(sql).Scan(&ret).Error
	if err != nil {
		return nil, err
	}
	return ret, nil
}
