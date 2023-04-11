package options

import "fmt"

type SearchOptions struct {
	Fields map[string]interface{}
}

func NewSearchOptions(fields map[string]interface{}) SearchOptions {
	return SearchOptions{Fields: fields}
}

type ListOptions struct {
	FieldMap map[string]interface{}
	Fields   []interface{}
	OrderBy  string
	Limit    int
	Page     int
}

func NewListOptions(page, limit int) *ListOptions {
	if page == 0 {
		page = 1
	}
	if limit == 0 {
		limit = 10
	}
	return &ListOptions{Limit: limit, Page: page}
}

func (l *ListOptions) Asc(field ...string) *ListOptions {
	ob := "id"
	if len(field) > 0 && field[0] != "" {
		ob = field[0]
	}
	l.OrderBy = fmt.Sprintf("%s ASC", ob)
	return l
}

func (l *ListOptions) Desc(field ...string) *ListOptions {
	ob := "id"
	if len(field) > 0 && field[0] != "" {
		ob = field[0]
	}
	l.OrderBy = fmt.Sprintf("%s DESC", ob)
	return l
}

func (l *ListOptions) WithFields(fields []interface{}) *ListOptions {
	l.Fields = fields
	return l
}
