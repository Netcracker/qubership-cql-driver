package mocks

import (
	cql "github.com/Netcracker/base/qubership-cql-driver"
	"github.com/gocql/gocql"
)

type TestCluster struct {
}

func (c *TestCluster) CreateSession() (session cql.Session, err error) {
	return &TestSession{}, nil
}

type TestSession struct {
	cql.Session
}

func (s *TestSession) Query(stmt string, values ...interface{}) cql.QueryInterface {
	return &TestQuery{}
}

func (s *TestSession) Close() {

}

func (s *TestSession) SetConsistency(consistency gocql.Consistency) {

}

type TestQuery struct {
}

func (q *TestQuery) Iter() cql.IterInterface {
	return &TestIter{}
}

func (q *TestQuery) Exec(panic bool) error {
	return nil
}

func (q *TestQuery) ExecWithRetry(panicIfError bool, interval, timeout int) error {
	return nil
}

type TestIter struct {
}

func (i *TestIter) Scan(dest ...interface{}) bool {
	return false
}

func (i *TestIter) Close() error {
	return nil
}

func (i *TestIter) RowData() (cql.RowData, error) {
	return cql.RowData{}, nil

}
