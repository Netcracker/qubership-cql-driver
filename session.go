package cql

import (
	"sync"
	"time"

	"github.com/gocql/gocql"
)

type Cluster interface {
	CreateSession() (Session, error)
}

type ClusterImpl struct {
	cluster *gocql.ClusterConfig
}

func (c *ClusterImpl) CreateSession() (Session, error) {
	cqlSession, err := c.cluster.CreateSession()
	if err != nil {
		return nil, err
	}

	return &SessionImpl{cqlSession}, nil
}

type Session interface {
	Query(string, ...interface{}) QueryInterface
	SetConsistency(consistency gocql.Consistency)
	Close()
}

type SessionImpl struct {
	session *gocql.Session
}

func (s *SessionImpl) Query(stmt string, values ...interface{}) QueryInterface {
	return &Query{s.session.Query(stmt, values...)}
}

func (s *SessionImpl) SetConsistency(consistency gocql.Consistency) {
	s.session.SetConsistency(consistency)
}

func (s *SessionImpl) Close() {
	if s.session != nil {
		s.session.Close()
	}
}

var once sync.Once
var session Session
var mu sync.Mutex

func createSession(cluster Cluster) (Session, error) {
	var session Session
	var err error
	for try := 0; try < 3; try++ {
		// cluster.Timeout = time.Duration(int(cluster.Timeout.Seconds())*(try+1)) * time.Second
		session, err = cluster.CreateSession()
		if err == nil {
			break
		}
		time.Sleep(time.Duration(2*(try+1)) * time.Second)
	}
	if err != nil {
		return nil, err
	}
	return session, nil
}

func GetSession(cluster Cluster, consistencyLevel gocql.Consistency) (Session, error) {
	var err error
	mu.Lock()
	defer mu.Unlock()
	if session == nil {
		session, err = createSession(cluster)
		if err != nil {
			return nil, err
		}
	}
	session.SetConsistency(consistencyLevel)
	return session, nil
}

func ExecInAutoCloseSession(cluster Cluster, sessionFunc func(session Session) error) error {
	autoCloseSession, err := createSession(cluster)
	if err != nil {
		return err
	}
	defer autoCloseSession.Close()
	return sessionFunc(autoCloseSession)
}
