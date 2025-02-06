package cql

import (
	"time"

	"github.com/aws/aws-sigv4-auth-cassandra-gocql-driver-plugin/sigv4"
	"github.com/gocql/gocql"
)

type ClusterBuilder interface {
	Build() Cluster
	WithHost(host ...string) ClusterBuilder
	WithPort(port int) ClusterBuilder
	WithUser(user string) ClusterBuilder
	WithPassword(password func() string) ClusterBuilder
	WithConsistency(consistency gocql.Consistency) ClusterBuilder
	WithKeyspace(keyspace string) ClusterBuilder
	WithConnectTimeout(connectTimeout int) ClusterBuilder
	WithTimeout(timeout int) ClusterBuilder
	WithTLSEnabled(tlsEnabled bool) ClusterBuilder
	WithRootCertPath(rootCertPath string) ClusterBuilder
}

type ClusterBuilderImpl struct {
	Host            []string
	Port            int
	User            string
	Password        func() string
	Keyspace        string
	DCName          string
	Consistency     gocql.Consistency
	ConnectTimeout  int
	Timeout         int
	TlsEnabled      bool
	RootCertPath    string
	AWS             bool
	AccessKeyId     string
	SecretAccessKey string
	Region          string
}

func (r *ClusterBuilderImpl) Build() Cluster {
	cluster := gocql.NewCluster(r.Host...)

	var caPath string
	if r.AWS {
		var auth sigv4.AwsAuthenticator = sigv4.NewAwsAuthenticator()
		auth.Region = r.Region
		auth.AccessKeyId = r.AccessKeyId
		auth.SecretAccessKey = r.SecretAccessKey
		cluster.Authenticator = auth
		caPath = "/usr/sf-class2-root.crt"
		cluster.Port = 9142
		cluster.DisableInitialHostLookup = false
	} else {
		cluster.Authenticator = gocql.PasswordAuthenticator{
			Username: r.User,
			Password: r.Password(),
		}
		caPath = r.RootCertPath
	}
	cluster.ProtoVersion = 4
	cluster.Keyspace = r.Keyspace
	cluster.Consistency = r.Consistency
	cluster.Timeout = time.Duration(r.Timeout) * time.Second
	cluster.ConnectTimeout = time.Duration(r.ConnectTimeout) * time.Second
	if r.DCName != "" {
		cluster.PoolConfig.HostSelectionPolicy = gocql.DCAwareRoundRobinPolicy(r.DCName)
	}

	if r.TlsEnabled {
		cluster.SslOpts = &gocql.SslOptions{
			CaPath:                 caPath,
			EnableHostVerification: false,
		}
	}

	return &ClusterImpl{cluster}
}

func (r *ClusterBuilderImpl) WithHost(host ...string) ClusterBuilder {
	r.Host = host
	return r
}

func (r *ClusterBuilderImpl) WithPort(port int) ClusterBuilder {
	r.Port = port
	return r
}

func (r *ClusterBuilderImpl) WithUser(user string) ClusterBuilder {
	r.User = user
	return r
}

func (r *ClusterBuilderImpl) WithPassword(password func() string) ClusterBuilder {
	r.Password = password
	return r
}

func (r *ClusterBuilderImpl) WithConsistency(consistency gocql.Consistency) ClusterBuilder {
	r.Consistency = consistency
	return r
}

func (r *ClusterBuilderImpl) WithKeyspace(keyspace string) ClusterBuilder {
	r.Keyspace = keyspace
	return r
}

func (r *ClusterBuilderImpl) WithConnectTimeout(connectTimeout int) ClusterBuilder {
	r.ConnectTimeout = connectTimeout
	return r
}

func (r *ClusterBuilderImpl) WithTimeout(timeout int) ClusterBuilder {
	r.Timeout = timeout
	return r
}

func (r *ClusterBuilderImpl) WithTLSEnabled(tlsEnabled bool) ClusterBuilder {
	r.TlsEnabled = tlsEnabled
	return r
}

func (r *ClusterBuilderImpl) WithRootCertPath(rootCertPath string) ClusterBuilder {
	r.RootCertPath = rootCertPath
	return r
}
