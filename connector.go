package mongo

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/x-cray/logrus-prefixed-formatter"
	m "go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type (
	Connector struct {
		Addr       string
		Username   string
		Password   string
		AuthDB     string
		Database   string
		Collection string
		handler    *m.Collection
		Logger     *logrus.Logger
	}
)

func (con *Connector) Initiation() error {
	// Initiation logger
	con.Logger = &logrus.Logger{
		Out:   os.Stderr,
		Level: logrus.DebugLevel,
		Formatter: &prefixed.TextFormatter{
			DisableColors:   false,
			TimestampFormat: time.RFC3339,
			FullTimestamp:   true,
			ForceFormatting: true,
		},
	}
	// Initiation Mongo Client
	opts := options.Client().ApplyURI(fmt.Sprintf("mongodb://%s", con.Addr))
	if con.Username != "" {
		opts.SetAuth(
			options.Credential{
				Username:   con.Username,
				Password:   con.Password,
				AuthSource: con.AuthDB,
			})
	}
	client, err := m.NewClient(opts)
	if err != nil {
		return err
	}
	err = client.Connect(context.TODO())
	if err != nil {
		return err
	}
	con.Logger.Infof("Initializing connection to MongoDB [%s] - database (%s) - collection (%s)", con.Addr, con.Database, con.Collection)
	con.handler = client.Database(con.Database).Collection(con.Collection)
	// Success
	return nil
}
