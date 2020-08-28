package mongo

import (
	"context"

	"go.mongodb.org/mongo-driver/bson"
	m "go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func (con *Connector) InsertOne(document interface{}, validation bool) (*m.InsertOneResult, error) {
	bypass := !validation
	opts := options.InsertOneOptions{BypassDocumentValidation: &bypass}
	// Success
	return con.handler.InsertOne(context.TODO(), document, &opts)
}

func (con *Connector) InsertMany(documents []interface{}, validation bool, interrupt bool) (*m.InsertManyResult, error) {
	bypass := !validation
	opts := options.InsertManyOptions{BypassDocumentValidation: &bypass, Ordered: &interrupt}
	// Success
	return con.handler.InsertMany(context.TODO(), documents, &opts)
}

func (con *Connector) UpdateOne(filter *bson.D, update interface{}, upsert bool) (*m.UpdateResult, error) {
	opts := options.UpdateOptions{
		Upsert: &upsert,
	}
	// Success
	return con.handler.UpdateOne(context.TODO(), filter, update, &opts)
}

func (con *Connector) UpdateMany(filter *bson.D, update interface{}, upsert bool) (*m.UpdateResult, error) {
	opts := options.UpdateOptions{
		Upsert: &upsert,
	}
	// Success
	return con.handler.UpdateMany(context.TODO(), filter, update, &opts)
}

func (con *Connector) DeleteOne(filter *bson.D) (*m.DeleteResult, error) {
	// Success
	return con.handler.DeleteOne(context.TODO(), filter)
}

func (con *Connector) DeleteMany(filter *bson.D) (*m.DeleteResult, error) {
	// Success
	return con.handler.DeleteMany(context.TODO(), filter)
}

func (con *Connector) FindOne(filter *bson.D, offset int64, sort *bson.E) (*m.SingleResult, error) {
	opts := options.FindOneOptions{
		Skip: &offset,
	}
	if sort != nil {
		opts.Sort = bson.D{*sort}
	}
	res := con.handler.FindOne(context.TODO(), filter, &opts)
	// Success
	return res, res.Err()
}

func (con *Connector) FindMany(filter *bson.D, offset int64, limit *int64, sort *bson.E) (*m.Cursor, error) {
	opts := options.FindOptions{
		Skip: &offset,
	}
	if limit != nil {
		opts.Limit = limit
	}
	if sort != nil {
		opts.Sort = bson.D{*sort}
	}
	// Success
	return con.handler.Find(context.TODO(), filter, &opts)
}

func (con *Connector) CountDocument(filter *bson.D) (int64, error) {
	// Success
	return con.handler.CountDocuments(context.TODO(), filter)
}

func (con *Connector) Disconnect() error {
	return con.handler.Database().Client().Disconnect(context.TODO())
}
