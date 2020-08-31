package mongo

import (
	"context"

	"go.mongodb.org/mongo-driver/bson"
	m "go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func (con *Connector) InsertOne(database, collection string, document interface{}, validation bool) (*m.InsertOneResult, error) {
	bypass := !validation
	opts := options.InsertOneOptions{BypassDocumentValidation: &bypass}
	// Success
	col := con.handler.Database(database).Collection(collection)
	return col.InsertOne(context.TODO(), document, &opts)
}

func (con *Connector) InsertMany(database, collection string, documents []interface{}, validation bool, interrupt bool) (*m.InsertManyResult, error) {
	bypass := !validation
	opts := options.InsertManyOptions{BypassDocumentValidation: &bypass, Ordered: &interrupt}
	// Success
	col := con.handler.Database(database).Collection(collection)
	return col.InsertMany(context.TODO(), documents, &opts)
}

func (con *Connector) UpdateOne(database, collection string, filter *bson.D, update interface{}, upsert bool) (*m.UpdateResult, error) {
	opts := options.UpdateOptions{
		Upsert: &upsert,
	}
	// Success
	col := con.handler.Database(database).Collection(collection)
	return col.UpdateOne(context.TODO(), filter, update, &opts)
}

func (con *Connector) UpdateMany(database, collection string, filter *bson.D, update interface{}, upsert bool) (*m.UpdateResult, error) {
	opts := options.UpdateOptions{
		Upsert: &upsert,
	}
	// Success
	col := con.handler.Database(database).Collection(collection)
	return col.UpdateMany(context.TODO(), filter, update, &opts)
}

func (con *Connector) DeleteOne(database, collection string, filter *bson.D) (*m.DeleteResult, error) {
	// Success
	col := con.handler.Database(database).Collection(collection)
	return col.DeleteOne(context.TODO(), filter)
}

func (con *Connector) DeleteMany(database, collection string, filter *bson.D) (*m.DeleteResult, error) {
	// Success
	col := con.handler.Database(database).Collection(collection)
	return col.DeleteMany(context.TODO(), filter)
}

func (con *Connector) FindOne(database, collection string, filter *bson.D, offset int64, sort *bson.E) (*m.SingleResult, error) {
	opts := options.FindOneOptions{
		Skip: &offset,
	}
	if sort != nil {
		opts.Sort = bson.D{*sort}
	}
	// Success
	col := con.handler.Database(database).Collection(collection)
	res := col.FindOne(context.TODO(), filter, &opts)
	return res, res.Err()
}

func (con *Connector) FindMany(database, collection string, filter *bson.D, offset int64, limit *int64, sort *bson.E) (*m.Cursor, error) {
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
	col := con.handler.Database(database).Collection(collection)
	return col.Find(context.TODO(), filter, &opts)
}

func (con *Connector) CountDocument(database, collection string, filter *bson.D) (int64, error) {
	// Success
	col := con.handler.Database(database).Collection(collection)
	return col.CountDocuments(context.TODO(), filter)
}
