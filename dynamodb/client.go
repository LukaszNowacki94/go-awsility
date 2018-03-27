package dynamodb

import (
	"fmt"
	"github.com/guregu/dynamo"
)

// Client structure defines a table name and set of methods that operates on defined table.
type Client struct {
	TableName string
	Db        *dynamo.DB
}

// Key struct represents DynamoDB hashKey and rangeKey:
// http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.CoreComponents.html
type Key struct {
	Name  string
	Value interface{}
}

// SetExpression is a structure which holds an update expression and set of arguments.
type SetExpression struct {
	Expression string
	Args       []interface{}
}

// BatchWrite is a function which allows for writing items in Batch. DynamoDB provides an ability to save
// documents in batch. Such approach reduces cost of using DynamoDB service and increases performance.
// Please read http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchWriteItem.html
func (client *Client) BatchWrite(items []interface{}) (wrote int, err error) {
	db := client.Db
	wrote, err = db.Table(client.TableName).Batch().Write().Put(items...).Run()
	return wrote, err
}

// Update is a function which allows for updating items:
// http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_UpdateItem.html
func (client *Client) Update(hashKey *Key, rangeKey *Key, expr *SetExpression) error {
	db := client.Db
	err := db.Table(client.TableName).
		Update(hashKey.Name, hashKey.Value).
		Range(rangeKey.Name, rangeKey.Value).
		SetExpr(expr.Expression, expr.Args...).
		Run()
	return err
}

// ScanAll is a function which allows for retrieving all items:
// http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Scan.html
func (client *Client) ScanAll(hashKey *Key, out interface{}) error {
	db := client.Db
	filterExpr := fmt.Sprintf("%v = ?", hashKey.Name)
	err := db.Table(client.TableName).Scan().Filter(filterExpr, hashKey.Value).All(out)
	return err
}

func (client *Client) GetAll(hashKey *Key, out interface{}) error {
	db := client.Db
	err := db.Table(client.TableName).Get(hashKey.Name, hashKey.Value).All(out)
	return err
}

// UpdateProvision sets read and write provisioning throughput for a given table
func (client *Client) UpdateProvision(read int64, write int64) error {
	db := client.Db
	_, err := db.Table(client.TableName).UpdateTable().Provision(read, write).Run()
	return err
}

// UpdateProvision sets read and write provisioning throughput for a given table
func (client *Client) DescribeTable() (dynamo.Description, error) {
	db := client.Db
	return db.Table(client.TableName).Describe().Run()
}
