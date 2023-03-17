package db_traction

import (
	"context"
	"encoding/json"

	"gorm.io/gorm"
)

type outbox struct {
	Query    string `json:"query"`
	Topic    string `json:"topic"`
	Action   string `json:"action"`
	EntityId int64  `json:"entity_id"`
}

func getOutboxQuery(data interface{}) (string, error) {
	b, err := json.Marshal(data)
	return string(b), err
}

func TransactionWithOutboxMsg[T any](ctx context.Context,
	db *gorm.DB, data *T,
	topic string,
	queryFunc func(tx *gorm.DB) (int64, error)) error {
	msg, err := getOutboxQuery(&data)
	if err != nil {
		return err
	}

	return db.WithContext(ctx).Transaction(func(_tx *gorm.DB) error {
		id, err := queryFunc(_tx)
		outbox := outbox{
			EntityId: id,
			Query:    msg,
			Action:   "Create/Update",
			Topic:    topic}

		if err != nil {
			return err
		}

		ret := _tx.Model(&outbox).Create(&outbox)
		return ret.Error
	})
}

func TransactionDeleteWithOutboxMsg(ctx context.Context,
	db *gorm.DB,
	topic string,
	id int64,
	queryFunc func(tx *gorm.DB) error) error {

	return db.WithContext(ctx).Transaction(func(_tx *gorm.DB) error {
		err := queryFunc(_tx)
		outbox := outbox{
			EntityId: id,
			Query:    "",
			Action:   "Delete",
			Topic:    topic}

		if err != nil {
			return err
		}

		ret := _tx.Model(&outbox).Create(&outbox)
		return ret.Error
	})
}
