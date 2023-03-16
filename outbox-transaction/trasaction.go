package db_traction

import (
	"context"
	"encoding/json"

	"gorm.io/gorm"
)

type outbox struct {
	Query    string `json:"query"`
	Topic    string `json:"topic"`
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

	err = db.WithContext(ctx).Transaction(func(_tx *gorm.DB) error {
		id, err := queryFunc(_tx)
		outbox := outbox{
			EntityId: id,
			Query:    msg,
			Topic:    topic}

		if err != nil {
			return err
		}

		ret := _tx.Model(&outbox).Create(&outbox)
		if ret.Error != nil {
			return ret.Error
		}
		return nil
	})

	if err != nil {
		return err
	}

	return nil
}
