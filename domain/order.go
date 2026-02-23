package domain

import (
	"encoding/json"
	"fmt"
	"time"
)

type OrderStatus string

const (
	StatusNew        OrderStatus = "new"
	StatusProcessing OrderStatus = "processing"
	StatusCompleted  OrderStatus = "completed"
)

type Order struct {
	ID          string      `json:"id"`
	UserID      string      `json:"user_id"`
	ProductID   string      `json:"product_id"`
	Quantity    int         `json:"quantity"`
	TotalAmount float64     `json:"total_amount"`
	Status      OrderStatus `json:"status"`
	CreatedAt   time.Time   `json:"created_at"`
	UpdatedAt   time.Time   `json:"updated_at"`
}

func (o *Order) MarshalBinary() ([]byte, error) {
	return json.Marshal(o)
}

func (o *Order) UnmarshalBinary(data []byte) error {
	if err := json.Unmarshal(data, o); err != nil {
		return fmt.Errorf("unmarshal order: %w", err)
	}
	return nil
}
