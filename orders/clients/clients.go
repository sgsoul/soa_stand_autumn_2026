package clients

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"orders/core"
)

type Client struct {
	http         *http.Client
	paymentURL   string
	inventoryURL string
	eventsURL    string

	mu        sync.Mutex
	openUntil time.Time
	slots     chan struct{}
}

func New(paymentURL, inventoryURL, eventsURL string) *Client {
	return &Client{
		http:         &http.Client{Timeout: 4 * time.Second},
		paymentURL:   strings.TrimRight(paymentURL, "/"),
		inventoryURL: strings.TrimRight(inventoryURL, "/"),
		eventsURL:    strings.TrimRight(eventsURL, "/"),
		slots:        make(chan struct{}, 4),
	}
}

func (c *Client) Reserve(ctx context.Context, order core.Order) error {
	items := make([]map[string]any, 0, len(order.Items))

	for _, item := range order.Items {
		items = append(items, map[string]any{
			"sku": item.SKU,
			"qty": item.Qty,
		})
	}

	body := map[string]any{
		"order_id": order.ID,
		"items":    items,
	}

	code, _, err := c.call(
		ctx, http.MethodPost, c.inventoryURL+"/reservations", body, "",
	)
	if err != nil {
		return err
	}

	if code != http.StatusOK && code != http.StatusCreated {
		return fmt.Errorf("inventory reserve status: %d", code)
	}

	return nil
}

func (c *Client) Release(ctx context.Context, orderID string) error {
	address := c.inventoryURL + "/reservations/" + url.PathEscape(orderID)

	code, _, err := c.call(ctx, http.MethodDelete, address, nil, "")
	if err != nil {
		return err
	}

	if code != http.StatusNoContent {
		return fmt.Errorf("inventory release status: %d", code)
	}

	return nil
}

func (c *Client) Pay(ctx context.Context, order core.Order) (bool, error) {
	select {
	case c.slots <- struct{}{}:
		defer func() { <-c.slots }()
	case <-ctx.Done():
		return false, ctx.Err()
	}

	c.mu.Lock()
	open := time.Now().Before(c.openUntil)
	c.mu.Unlock()

	if open {
		return false, errors.New("payment circuit open")
	}

	// Прошлый запрос мог списать деньги, но не вернуть ответ.
	address := c.paymentURL + "/payments?order_id=" + url.QueryEscape(order.ID)

	code, body, err := c.call(ctx, http.MethodGet, address, nil, "")
	if err != nil {
		c.pausePayment()
		return false, err
	}

	if code != http.StatusOK {
		return false, fmt.Errorf("payment lookup status: %d", code)
	}

	var charges []struct {
		OrderID string `json:"order_id"`
		Amount  int64  `json:"amount_cents"`
		Status  string `json:"status"`
	}

	if err := json.Unmarshal(body, &charges); err != nil {
		return false, err
	}

	for _, charge := range charges {
		if charge.OrderID == order.ID &&
			charge.Amount == order.AmountCents &&
			charge.Status == "captured" {
			return false, nil
		}
	}

	request := map[string]any{
		"order_id":     order.ID,
		"amount_cents": order.AmountCents,
		"currency":     "RUB",
	}

	// Ключ и тело не меняются между попытками одного заказа.
	code, _, err = c.call(
		ctx, http.MethodPost, c.paymentURL+"/payments", request, order.ID,
	)
	if err != nil {
		c.pausePayment()
		return false, err
	}

	switch code {
	case http.StatusOK, http.StatusCreated:
		return false, nil
	case http.StatusPaymentRequired:
		return true, nil
	default:
		return false, fmt.Errorf("payment charge status: %d", code)
	}
}

func (c *Client) pausePayment() {
	c.mu.Lock()
	defer c.mu.Unlock()

	until := time.Now().Add(8 * time.Second)
	if until.After(c.openUntil) {
		c.openUntil = until
	}
}

func (c *Client) Publish(ctx context.Context, event core.OrderPaidEvent) error {
	code, _, err := c.call(
		ctx, http.MethodPost, c.eventsURL+"/events", event, "",
	)
	if err != nil {
		return err
	}

	if code != http.StatusAccepted {
		return fmt.Errorf("event publication status: %d", code)
	}

	return nil
}

func (c *Client) call(
	ctx context.Context,
	method, address string,
	body any,
	key string,
) (int, []byte, error) {
	var reader io.Reader

	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return 0, nil, err
		}

		reader = bytes.NewReader(data)
	}

	request, err := http.NewRequestWithContext(ctx, method, address, reader)
	if err != nil {
		return 0, nil, err
	}

	request.Header.Set("Content-Type", "application/json")

	if key != "" {
		request.Header.Set("Idempotency-Key", key)
	}

	response, err := c.http.Do(request)
	if err != nil {
		return 0, nil, err
	}
	defer response.Body.Close()

	data, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	return response.StatusCode, data, err
}