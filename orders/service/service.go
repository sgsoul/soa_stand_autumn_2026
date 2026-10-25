package service

import (
	"context"
	"crypto/rand"
	"fmt"
	"math"
	"sync"

	"orders/core"
)

type Store interface {
	Ready(ctx context.Context) error

	CreateOrder(ctx context.Context, order core.Order) error
	GetOrder(ctx context.Context, id string) (core.Order, error)
	PendingOrders(ctx context.Context) ([]core.Order, error)

	SetPhase(ctx context.Context, id, phase string) error
	RetryLater(ctx context.Context, id string) error
	MarkPaidAndQueueEvent(ctx context.Context, id string) error
	MarkRejected(ctx context.Context, id string) error

	PendingEvents(ctx context.Context) ([]core.OrderPaidEvent, error)
	MarkEventSent(ctx context.Context, id string) error

	CreditBonusOnce(ctx context.Context, event core.DeliveryEvent) error
	GetBonus(ctx context.Context, userID string) (int64, error)
}

type Payment interface {
	Pay(ctx context.Context, order core.Order) (declined bool, err error)
}

type Inventory interface {
	Reserve(ctx context.Context, order core.Order) error
	Release(ctx context.Context, orderID string) error
}

type Events interface {
	Publish(ctx context.Context, event core.OrderPaidEvent) error
}

type Service struct {
	store     Store
	payment   Payment
	inventory Inventory
	events    Events

	mu     sync.Mutex
	active map[string]bool
}

func New(
	store Store,
	payment Payment,
	inventory Inventory,
	events Events,
) *Service {
	return &Service{
		store:     store,
		payment:   payment,
		inventory: inventory,
		events:    events,
		active:    make(map[string]bool),
	}
}

func (s *Service) Ready(ctx context.Context) error {
	return s.store.Ready(ctx)
}

func (s *Service) CreateOrder(
	ctx context.Context,
	request core.OrderRequest,
) (core.Order, error) {
	amount, err := calculateAmount(request)
	if err != nil {
		return core.Order{}, err
	}

	order := core.Order{
		ID:          newID(),
		UserID:      request.UserID,
		Items:       request.Items,
		AmountCents: amount,
		Status:      core.Pending,
		Phase:       core.ReservePhase,
	}

	if err := s.store.CreateOrder(ctx, order); err != nil {
		return core.Order{}, err
	}

	return order, nil
}

func (s *Service) GetOrder(ctx context.Context, id string) (core.Order, error) {
	return s.store.GetOrder(ctx, id)
}

func (s *Service) ConfirmDelivery(
	ctx context.Context,
	event core.DeliveryEvent,
) error {
	if event.ID == "" || event.UserID == "" ||
		event.Type != "delivery.confirmed" || event.BonusPoints < 1 {
		return core.ErrInvalid
	}

	return s.store.CreditBonusOnce(ctx, event)
}

func (s *Service) GetBonus(ctx context.Context, userID string) (int64, error) {
	return s.store.GetBonus(ctx, userID)
}

func calculateAmount(request core.OrderRequest) (int64, error) {
	if request.UserID == "" || len(request.Items) == 0 {
		return 0, core.ErrInvalid
	}

	var amount int64

	for _, item := range request.Items {
		if item.SKU == "" || item.Qty < 1 || item.PriceCents < 0 {
			return 0, core.ErrInvalid
		}

		if item.PriceCents > 0 {
			maxQuantity := (math.MaxInt64 - amount) / item.PriceCents
			if item.Qty > maxQuantity {
				return 0, core.ErrInvalid
			}
		}

		amount += item.Qty * item.PriceCents
	}

	return amount, nil
}

func newID() string {
	var b [16]byte

	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}

	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80

	return fmt.Sprintf(
		"%x-%x-%x-%x-%x",
		b[:4], b[4:6], b[6:8], b[8:10], b[10:],
	)
}