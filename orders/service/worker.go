package service

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"orders/core"
)

func (s *Service) Start(ctx context.Context) {
	go s.orderLoop(ctx)
	go s.eventLoop(ctx)
}

func (s *Service) orderLoop(ctx context.Context) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return

		case <-ticker.C:
			s.startPendingOrders(ctx)
		}
	}
}

func (s *Service) startPendingOrders(ctx context.Context) {
	s.mu.Lock()
	full := len(s.active) >= 16
	s.mu.Unlock()

	if full {
		return
	}

	readCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	orders, err := s.store.PendingOrders(readCtx)
	if err != nil {
		slog.Error("чтение очереди заказов", "err", err)
		return
	}

	for _, order := range orders {
		s.mu.Lock()

		if s.active[order.ID] || len(s.active) >= 16 {
			s.mu.Unlock()
			continue
		}

		s.active[order.ID] = true
		s.mu.Unlock()

		go s.runOrder(ctx, order)
	}
}

func (s *Service) runOrder(parent context.Context, order core.Order) {
	defer func() {
		s.mu.Lock()
		delete(s.active, order.ID)
		s.mu.Unlock()
	}()

	ctx, cancel := context.WithTimeout(parent, 15*time.Second)
	defer cancel()

	if err := s.processOrder(ctx, order); err != nil {
		slog.Warn("заказ отложен", "id", order.ID, "err", err)

		retryCtx, done := context.WithTimeout(
			context.Background(),
			time.Second,
		)
		defer done()

		if err := s.store.RetryLater(retryCtx, order.ID); err != nil {
			slog.Error("планирование повтора", "id", order.ID, "err", err)
		}
	}
}

func (s *Service) processOrder(ctx context.Context, order core.Order) error {
	if order.Phase == core.ReservePhase {
		if err := s.inventory.Reserve(ctx, order); err != nil {
			return err
		}

		if err := s.store.SetPhase(ctx, order.ID, core.PaymentPhase); err != nil {
			return err
		}

		order.Phase = core.PaymentPhase
	}

	if order.Phase == core.PaymentPhase {
		// Прежняя трактовка: нулевая сумма — заказ без списания.
		if order.AmountCents == 0 {
			return s.store.MarkPaidAndQueueEvent(ctx, order.ID)
		}

		declined, err := s.payment.Pay(ctx, order)
		if err != nil {
			return err
		}

		if !declined {
			return s.store.MarkPaidAndQueueEvent(ctx, order.ID)
		}

		if err := s.store.SetPhase(ctx, order.ID, core.ReleasePhase); err != nil {
			return err
		}

		order.Phase = core.ReleasePhase
	}

	if order.Phase == core.ReleasePhase {
		if err := s.inventory.Release(ctx, order.ID); err != nil {
			return err
		}

		return s.store.MarkRejected(ctx, order.ID)
	}

	return fmt.Errorf("unknown order phase: %s", order.Phase)
}

func (s *Service) eventLoop(ctx context.Context) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return

		case <-ticker.C:
			if err := s.publishEvents(ctx); err != nil {
				slog.Warn("отправка событий отложена", "err", err)

				select {
				case <-ctx.Done():
					return
				case <-time.After(time.Second):
				}
			}
		}
	}
}

func (s *Service) publishEvents(parent context.Context) error {
	ctx, cancel := context.WithTimeout(parent, 6*time.Second)
	defer cancel()

	events, err := s.store.PendingEvents(ctx)
	if err != nil {
		return err
	}

	for _, event := range events {
		if err := s.events.Publish(ctx, event); err != nil {
			return err
		}

		if err := s.store.MarkEventSent(ctx, event.ID); err != nil {
			return err
		}
	}

	return nil
}