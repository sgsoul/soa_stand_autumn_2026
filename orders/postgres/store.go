package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	"orders/core"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const schema = `
CREATE TABLE IF NOT EXISTS orders (
    id           TEXT PRIMARY KEY,
    user_id      TEXT NOT NULL,
    status       TEXT NOT NULL,
    amount_cents BIGINT NOT NULL,
    items        JSONB NOT NULL,
    phase        TEXT NOT NULL DEFAULT 'reserve',
    next_attempt TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

ALTER TABLE orders ADD COLUMN IF NOT EXISTS
    phase TEXT NOT NULL DEFAULT 'reserve';

ALTER TABLE orders ADD COLUMN IF NOT EXISTS
    next_attempt TIMESTAMPTZ NOT NULL DEFAULT now();

CREATE INDEX IF NOT EXISTS orders_pending_due
    ON orders(next_attempt)
    WHERE status = 'pending';

CREATE TABLE IF NOT EXISTS outbox (
    id          TEXT PRIMARY KEY,
    order_id    TEXT NOT NULL UNIQUE REFERENCES orders(id),
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    sent        BOOLEAN NOT NULL DEFAULT false
);

CREATE TABLE IF NOT EXISTS inbox (
    id           TEXT PRIMARY KEY,
    user_id      TEXT NOT NULL,
    points       BIGINT NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS bonus_balances (
    user_id TEXT PRIMARY KEY,
    points  BIGINT NOT NULL DEFAULT 0 CHECK (points >= 0)
);
`

type Store struct {
	db *pgxpool.Pool
}

func Open(dsn string) (*Store, error) {
	var lastErr error
	deadline := time.Now().Add(100 * time.Second)

	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(
			context.Background(),
			3*time.Second,
		)

		db, err := pgxpool.New(ctx, dsn)
		if err == nil {
			_, err = db.Exec(ctx, schema)
			if err == nil {
				cancel()
				return &Store{db: db}, nil
			}

			db.Close()
		}

		cancel()
		lastErr = err

		slog.Warn("жду базу", "err", err)
		time.Sleep(time.Second)
	}

	return nil, lastErr
}

func (s *Store) Close() {
	s.db.Close()
}

func (s *Store) Ready(ctx context.Context) error {
	return s.db.Ping(ctx)
}

func (s *Store) CreateOrder(ctx context.Context, order core.Order) error {
	items, err := json.Marshal(order.Items)
	if err != nil {
		return err
	}

	_, err = s.db.Exec(ctx, `
		INSERT INTO orders (
			id, user_id, status, amount_cents, items, phase
		)
		VALUES ($1, $2, $3, $4, $5, $6)
	`, order.ID, order.UserID, order.Status,
		order.AmountCents, items, order.Phase)

	return err
}

func (s *Store) GetOrder(ctx context.Context, id string) (core.Order, error) {
	var order core.Order

	err := s.db.QueryRow(ctx, `
		SELECT id, status, amount_cents
		FROM orders
		WHERE id = $1
	`, id).Scan(&order.ID, &order.Status, &order.AmountCents)

	if errors.Is(err, pgx.ErrNoRows) {
		return core.Order{}, core.ErrNotFound
	}

	return order, err
}

func (s *Store) PendingOrders(ctx context.Context) ([]core.Order, error) {
	rows, err := s.db.Query(ctx, `
		SELECT id, user_id, status, amount_cents, items, phase
		FROM orders
		WHERE status = 'pending' AND next_attempt <= now()
		ORDER BY next_attempt, created_at
		LIMIT 64
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var orders []core.Order

	for rows.Next() {
		var order core.Order
		var items []byte

		err := rows.Scan(
			&order.ID,
			&order.UserID,
			&order.Status,
			&order.AmountCents,
			&items,
			&order.Phase,
		)
		if err != nil {
			return nil, err
		}

		if err := json.Unmarshal(items, &order.Items); err != nil {
			return nil, err
		}

		orders = append(orders, order)
	}

	return orders, rows.Err()
}

func (s *Store) SetPhase(ctx context.Context, id, phase string) error {
	_, err := s.db.Exec(ctx, `
		UPDATE orders SET phase = $2
		WHERE id = $1 AND status = 'pending'
	`, id, phase)

	return err
}

func (s *Store) RetryLater(ctx context.Context, id string) error {
	_, err := s.db.Exec(ctx, `
		UPDATE orders
		SET next_attempt = now() + interval '500 milliseconds'
		WHERE id = $1 AND status = 'pending'
	`, id)

	return err
}

func (s *Store) MarkRejected(ctx context.Context, id string) error {
	_, err := s.db.Exec(ctx, `
		UPDATE orders SET status = 'rejected', phase = 'done'
		WHERE id = $1 AND status = 'pending'
	`, id)

	return err
}

func (s *Store) MarkPaidAndQueueEvent(ctx context.Context, id string) error {
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())

	_, err = tx.Exec(ctx, `
		UPDATE orders SET status = 'paid', phase = 'done'
		WHERE id = $1 AND status = 'pending'
	`, id)
	if err != nil {
		return err
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO outbox (id, order_id)
		SELECT $2, id FROM orders
		WHERE id = $1 AND status = 'paid'
		ON CONFLICT (order_id) DO NOTHING
	`, id, "order.paid:"+id)
	if err != nil {
		return err
	}

	return tx.Commit(ctx)
}

func (s *Store) PendingEvents(ctx context.Context) ([]core.OrderPaidEvent, error) {
	rows, err := s.db.Query(ctx, `
		SELECT id, order_id, occurred_at
		FROM outbox
		WHERE NOT sent
		ORDER BY occurred_at
		LIMIT 100
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var events []core.OrderPaidEvent

	for rows.Next() {
		event := core.OrderPaidEvent{Type: "order.paid"}

		err := rows.Scan(&event.ID, &event.OrderID, &event.OccurredAt)
		if err != nil {
			return nil, err
		}

		events = append(events, event)
	}

	return events, rows.Err()
}

func (s *Store) MarkEventSent(ctx context.Context, id string) error {
	_, err := s.db.Exec(ctx, `
		UPDATE outbox SET sent = true WHERE id = $1
	`, id)

	return err
}

func (s *Store) CreditBonusOnce(
	ctx context.Context,
	event core.DeliveryEvent,
) error {
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())

	result, err := tx.Exec(ctx, `
		INSERT INTO inbox (id, user_id, points)
		VALUES ($1, $2, $3)
		ON CONFLICT (id) DO NOTHING
	`, event.ID, event.UserID, event.BonusPoints)
	if err != nil {
		return err
	}

	if result.RowsAffected() == 1 {
		_, err = tx.Exec(ctx, `
			INSERT INTO bonus_balances (user_id, points)
			VALUES ($1, $2)
			ON CONFLICT (user_id) DO UPDATE
			SET points = bonus_balances.points + EXCLUDED.points
		`, event.UserID, event.BonusPoints)
		if err != nil {
			return err
		}
	}

	return tx.Commit(ctx)
}

func (s *Store) GetBonus(ctx context.Context, userID string) (int64, error) {
	var points int64

	err := s.db.QueryRow(ctx, `
		SELECT points FROM bonus_balances WHERE user_id = $1
	`, userID).Scan(&points)

	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil
	}

	return points, err
}