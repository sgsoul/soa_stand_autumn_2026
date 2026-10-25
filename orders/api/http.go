package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"time"

	"orders/core"
)

type Orders interface {
	Ready(ctx context.Context) error
	CreateOrder(ctx context.Context, request core.OrderRequest) (core.Order, error)
	GetOrder(ctx context.Context, id string) (core.Order, error)
	ConfirmDelivery(ctx context.Context, event core.DeliveryEvent) error
	GetBonus(ctx context.Context, userID string) (int64, error)
}

type Handler struct {
	orders Orders
}

func New(orders Orders) *Handler {
	return &Handler{orders: orders}
}

func (h *Handler) Routes() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /healthz", h.health)
	mux.HandleFunc("POST /orders", h.createOrder)
	mux.HandleFunc("GET /orders/{id}", h.getOrder)
	mux.HandleFunc("POST /webhooks/delivery", h.delivery)
	mux.HandleFunc("GET /users/{id}/bonus", h.bonus)

	return mux
}

func (h *Handler) health(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 500*time.Millisecond)
	defer cancel()

	if err := h.orders.Ready(ctx); err != nil {
		writeError(w, http.StatusServiceUnavailable, "not_ready")
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *Handler) createOrder(w http.ResponseWriter, r *http.Request) {
	var request core.OrderRequest

	if err := decodeJSON(w, r, &request); err != nil {
		writeError(w, http.StatusBadRequest, "bad_request")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 750*time.Millisecond)
	defer cancel()

	order, err := h.orders.CreateOrder(ctx, request)
	if err != nil {
		handleError(w, err)
		return
	}

	writeJSON(w, http.StatusCreated, map[string]string{
		"id":     order.ID,
		"status": order.Status,
	})
}

func (h *Handler) getOrder(w http.ResponseWriter, r *http.Request) {
	order, err := h.orders.GetOrder(r.Context(), r.PathValue("id"))
	if err != nil {
		handleError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, order)
}

func (h *Handler) delivery(w http.ResponseWriter, r *http.Request) {
	var event core.DeliveryEvent

	if err := decodeJSON(w, r, &event); err != nil {
		writeError(w, http.StatusBadRequest, "bad_event")
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()

	if err := h.orders.ConfirmDelivery(ctx, event); err != nil {
		handleError(w, err)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) bonus(w http.ResponseWriter, r *http.Request) {
	points, err := h.orders.GetBonus(r.Context(), r.PathValue("id"))
	if err != nil {
		handleError(w, err)
		return
	}

	writeJSON(w, http.StatusOK, map[string]int64{"points": points})
}

func handleError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, core.ErrInvalid):
		writeError(w, http.StatusBadRequest, "bad_request")

	case errors.Is(err, core.ErrNotFound):
		writeError(w, http.StatusNotFound, "not_found")

	default:
		slog.Error("обработка HTTP-запроса", "err", err)
		writeError(w, http.StatusServiceUnavailable, "unavailable")
	}
}

func decodeJSON(w http.ResponseWriter, r *http.Request, target any) error {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	decoder := json.NewDecoder(r.Body)

	if err := decoder.Decode(target); err != nil {
		return err
	}

	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return errors.New("expected one JSON value")
	}

	return nil
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)

	if err := json.NewEncoder(w).Encode(body); err != nil {
		slog.Warn("запись HTTP-ответа", "err", err)
	}
}