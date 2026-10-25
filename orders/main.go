package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"time"

	"orders/api"
	"orders/clients"
	"orders/postgres"
	"orders/service"
)

func main() {
	store, err := postgres.Open(mustEnv("DATABASE_URL"))
	if err != nil {
		slog.Error("подключение к базе", "err", err)
		os.Exit(1)
	}
	defer store.Close()

	neighbors := clients.New(
		mustEnv("PAYMENT_URL"),
		mustEnv("INVENTORY_URL"),
		mustEnv("EVENTS_URL"),
	)

	orders := service.New(store, neighbors, neighbors, neighbors)
	orders.Start(context.Background())

	handler := api.New(orders)

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	server := &http.Server{
		Addr:              ":" + port,
		Handler:           handler.Routes(),
		ReadHeaderTimeout: 3 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	slog.Info("Orders запущен", "port", port)

	if err := server.ListenAndServe(); err != nil {
		slog.Error("HTTP-сервер", "err", err)
		os.Exit(1)
	}
}

func mustEnv(key string) string {
	value := os.Getenv(key)

	if value == "" {
		slog.Error("не задана переменная окружения", "name", key)
		os.Exit(1)
	}

	return value
}