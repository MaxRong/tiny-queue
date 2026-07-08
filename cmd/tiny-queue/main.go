package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"time"

	"example.com/tiny-queue/internal/queue"
)

func main() {
	q := queue.New(queue.Config{})
	if err := q.Register("echo", func(ctx context.Context, job queue.Job) error {
		return nil
	}); err != nil {
		log.Fatal(err)
	}

	go q.Run(context.Background(), 250*time.Millisecond)

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	log.Printf("tiny-queue listening on http://localhost:%s/status", port)
	log.Fatal(http.ListenAndServe(":"+port, queue.Router(q)))
}
