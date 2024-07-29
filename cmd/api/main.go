package main

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/redis/go-redis/v9"
	"log"
	"net/http"
	"strings"
	"time"
)

type response struct {
	HasEmailHash bool   `json:"hasEmailHash"`
	Csf          string `json:"csf,omitempty"`
	Cdf          string `json:"cdf,omitempty"`
}
type server struct {
	redisClient *redis.Client
}

const redisHost = "localhost:6379"

func main() {

	redisClient := redis.NewClient(&redis.Options{
		Addr: redisHost,
	})

	_, err := redisClient.Ping(context.Background()).Result()
	if err != nil {
		log.Fatalf("Error connecting to redis: %v", err)
	}

	mux := http.NewServeMux()
	s := &server{
		redisClient: redisClient,
	}

	mux.HandleFunc("/search", s.search)

	fmt.Printf("Starting server on :8099\n")

	log.Fatal(http.ListenAndServe(":8099", latency(mux)))

}

func (s *server) search(w http.ResponseWriter, r *http.Request) {

	ctx := context.Background()

	query := r.URL.Query().Get("q")

	if query == "" {
		s.writeJson(w, map[string]interface{}{
			"error": "missing query parameter",
		}, http.StatusBadRequest)
		return
	}

	var resp response

	// search redis
	val, err := s.redisClient.Get(ctx, query).Result()
	if err != nil {
		s.writeJson(w, resp, http.StatusBadRequest)
		return
	}

	parts := strings.Split(val, ",")
	if len(parts) != 3 {
		s.writeJson(w, resp, http.StatusBadRequest)
		return
	}

	resp.HasEmailHash = true
	resp.Csf = parts[0]
	resp.Cdf = parts[1]

	s.writeJson(w, resp, http.StatusOK)

}

func (s *server) writeJson(w http.ResponseWriter, data interface{}, status int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)

	payload, err := json.Marshal(data)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	fmt.Fprintf(w, "%s", string(payload))
}

func latency(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		fmt.Printf("Request took: %v\n", time.Since(start))
	})
}
