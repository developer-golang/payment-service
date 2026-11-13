package main

import (
	"command/razorpay"
	"command/stripecheckouts"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
)

func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "https://088dptzh-5173.inc1.devtunnels.ms") // your React port
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")

		// Preflight request
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}

		next.ServeHTTP(w, r)
	})
}

func main() {
	http.HandleFunc("/api/create-checkout-session", stripecheckouts.CheckoutSessionHandler)
	http.HandleFunc("/createstripesession", stripecheckouts.CheckoutSessionHandler)
	http.HandleFunc("/success", stripecheckouts.SuccessHandler)
	http.HandleFunc("/api/cancel", stripecheckouts.CancelHandler)
	http.HandleFunc("/webhook", stripecheckouts.WebhookHandler)
	// razor pay
	http.HandleFunc("/createorderinrazorpay", razorpay.CreateOrder)

	// Serve the built React app (frontend/dist) if present, with SPA fallback.
	dist := filepath.Join("frontend", "dist")
	if _, err := os.Stat(dist); err == nil {
		fileServer := http.FileServer(http.Dir(dist))
		http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			path := filepath.Join(dist, filepath.Clean(r.URL.Path))
			if info, err := os.Stat(path); err == nil && !info.IsDir() {
				fileServer.ServeHTTP(w, r)
				return
			}
			http.ServeFile(w, r, filepath.Join(dist, "index.html"))
		})
	}

	fmt.Println("Server starting on :4242")

	log.Fatal(http.ListenAndServe(":4242", corsMiddleware(http.DefaultServeMux)))
}
