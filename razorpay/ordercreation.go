package razorpay

import (
	"encoding/json"
	"fmt"

	"log"
	"net/http"
	"os"

	"github.com/joho/godotenv"
	"github.com/razorpay/razorpay-go"
)

type CreateOrderRequest struct {
	ProductName string `json:"product_name"`
	Amount      int    `json:"amount"`
}

func CreateOrder(w http.ResponseWriter, r *http.Request) {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}
	var req CreateOrderRequest

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}

	client := razorpay.NewClient(
		os.Getenv("RAZORPAY_API_KEY"),
		os.Getenv("RAZORPAY_SECREAT_KEY"),
	)
	fmt.Println("request body :", req)
	data := map[string]interface{}{
		"amount":       req.Amount * 100, // ₹499 → 49900 paise
		"currency":     "INR",
		"product_name": req.ProductName,
		"receipt":      "receipt_123",
	}

	order, err := client.Order.Create(data, nil)
	if err != nil {
		http.Error(w, "failed to create order", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(order)
}
