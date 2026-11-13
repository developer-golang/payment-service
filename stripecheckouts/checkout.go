package stripecheckouts

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"

	"github.com/stripe/stripe-go/v86"
	"github.com/stripe/stripe-go/v86/checkout/session"
)

type LineItemRequest struct {
	Name     string `json:"name"`
	Amount   int64  `json:"amount"`
	Currency string `json:"currency"`
	Quantity int64  `json:"quantity"`
	PriceID  string `json:"price_id"`
}

type CheckoutRequest struct {
	LineItems []LineItemRequest `json:"line_items"`
}

func frontendBaseURL() string {
	if base := os.Getenv("FRONTEND_BASE_URL"); base != "" {
		return base
	}
	return "https://088dptzh-5173.inc1.devtunnels.ms"
}

func writeJSON(w http.ResponseWriter, status int, payload map[string]string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(payload)
}

func CheckoutSessionHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Println("Creating checkout session...")
	if key := os.Getenv("STRIPE_SECRET_KEY"); key != "" {
		stripe.Key = key
	}

	var req CheckoutRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid request body: " + err.Error()})
		return
	}
	if len(req.LineItems) == 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "line_items is required"})
		return
	}

	lineItems := make([]*stripe.CheckoutSessionLineItemParams, 0, len(req.LineItems))
	for i, li := range req.LineItems {
		quantity := li.Quantity
		if quantity <= 0 {
			quantity = 1
		}

		item := &stripe.CheckoutSessionLineItemParams{
			Quantity: stripe.Int64(quantity),
		}

		if li.PriceID != "" {
			item.Price = stripe.String(li.PriceID)
		} else {
			if li.Name == "" || li.Amount <= 0 {
				writeJSON(w, http.StatusBadRequest, map[string]string{
					"error": fmt.Sprintf("line_items[%d]: name and a positive amount are required when price_id is not set", i),
				})
				return
			}
			currency := strings.ToLower(li.Currency)
			if currency == "" {
				currency = "usd"
			}
			item.PriceData = &stripe.CheckoutSessionLineItemPriceDataParams{
				Currency:   stripe.String(currency),
				UnitAmount: stripe.Int64(li.Amount),
				ProductData: &stripe.CheckoutSessionLineItemPriceDataProductDataParams{
					Name: stripe.String(li.Name),
				},
			}
		}

		lineItems = append(lineItems, item)
	}

	params := &stripe.CheckoutSessionParams{
		Mode: stripe.String(string(stripe.CheckoutSessionModePayment)),
		SuccessURL: stripe.String(
			frontendBaseURL() + "/success?session_id={CHECKOUT_SESSION_ID}",
		),
		CancelURL: stripe.String(frontendBaseURL() + "/cancel"),
		LineItems: lineItems,
	}

	sess, err := session.New(params)
	if err != nil {
		log.Println(err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{"url": sess.URL})
}

func SuccessHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "Payment successful! Thank you for your purchase.")
}

func CancelHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "Payment canceled. You can try again.")
}

var sc *stripe.Client

func WebhookHandler(w http.ResponseWriter, req *http.Request) {
	fmt.Println("Webhook received...")
	const MaxBodyBytes = int64(65536)
	req.Body = http.MaxBytesReader(w, req.Body, MaxBodyBytes)
	payload, err := ioutil.ReadAll(req.Body)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading request body: %v\n", err)
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}

	event := stripe.Event{}

	if err := json.Unmarshal(payload, &event); err != nil {
		fmt.Fprintf(os.Stderr, "⚠️  Webhook error while parsing basic request. %v\n", err.Error())
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	// To run this example, set an environment variable STRIPE_WEBHOOK_SECRET to
	// your endpoint's unique secret.
	//
	// If you are testing with the CLI, find the secret by running 'stripe listen'.
	// If you are using an endpoint defined with the API or dashboard, look in
	// your webhook settings at https://dashboard.stripe.com/webhooks.
	//
	// Don't include webhook secrets in code.
	endpointSecret := os.Getenv("STRIPE_WEBHOOK_SECRET")

	signatureHeader := req.Header.Get("Stripe-Signature")
	event, err = sc.ConstructEvent(payload, signatureHeader, endpointSecret)
	if err != nil {
		fmt.Fprintf(os.Stderr, "⚠️  Webhook signature verification failed. %v\n", err)
		w.WriteHeader(http.StatusBadRequest) // Return a 400 error on a bad signature
		return
	}
	// Unmarshal the event data into an appropriate struct depending on its Type
	switch event.Type {
	case "checkout.session.completed":
		var paymentIntent stripe.PaymentIntent
		err := json.Unmarshal(event.Data.Raw, &paymentIntent)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error parsing webhook JSON: %v\n", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		log.Printf("Successful payment for %d.", paymentIntent.Amount)
		// Then define and call a func to handle the successful payment intent.
		// handlePaymentIntentSucceeded(paymentIntent)
	case "payment_method.attached":
		var paymentMethod stripe.PaymentMethod
		err := json.Unmarshal(event.Data.Raw, &paymentMethod)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error parsing webhook JSON: %v\n", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		// Then define and call a func to handle the successful attachment of a PaymentMethod.
		// handlePaymentMethodAttached(paymentMethod)
	default:
		fmt.Fprintf(os.Stderr, "Unhandled event type: %s\n", event.Type)
	}

	w.WriteHeader(http.StatusOK)
}

// func WebhookHandler(w http.ResponseWriter, r *http.Request) {
//     fmt.Println("🔥 Webhook received!")

//     body, _ := io.ReadAll(r.Body)
//     fmt.Println(string(body))

//     w.WriteHeader(http.StatusOK)
// }
