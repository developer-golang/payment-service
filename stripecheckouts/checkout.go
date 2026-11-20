package stripecheckouts

import (
	"command/kafka"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"

	"github.com/stripe/stripe-go/v86"
	"github.com/stripe/stripe-go/v86/checkout/session"
	"github.com/stripe/stripe-go/v86/webhook"
)

type LineItemRequest struct {
	Name     string `json:"name"`
	Amount   int64  `json:"amount"`
	Currency string `json:"currency"`
	Quantity int64  `json:"quantity"`
	PriceID  string `json:"price_id"`
}

type CheckoutRequest struct {
	OrderID   string            `json:"order_id"`
	LineItems []LineItemRequest `json:"line_items"`
}

var producer *kafka.Producer

func SetProducer(p *kafka.Producer) {
	producer = p
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
	// fmt.Println("Creating checkout session...", os.Getenv("STRIPE_SECRET_KEY"))
	// if key := os.Getenv("STRIPE_SECRET_KEY"); key != "" {
	// 	stripe.Key = key
	// }
	stripe.Key = ""
	var req CheckoutRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		fmt.Println("Error decoding request body:", err)
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
		// Automatically generate an invoice
		InvoiceCreation: &stripe.CheckoutSessionInvoiceCreationParams{
			Enabled: stripe.Bool(true),

			InvoiceData: &stripe.CheckoutSessionInvoiceCreationInvoiceDataParams{
				Description: stripe.String("Product purchase invoice"),
				Footer:      stripe.String("Thank you for your purchase!"),
			},
		},

		CancelURL: stripe.String(frontendBaseURL() + "/cancel"),
		LineItems: lineItems,
	}

	if req.OrderID != "" {
		params.ClientReferenceID = stripe.String(req.OrderID)
		params.Metadata = map[string]string{"order_id": req.OrderID}
	}

	sess, err := session.New(params)
	if err != nil {
		log.Println(err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}else{
		
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
	const MaxBodyBytes = int64(65536)
	req.Body = http.MaxBytesReader(w, req.Body, MaxBodyBytes)
	payload, err := io.ReadAll(req.Body)
	if err != nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}

	endpointSecret := os.Getenv("STRIPE_WEBHOOK_SECRET")
	if endpointSecret == "" {
		endpointSecret = "whsec_PXMGy8EmuYL04OwkgxyN9P8cQ56MKjzp"
	}
	event, err := webhook.ConstructEvent(
		payload,
		req.Header.Get("Stripe-Signature"),
		endpointSecret,
	)
	if err != nil {
		log.Printf("webhook signature verification failed: %v", err)
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	switch event.Type {
	case "checkout.session.completed":
		var cs stripe.CheckoutSession
		if err := json.Unmarshal(event.Data.Raw, &cs); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if cs.PaymentStatus == stripe.CheckoutSessionPaymentStatusPaid {
			log.Printf("Payment done: session=%s amount=%d email=%s",
				cs.ID, cs.AmountTotal, cs.CustomerDetails.Email)

			orderID := cs.ClientReferenceID
			if orderID == "" {
				orderID = cs.Metadata["order_id"]
			}
			if orderID == "" {
				log.Printf("Paid session %s has no order_id; skipping Kafka event", cs.ID)
				break
			}
			if producer == nil {
				log.Printf("Kafka producer not configured; event not published for order %s", orderID)
				break
			}

			paymentEvent := kafka.PaymentCompletedEvent{
				EventID:   fmt.Sprintf("payment-%s", cs.ID),
				EventType: "payment.completed",
				OrderID:   orderID,
				PaymentID: cs.ID,
				Status:    kafka.PaymentStatusSuccess,
				Amount:    float64(cs.AmountTotal) / 100,
				Currency:  string(cs.Currency),
			}
			if err := producer.PublishPaymentCompleted(req.Context(), paymentEvent); err != nil {
				log.Printf("Failed to publish payment event for order %s: %v", orderID, err)
			}
		}

	case "invoice.paid":
		// Fires once the invoice is generated and paid; PDF + hosted URLs are ready here.
		var inv stripe.Invoice
		if err := json.Unmarshal(event.Data.Raw, &inv); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		log.Printf("Invoice %s paid. PDF: %s | Hosted: %s",
			inv.Number, inv.InvoicePDF, inv.HostedInvoiceURL)
		// save inv.ID, inv.Number, inv.InvoicePDF, inv.HostedInvoiceURL in your DB
		// and/or email the PDF link to the customer (inv.CustomerEmail)

	default:
		log.Printf("Unhandled event type: %s", event.Type)
	}

	w.WriteHeader(http.StatusOK)
}

// func WebhookHandler(w http.ResponseWriter, r *http.Request) {
//     fmt.Println("🔥 Webhook received!")

//     body, _ := io.ReadAll(r.Body)
//     fmt.Println(string(body))

//     w.WriteHeader(http.StatusOK)
// }
