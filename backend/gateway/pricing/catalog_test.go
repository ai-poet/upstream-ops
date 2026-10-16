package pricing

import (
	"crypto/sha256"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/bejix/upstream-ops/backend/config"
)

func TestPricingCatalogSyncsRemoteByHash(t *testing.T) {
	body := []byte(`{"remote-model":{"input_cost_per_token":0.000001,"output_cost_per_token":0.000002}}`)
	hash := func() string {
		sum := sha256.Sum256(body)
		return fmt.Sprintf("%x", sum)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/prices.json":
			_, _ = w.Write(body)
		case "/prices.sha256":
			_, _ = w.Write([]byte(hash() + "  prices.json\n"))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	catalog := NewCatalog(nil)
	catalog.Start(config.PricingConfig{
		Enabled:                  true,
		RemoteURL:                server.URL + "/prices.json",
		HashURL:                  server.URL + "/prices.sha256",
		DataDir:                  t.TempDir(),
		HashCheckIntervalMinutes: 60,
	}, nil)
	defer catalog.Stop()

	if got := catalog.Resolve("remote-model"); got.InputPricePerToken != 1e-6 || got.OutputPricePerToken != 2e-6 {
		t.Fatalf("initial remote price = %+v", got)
	}
	body = []byte(`{"remote-model":{"input_cost_per_token":0.000003,"output_cost_per_token":0.000004}}`)
	if err := catalog.sync(); err != nil {
		t.Fatalf("sync: %v", err)
	}
	if got := catalog.Resolve("remote-model"); got.InputPricePerToken != 3e-6 || got.OutputPricePerToken != 4e-6 {
		t.Fatalf("updated remote price = %+v", got)
	}
}

func TestCalculateCostWithServiceTierAndLongContext(t *testing.T) {
	pricing := ModelPricing{
		InputPricePerToken:           1e-6,
		OutputPricePerToken:          2e-6,
		CacheCreationPricePerToken:   1.25e-6,
		CacheCreation1hPricePerToken: 2e-6,
		CacheReadPricePerToken:       .1e-6,
		LongContextInputThreshold:    100,
		LongContextInputMultiplier:   2,
		LongContextOutputMultiplier:  1.5,
	}
	cost := CalculateCostWithServiceTier(pricing, UsageTokens{
		InputTokens:           110,
		OutputTokens:          10,
		CacheCreationTokens:   10,
		CacheCreation5mTokens: 6,
		CacheCreation1hTokens: 4,
		CacheReadTokens:       5,
	}, 1, 1, "")
	want := 110*2e-6 + 10*3e-6 + (6*1.25e-6+4*2e-6)*2 + 5*.1e-6*2
	if diff := cost.TotalCost - want; diff > 1e-12 || diff < -1e-12 {
		t.Fatalf("total = %.12f, want %.12f", cost.TotalCost, want)
	}
}

func TestCalculateCostWithBillingModes(t *testing.T) {
	max := 100
	perRequest := CalculateCostWithBillingInput(ModelPricing{
		BillingMode:     BillingModePerRequest,
		PerRequestPrice: .01,
		RequestTiers:    []Tier{{TierLabel: "HD", PerRequestPrice: .03}, {MinTokens: 0, MaxTokens: &max, PerRequestPrice: .02}},
	}, UsageTokens{InputTokens: 80}, 1, 2, "", BillingInput{RequestCount: 2, SizeTier: "HD"})
	if !closePrice(perRequest.TotalCost, .06) || !closePrice(perRequest.ActualCost, .12) || perRequest.BillingMode != "per_request" {
		t.Fatalf("per request cost = %+v", perRequest)
	}
	image := CalculateCostWithBillingInput(ModelPricing{BillingMode: BillingModeImage, ImagePrice2K: .08}, UsageTokens{}, 1, 1, "", BillingInput{RequestCount: 3, SizeTier: "2048x2048"})
	if !closePrice(image.TotalCost, .24) || image.BillingMode != "image" {
		t.Fatalf("image cost = %+v", image)
	}
	video := CalculateCostWithBillingInput(ModelPricing{BillingMode: BillingModeVideo, VideoPrice720P: .07}, UsageTokens{}, 1, 1, "", BillingInput{RequestCount: 2, SizeTier: "720p", VideoDurationSeconds: 10})
	if !closePrice(video.TotalCost, 1.4) || video.BillingMode != "video" {
		t.Fatalf("video cost = %+v", video)
	}
}

func closePrice(got, want float64) bool {
	diff := got - want
	return diff <= 1e-12 && diff >= -1e-12
}

func TestDefaultImageGenerationPricingMode(t *testing.T) {
	price := NewCatalog(nil).Resolve("gemini-2.0-flash-exp-image-generation")
	if price.BillingMode != BillingModeImage || price.OutputPricePerImage != 0.034 {
		t.Fatalf("pricing=%+v", price)
	}
}
