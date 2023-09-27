package purityrouter

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/ethereum/go-ethereum/log"
)

type PurityRouter struct {
	trustedPeers []string
	topPeersAddr string
}

func NewPurityRouter() (*PurityRouter, error) {
	topPeersAddrEnv := os.Getenv("TOP_PEERS_ADDR")
	if topPeersAddrEnv == "" {
		log.Warn("No dashboard address specified")
	}

	pr := &PurityRouter{
		topPeersAddr: topPeersAddrEnv,
		trustedPeers: []string{},
	}

	// Initial call
	pr.fetchTopPeers()

	// Set up periodic calls
	go func() {
		ticker := time.NewTicker(10 * time.Minute)
		defer ticker.Stop()

		for range ticker.C {
			pr.fetchTopPeers()
		}
	}()

	return pr, nil
}

func (pr *PurityRouter) fetchTopPeers() error {
	resp, err := http.Get(pr.topPeersAddr + "/top-peers")
	if err != nil {
		log.Warn("Error fetching top peers:", err)
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.Warn("Unexpected status code:", resp.StatusCode)
		return fmt.Errorf("unexpected status code: %v", resp.StatusCode)
	}

	var peers []string
	if err := json.NewDecoder(resp.Body).Decode(&peers); err != nil {
		log.Warn("Error decoding response:", err)
		return err
	}

	pr.trustedPeers = peers
	return nil
}

func (pr *PurityRouter) GetTrustedPeers() []string {
	return pr.trustedPeers
}

func (pr *PurityRouter) GetTrustedPeersSet() map[string]bool {
	peersSet := make(map[string]bool)
	for _, peer := range pr.trustedPeers {
		peersSet[peer] = true
	}
	return peersSet
}