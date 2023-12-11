package plaguewatcher

import (
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/log"
	"github.com/hashicorp/golang-lru/v2/expirable"
	_ "github.com/lib/pq"
)

type PlagueWatcher struct {
	db    *sql.DB
	cache *expirable.LRU[string, string]
}

type PreparedTransaction struct {
	tx_hash       string
	tx_fee        string
	gas_fee_cap   string
	gas_tip_cap   string
	tx_first_seen int64
	receiver      string
	signer        string
	nonce         string
}

type TxSummaryTransaction struct {
	tx_hash       string
	tx_first_seen int64
}

func Init() (*PlagueWatcher, error) {
	mock_plague := os.Getenv("MOCK_PLAGUE")
	if mock_plague == "true" {
		return &PlagueWatcher{db: nil, cache: nil}, nil
	}

	db, err := OpenDB()
	if err != nil {
		return nil, err
	}
	cache := expirable.NewLRU[string, string](1000000, nil, time.Hour*24)
	return &PlagueWatcher{db: db, cache: cache}, nil
}

func (pw *PlagueWatcher) HandleTxs(txs []*types.Transaction, peerID string) error {
	mock_plague := os.Getenv("MOCK_PLAGUE")
	if mock_plague == "true" {
		return nil
	}
	preparedTxs, txs_summary := pw.prepareTransactions(txs)
	if len(preparedTxs) == 0 && len(txs_summary) == 0 {
		log.Warn("No new txs")
		return nil
	}

	if len(preparedTxs) == 0 && len(txs_summary) > 0 {
		log.Warn("Storing txs summary", "txs", len(txs_summary))
		pw.StoreTxSummary(txs_summary, peerID)
	}
	return nil
}

func (pw *PlagueWatcher) StoreTxSummary(txs []*TxSummaryTransaction, peerID string) {
	_, err := pw.db.Exec("INSERT INTO peer (peer_id) VALUES ($1) ON CONFLICT (peer_id) DO NOTHING;", peerID)
	if err != nil {
		log.Warn("Failed to insert peer:", "err", err)
		return
	}
	sqlstring := `WITH input_rows(tx_hash, peer_id, tx_first_seen, time) AS (
		VALUES %s
	)
	INSERT INTO tx_summary (tx_hash, peer_id, tx_first_seen, time)
	SELECT input_rows.tx_hash, input_rows.peer_id, input_rows.tx_first_seen, input_rows.time
	FROM input_rows
	ON CONFLICT (tx_hash, peer_id, tx_first_seen) DO NOTHING;`
	valuesSQL := ""
	for _, tx := range txs {
		valuesSQL += fmt.Sprintf("('%s', '%s', %d, %d),", tx.tx_hash, peerID, tx.tx_first_seen, tx.tx_first_seen)
	}
	valuesSQL = strings.TrimSuffix(valuesSQL, ",")
	query := fmt.Sprintf(sqlstring, valuesSQL)
	_, err = pw.db.Exec(query)
	if err != nil {
		log.Warn("Failed to insert txs:", "err", err)
	}
}

func (pw *PlagueWatcher) prepareTransactions(txs []*types.Transaction) ([]*PreparedTransaction, []*TxSummaryTransaction) {
	//empty slice of prepared transactions
	var preparedTxs []*PreparedTransaction
	var tx_summary []*TxSummaryTransaction
	for _, tx := range txs {
		//check if tx is already in cache
		ts := time.Now().UnixMilli()
		tx_summary = append(tx_summary, &TxSummaryTransaction{
			tx_hash:       tx.Hash().Hex(),
			tx_first_seen: ts,
		})
		if _, ok := pw.cache.Get(tx.Hash().Hex()); ok {
			continue
		}
		gasFeeCap := tx.GasFeeCapUint().Clone().String()
		gasTipCap := tx.GasTipCapUint().String()
		fee := strconv.FormatUint(tx.GasPrice().Uint64()*tx.Gas(), 10)
		nonce := strconv.FormatUint(tx.Nonce(), 10)
		signer := types.NewLondonSigner(tx.ChainId())
		addr, err := signer.Sender(tx)
		if err != nil {
			log.Warn("Failed to get the sender:", "err", err)
			addr = common.HexToAddress("0x438308")
		}
		var to string
		if tx.To() == nil {
			to = "0x0"
		} else {
			to = tx.To().Hex()
		}
		preparedTxs = append(preparedTxs, &PreparedTransaction{
			tx_hash:       tx.Hash().Hex(),
			tx_fee:        fee,
			gas_fee_cap:   gasFeeCap,
			gas_tip_cap:   gasTipCap,
			tx_first_seen: ts,
			receiver:      to,
			signer:        addr.Hex(),
			nonce:         nonce,
		})
		pw.cache.Add(tx.Hash().Hex(), tx.Hash().Hex())
	}
	///Summary
	///tx_hash string, peer_id string, tx_first_seen int64

	///Fetched
	///tx_hash string, tx_fee string, gas_fee_cap string, gas_tip_cap string, tx_first_seen int64, from string, to string, nonce string

	return preparedTxs, tx_summary
}

func (pw *PlagueWatcher) HandleBlocksFetched(block *types.Block, peerID string, peerRemoteAddr string, peerLocalAddr string) error {
	mock_plague := os.Getenv("MOCK_PLAGUE")
	if mock_plague == "true" {
		return nil
	}

	_, err := pw.db.Exec("INSERT INTO peer (peer_id) VALUES ($1) ON CONFLICT (peer_id) DO NOTHING;", peerID)
	if err != nil {
		log.Warn("Failed to insert peer:", "err", err)
		return err
	}
	ts := time.Now().UnixMilli()
	insertSQL := `INSERT INTO block_fetched(block_hash, block_number, first_seen_ts, peer, peer_remote_addr, peer_local_addr) VALUES($1,$2,$3,$4,$5,$6)`
	log.Warn("Inserting block", "block", block.NumberU64())
	_, err = pw.db.Exec(insertSQL, block.Hash().Hex(), block.NumberU64(), ts, peerID, peerRemoteAddr, peerLocalAddr)
	return err
}

func OpenDB() (*sql.DB, error) {
	host := os.Getenv("POSTGRES_HOST")
	port := os.Getenv("POSTGRES_PORT")
	user := os.Getenv("POSTGRES_USER")
	password := os.Getenv("POSTGRES_PASSWORD")
	dbname := os.Getenv("POSTGRES_DB")

	psqlInfo := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable", host, port, user, password, dbname)
	log.Warn("Opening DB")

	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		log.Warn("Failed to open DB", "err", err)
		return nil, err
	}

	log.Warn("DB opened")
	return db, nil
}

func prepareAndExecQuery(db *sql.DB, queryString string) error {
	query, err := db.Prepare(queryString)
	if err != nil {
		return err
	}
	_, err = query.Exec()
	return err
}
