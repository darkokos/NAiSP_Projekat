package main

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

// koristimo nas Engine da cuvamo podatke za token bucket

type Engine struct{}

func (e *Engine) Put(key string, value []byte) error {
	return nil
}

func (e *Engine) Get(key string) ([]byte, error) {
	return nil, nil
}

func (e *Engine) Delete(key string) error {
	return nil
}

//

type TokenBucket struct {
	tokens chan struct{}
	stop   chan struct{}
	userID string
}

type TokenBucketManager struct {
	db *Engine
	mu sync.Mutex
}

func InitializeTokenBucketManager(db *Engine) *TokenBucketManager {
	return &TokenBucketManager{
		db: db,
	}
}

func (tb *TokenBucket) toJSON() []byte {
	jsonBytes, _ := json.Marshal(tb)
	return jsonBytes
}

func (tbm *TokenBucketManager) NewTokenBucket(userID string, rate int, capacity int) (*TokenBucket, error) {
	tb, err := tbm.GetTokenBucket(rate, userID)
	if tb != nil {
		return tb, err
	}

	tbm.mu.Lock()
	defer tbm.mu.Unlock()

	tb = &TokenBucket{
		tokens: make(chan struct{}, capacity),
		stop:   make(chan struct{}),
		userID: userID,
	}

	for i := 0; i < capacity; i++ {
		tb.tokens <- struct{}{}
	}

	go tb.fill(rate, tbm)

	tbm.db.Put(userID, []byte(tb.toJSON()))

	return tb, nil
}

func (tbm *TokenBucketManager) GetTokenBucket(rate int, userID string) (*TokenBucket, error) {
	tbm.mu.Lock()
	defer tbm.mu.Unlock()

	data, _ := tbm.db.Get(userID)
	if data == nil {
		return nil, fmt.Errorf("Token bucket ne postoji za korisnika: %s", userID)
	}

	tb := &TokenBucket{}
	json.Unmarshal(data, tb)

	go tb.fill(rate, tbm)

	return tb, nil
}

func (tb *TokenBucket) fill(rate int, tbm *TokenBucketManager) {
	ticker := time.NewTicker(time.Duration(1e9 / rate))
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			select {
			case tb.tokens <- struct{}{}:
			default:
			}
			tbm.db.Put(tb.userID, []byte(tb.toJSON()))
		case <-tb.stop:
			return
		}
	}
}

func (tb *TokenBucket) Take(tbm *TokenBucketManager) bool {
	select {
	case <-tb.tokens:
		tbm.mu.Lock()
		defer tbm.mu.Unlock()
		data, _ := json.Marshal(tb)
		tbm.db.Put(tb.userID, data)
		return true
	default:
		return false
	}
}

func (tb *TokenBucket) Close(tbm *TokenBucketManager) {
	tb.stop <- struct{}{}
	tbm.db.Delete(tb.userID)
}
