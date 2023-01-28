package tokenbucket_memory

import (
	"fmt"
	"sync"
	"time"
)

type TokenBucket struct {
	tokens chan struct{}
	stop   chan struct{}
	userID string
}

type TokenBucketManager struct {
	buckets map[string]*TokenBucket
	mutex   sync.Mutex
}

func InitializeTokenBucketManager() *TokenBucketManager {
	tbm := &TokenBucketManager{
		buckets: make(map[string]*TokenBucket),
	}
	return tbm
}

func (tbm *TokenBucketManager) NewTokenBucket(userID string, rate int, capacity int) (*TokenBucket, error) {
	if _, exists := tbm.buckets[userID]; exists {
		return tbm.GetTokenBucket(userID), fmt.Errorf("Token bucket vec postoji za korisnika: %s", userID)
	}
	tbm.mutex.Lock()
	defer tbm.mutex.Unlock()
	tb := &TokenBucket{
		tokens: make(chan struct{}, capacity),
		stop:   make(chan struct{}),
		userID: userID,
	}
	for i := 0; i < capacity; i++ {
		tb.tokens <- struct{}{}
	}
	go tb.fill(rate)
	tbm.buckets[userID] = tb
	return tb, nil
}

func (tbm *TokenBucketManager) GetTokenBucket(userID string) *TokenBucket {
	tbm.mutex.Lock()
	defer tbm.mutex.Unlock()
	return tbm.buckets[userID]
}

func (tb *TokenBucket) fill(rate int) {
	ticker := time.NewTicker(time.Duration(1e9 / rate)) // 1e9 nanosekundi = 1 sekunda
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			select {
			case tb.tokens <- struct{}{}:
			default: // bucket je pun
			}
		case <-tb.stop:
			return
		}
	}
}

func (tb *TokenBucket) Take() bool {
	select {
	case <-tb.tokens:
		return true
	default:
		return false
	}
}

func (tb *TokenBucket) GetUserID() string {
	return tb.userID
}

func (tb *TokenBucket) Close() {
	close(tb.stop)
}

func TokenBucketTest() {
	tbm := InitializeTokenBucketManager()
	tb1, err := tbm.NewTokenBucket("user1", 1, 5) // 1 token po sekundi, kapacitet max 5 tokena
	if err != nil {
		fmt.Println(err)
		return
	}
	tb2, err := tbm.NewTokenBucket("user2", 2, 10) // 2 tokena po sekundi, kapacitet max 10 tokena
	if err != nil {
		fmt.Println(err)
		return
	}
	for i := 0; i < 80; i++ {
		time.Sleep(50 * time.Millisecond)
		//tb1 := tbm.GetTokenBucket("user1")
		//tb2 := tbm.GetTokenBucket("user2")
		fmt.Println("Zahtev", i+1)
		if tb1.Take() {
			fmt.Println("Token uzet za korisnika:", tb1.GetUserID())
		} else {
			fmt.Printf("Previse zahteva za korisnika: %s, pokusajte opet kasnije.\n", tb1.GetUserID())
		}
		if tb2.Take() {
			fmt.Println("Token uzet za korisnika:", tb2.GetUserID())
		} else {
			fmt.Printf("Previse zahteva za korisnika: %s, pokusajte opet kasnije.\n", tb2.GetUserID())
		}
	}
	tb1.Close()
	tb2.Close()
}
