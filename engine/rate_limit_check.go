package engine

const USER_ID = "user_token_bucket"

func (engine *DB) RateLimitCheck() bool {
	return engine.token_bucket.Take(engine.tbm)
}
