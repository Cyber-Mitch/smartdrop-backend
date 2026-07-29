# Leader Election Implementation — TODO

## Completed Steps

### Step 1: Create `src/services/leaderElection.js`
- [x] Implement Redis lease-based distributed lock
- [x] `tryAcquire()` — SET key value NX PX <ttl>
- [x] `renew()` — Lua script for atomic check-and-renew
- [x] `release()` — Lua script for atomic conditional DEL
- [x] `isLeader()` — check if current instance holds lease
- [x] `getCurrentLeader()` — get current lease holder from Redis
- [x] Periodic renewal loop (start/stop)
- [x] Clear logging on state transitions

### Step 2: Update `src/config.js`
- [x] Add `INSTANCE_ID` env var (default: auto-generated from hostname + random suffix)
- [x] Add `LEASE_TTL_MS` env var (default: 15000)
- [x] Add `LEASE_RENEW_INTERVAL_MS` env var (default: 5000)
- [x] Export `leaderElection` config section

### Step 3: Create `src/jobs/leaderAwareJob.js`
- [x] Factory that wraps job modules
- [x] Only activates underlying job when leader
- [x] Reacts to leadership transitions (acquire/renew/release)
- [x] Graceful lease release on stop
- [x] Extended getHealth() with leadership info
- [x] Clear logging: "acting as follower" / "acquired leader lease"

### Step 4: Update `src/index.js`
- [x] Import leader election service and leader-aware job wrapper
- [x] Create leader election instances for price_refresh, webhook_retry, airdrop_expiry
- [x] Wrap all three background jobs with leader-aware wrapper
- [x] Use wrapped jobs in startServer()
- [x] Use wrapped jobs in shutdown() (await stop for graceful lease release)
- [x] Update health endpoint to include leadership state per job
- [x] Add `leader_election` section to health response
- [x] Clean up duplicate/broken code

### Step 5: Update `README.md`
- [ ] Document leader-election mechanism
- [ ] Document failover timing (TTL + renewal gap)
- [ ] New env vars table entries
- [ ] How to verify which replica holds the lock

### Step 6: Create tests in `test/leaderElection.test.js`
- [x] Test file created with comprehensive test suite

