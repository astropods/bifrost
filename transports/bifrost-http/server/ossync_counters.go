package server

// Counter sync: budget and rate-limit usage shared across gateway replicas.
//
// Each replica counts the requests it serves in its own memory, and upstream's
// usage dump writes that local count over the shared row, so N replicas can
// spend up to N times a budget. This keeps every replica's in-memory counter at
// the cluster total, within about one tick (1s by default), without changing
// the governance plugin:
//
//   - Every replica writes its own usage per limit and per window to
//     astropods_ossync_usage, keyed by (kind, limit, window, epoch, node). A
//     window is the limit's last_reset; an epoch moves on an operator reset.
//   - Every tick it reads the other replicas' rows for the same window and
//     epoch, and adds the difference to its in-memory counter with the store's
//     atomic delta methods. Enforcement keeps reading memory, unchanged.
//   - "Own usage" is the in-memory counter minus what was folded in from the
//     others, so requests that land between ticks are never lost.
//   - The first replica to see a window with no rows seeds it with its current
//     count, so switching this on does not drop the usage already recorded.
//   - Upstream's dump keeps writing governance_budgets; with every replica near
//     the total, that row stays near the total too, and it is still what a
//     replica loads at startup before its first tick.
//
// Enabled with BIFROST_OSS_SYNC_COUNTERS=1, on top of BIFROST_OSS_SYNC=1.

import (
	"context"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/maximhq/bifrost/framework/configstore/tables"
	"github.com/maximhq/bifrost/plugins/governance"
	"gorm.io/gorm"
)

const (
	ctrKindBudget   = "budget"
	ctrKindTokens   = "rl_tokens"
	ctrKindRequests = "rl_requests"
	// The row that carries a window's usage from before counter sync saw it.
	ctrSeedNode = "_seed"
	// Rows written per statement.
	ctrBatch = 500
)

// ossyncUsageRow is one replica's usage of one limit in one window.
type ossyncUsageRow struct {
	Kind        string    `gorm:"primaryKey;size:16"`
	LimitID     string    `gorm:"primaryKey;size:255"`
	WindowStart int64     `gorm:"primaryKey"` // the limit's last_reset, unix microseconds
	Epoch       int64     `gorm:"primaryKey"` // moves on an operator usage reset
	NodeID      string    `gorm:"primaryKey;size:255"`
	Usage       float64   `gorm:"not null"`
	UpdatedAt   time.Time `gorm:"index;not null"`
}

func (ossyncUsageRow) TableName() string { return "astropods_ossync_usage" }

// ossyncEpochRow holds the current epoch of a budget whose usage an operator reset.
type ossyncEpochRow struct {
	Kind      string    `gorm:"primaryKey;size:16"`
	LimitID   string    `gorm:"primaryKey;size:255"`
	Epoch     int64     `gorm:"not null"`
	UpdatedAt time.Time `gorm:"index;not null"`
}

func (ossyncEpochRow) TableName() string { return "astropods_ossync_epoch" }

// ossyncBumper is the part of the governance store that adds a delta to a
// counter atomically, without a window reset. LocalGovernanceStore has it.
type ossyncBumper interface {
	BumpBudgetUsageBy(ctx context.Context, budgetID string, delta float64) error
	BumpRateLimitUsageBy(ctx context.Context, rateLimitID string, tokenDelta, requestDelta int64) error
}

type ctrKey struct {
	kind, id      string
	window, epoch int64
}

type ctrState struct {
	window, epoch int64
	folded        float64 // the other replicas' usage already added to memory
	written       float64 // own usage last written
	wrote         bool
}

type counterSync struct {
	o     *OSSync
	db    *gorm.DB
	every time.Duration

	// Touched by message handlers and the tick.
	mu           sync.Mutex
	epochs       map[string]int64 // "kind|id" -> current epoch
	pendingReset map[string]bool  // budget IDs whose epoch moved without a local reset
	reinit       map[string]bool  // "kind|id" -> treat next tick as a fresh start

	// Owned by the run goroutine.
	state     map[string]*ctrState
	rows      map[ctrKey]map[string]float64 // window -> node -> usage, every node
	watermark time.Time                     // newest updated_at read, database clock
	epochMark time.Time
	lastFull  time.Time
	lastGC    time.Time

	stats struct {
		Folds, WriteErrors, ReadErrors, Seeds atomic.Int64
		LastTickUnix                          atomic.Int64
		Limits                                atomic.Int64
	}
}

func newCounterSyncFromEnv(o *OSSync) *counterSync {
	if on, _ := strconv.ParseBool(os.Getenv("BIFROST_OSS_SYNC_COUNTERS")); !on {
		return nil
	}
	return &counterSync{
		o:            o,
		db:           o.db,
		every:        envDuration("BIFROST_OSS_SYNC_COUNTER_INTERVAL", time.Second),
		epochs:       map[string]int64{},
		pendingReset: map[string]bool{},
		reinit:       map[string]bool{},
		state:        map[string]*ctrState{},
		rows:         map[ctrKey]map[string]float64{},
	}
}

func ctrName(kind, id string) string { return kind + "|" + id }

func ctrWindow(t time.Time) int64 { return t.UTC().Truncate(time.Microsecond).UnixMicro() }

// ---------------------------------------------------------------------------
// Events from the governance sync

// onReset records an operator usage reset of budgets at epoch. alreadyReset is
// true when this node's memory was just reset (locally or from a peer's
// message). persist writes the epoch for replicas that missed the message.
func (c *counterSync) onReset(ctx context.Context, budgetIDs []string, epoch int64, alreadyReset, persist bool) {
	if c == nil || epoch == 0 {
		return
	}
	c.mu.Lock()
	for _, id := range budgetIDs {
		k := ctrName(ctrKindBudget, id)
		if epoch > c.epochs[k] {
			c.epochs[k] = epoch
			if !alreadyReset {
				c.pendingReset[id] = true
			}
		}
	}
	c.mu.Unlock()
	if !persist || len(budgetIDs) == 0 {
		return
	}
	for start := 0; start < len(budgetIDs); start += ctrBatch {
		chunk := budgetIDs[start:min(start+ctrBatch, len(budgetIDs))]
		var sb strings.Builder
		args := make([]any, 0, len(chunk)*3)
		sb.WriteString("INSERT INTO astropods_ossync_epoch (kind, limit_id, epoch, updated_at) VALUES ")
		for i, id := range chunk {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString("(?, ?, ?, now())")
			args = append(args, ctrKindBudget, id, epoch)
		}
		sb.WriteString(" ON CONFLICT (kind, limit_id) DO UPDATE SET epoch = GREATEST(astropods_ossync_epoch.epoch, EXCLUDED.epoch), updated_at = now()")
		if err := c.db.WithContext(ctx).Exec(sb.String(), args...).Error; err != nil {
			c.stats.WriteErrors.Add(1)
			logger.Warn("ossync: counters: record reset epoch: %v", err)
		}
	}
}

// onAdopt marks limits whose window moved by calendar adoption, which keeps
// their usage: the next tick starts them afresh in the new window.
func (c *counterSync) onAdopt(budgetIDs, rateLimitIDs []string) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, id := range budgetIDs {
		c.reinit[ctrName(ctrKindBudget, id)] = true
	}
	for _, id := range rateLimitIDs {
		c.reinit[ctrName(ctrKindTokens, id)] = true
		c.reinit[ctrName(ctrKindRequests, id)] = true
	}
}

// ---------------------------------------------------------------------------
// Loop

func (c *counterSync) run(ctx context.Context) {
	if err := c.db.WithContext(ctx).AutoMigrate(&ossyncUsageRow{}, &ossyncEpochRow{}); err != nil {
		logger.Error("ossync: counters: create tables: %v; counter sync disabled", err)
		return
	}
	// The epochs as they stand: memory was loaded after any reset they record,
	// so none of them needs a local reset now.
	if err := c.refreshEpochs(ctx, true, false); err != nil {
		c.stats.ReadErrors.Add(1)
		logger.Warn("ossync: counters: load epochs: %v", err)
	}
	logger.Info("ossync: counter sync enabled, every %s", c.every)
	t := time.NewTicker(c.every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			tctx, cancel := context.WithTimeout(ctx, max(c.every*5, 10*time.Second))
			c.tick(tctx)
			cancel()
		}
	}
}

type ctrEntry struct {
	kind, id string
	usage    float64
	window   int64
}

func (c *counterSync) tick(ctx context.Context) {
	store, ok := c.o.governanceStore()
	if !ok {
		return
	}
	bump, ok := store.(ossyncBumper)
	if !ok {
		logger.Error("ossync: counters: the governance store cannot add deltas; counter sync stopped")
		return
	}

	// 1. Learn what the other replicas wrote since the last tick.
	full := c.lastFull.IsZero() || time.Since(c.lastFull) > time.Minute
	if err := c.refreshRows(ctx, full); err != nil {
		c.stats.ReadErrors.Add(1)
		logger.Warn("ossync: counters: read usage: %v", err)
		return // folding against a stale view could double count
	}
	if err := c.refreshEpochs(ctx, false, true); err != nil {
		c.stats.ReadErrors.Add(1)
		logger.Warn("ossync: counters: read epochs: %v", err)
	}

	// 2. Resets this node learned of only from the table.
	c.mu.Lock()
	resets := make([]string, 0, len(c.pendingReset))
	for id := range c.pendingReset {
		resets = append(resets, id)
	}
	c.pendingReset = map[string]bool{}
	c.mu.Unlock()
	for _, id := range resets {
		store.ResetBudgetUsageInMemory(ctx, id)
	}

	// 3. Fold and write.
	data := store.GetGovernanceUsageData(ctx)
	if data == nil {
		return
	}
	entries := make([]ctrEntry, 0, len(data.Budgets)+2*len(data.RateLimits))
	for id, b := range data.Budgets {
		if b != nil {
			entries = append(entries, ctrEntry{ctrKindBudget, id, b.CurrentUsage, ctrWindow(b.LastReset)})
		}
	}
	for id, rl := range data.RateLimits {
		if rl == nil {
			continue
		}
		if rl.TokenMaxLimit != nil {
			entries = append(entries, ctrEntry{ctrKindTokens, id, float64(rl.TokenCurrentUsage), ctrWindow(rl.TokenLastReset)})
		}
		if rl.RequestMaxLimit != nil {
			entries = append(entries, ctrEntry{ctrKindRequests, id, float64(rl.RequestCurrentUsage), ctrWindow(rl.RequestLastReset)})
		}
	}

	me := c.o.nodeID
	var writes, seeds []ossyncUsageRow
	seen := make(map[string]struct{}, len(entries))
	for _, e := range entries {
		k := ctrName(e.kind, e.id)
		seen[k] = struct{}{}
		c.mu.Lock()
		epoch := c.epochs[k]
		fresh := c.reinit[k]
		delete(c.reinit, k)
		c.mu.Unlock()
		key := ctrKey{e.kind, e.id, e.window, epoch}

		st := c.state[k]
		switch {
		case st == nil || fresh:
			// A fresh start: memory holds the total as this node knows it.
			// Its own share is whatever it wrote to this window before.
			prev := c.rows[key][me]
			st = &ctrState{window: e.window, epoch: epoch, folded: e.usage - prev, written: prev, wrote: c.rows[key] != nil && hasNode(c.rows[key], me)}
			c.state[k] = st
			if len(c.rows[key]) == 0 {
				seed := ossyncUsageRow{Kind: e.kind, LimitID: e.id, WindowStart: e.window, Epoch: epoch, NodeID: ctrSeedNode, Usage: e.usage}
				seeds = append(seeds, seed)
				c.addRow(key, ctrSeedNode, e.usage)
				st.folded = e.usage
				st.written, st.wrote = 0, false
			}
		case st.epoch == epoch && st.window != e.window && len(c.rows[key]) > 0 &&
			st.folded+st.written > 0 && e.usage >= st.folded+st.written:
			// The window moved but the usage did not drop: it was carried over
			// (calendar adoption, with or without its message, or a reload that
			// changed last_reset), not reset. Memory still holds the other
			// replicas' share, so join the window they already count in instead
			// of claiming it all as this node's own. A real reset zeroes memory
			// and takes the branch below.
			prev := c.rows[key][me]
			st.window = e.window
			st.folded, st.written, st.wrote = e.usage-prev, prev, hasNode(c.rows[key], me)
		case st.window != e.window || st.epoch != epoch:
			// First into a new window, or an operator reset moved the epoch:
			// everything in memory now is this node's own.
			st.window, st.epoch = e.window, epoch
			st.folded, st.written, st.wrote = 0, 0, false
		}

		own := max(e.usage-st.folded, 0)
		if !st.wrote || own != st.written {
			writes = append(writes, ossyncUsageRow{Kind: e.kind, LimitID: e.id, WindowStart: e.window, Epoch: epoch, NodeID: me, Usage: own})
			st.written, st.wrote = own, true
			c.addRow(key, me, own)
		}

		others := 0.0
		for node, u := range c.rows[key] {
			if node != me {
				others += u
			}
		}
		if delta := others - st.folded; delta != 0 && c.windowUnchanged(ctx, store, e) {
			if applied, err := c.fold(ctx, bump, e, delta); err == nil {
				st.folded += applied
				if applied != 0 {
					c.stats.Folds.Add(1)
				}
			}
		}
	}
	for k := range c.state {
		if _, ok := seen[k]; !ok {
			delete(c.state, k) // the limit was deleted
		}
	}
	c.stats.Limits.Store(int64(len(c.state)))

	if err := c.upsert(ctx, seeds, true); err != nil {
		c.stats.WriteErrors.Add(1)
		logger.Warn("ossync: counters: seed usage: %v", err)
	} else {
		c.stats.Seeds.Add(int64(len(seeds)))
	}
	if err := c.upsert(ctx, writes, false); err != nil {
		c.stats.WriteErrors.Add(1)
		logger.Warn("ossync: counters: write usage: %v", err)
		for _, w := range writes {
			if st := c.state[ctrName(w.Kind, w.LimitID)]; st != nil {
				st.wrote = false // retry next tick
			}
		}
	}

	if time.Since(c.lastGC) > 10*time.Minute {
		c.gc(ctx)
		c.lastGC = time.Now()
	}
	c.stats.LastTickUnix.Store(time.Now().Unix())
}

func hasNode(m map[string]float64, node string) bool { _, ok := m[node]; return ok }

func (c *counterSync) addRow(key ctrKey, node string, usage float64) {
	m := c.rows[key]
	if m == nil {
		m = map[string]float64{}
		c.rows[key] = m
	}
	m[node] = usage
}

// windowUnchanged re-reads the limit just before a fold: a window that closed
// since the snapshot must not receive the old window's usage.
func (c *counterSync) windowUnchanged(ctx context.Context, store governance.GovernanceStore, e ctrEntry) bool {
	switch e.kind {
	case ctrKindBudget:
		b := store.LoadBudget(ctx, e.id)
		return b != nil && ctrWindow(b.LastReset) == e.window
	case ctrKindTokens:
		rl := store.LoadRateLimit(ctx, e.id)
		return rl != nil && ctrWindow(rl.TokenLastReset) == e.window
	case ctrKindRequests:
		rl := store.LoadRateLimit(ctx, e.id)
		return rl != nil && ctrWindow(rl.RequestLastReset) == e.window
	}
	return false
}

// fold adds delta to the in-memory counter and returns what it added. Rate
// limits count whole tokens and requests, so their delta is rounded.
func (c *counterSync) fold(ctx context.Context, bump ossyncBumper, e ctrEntry, delta float64) (float64, error) {
	switch e.kind {
	case ctrKindBudget:
		return delta, bump.BumpBudgetUsageBy(ctx, e.id, delta)
	case ctrKindTokens:
		d := int64(math.Round(delta))
		return float64(d), bump.BumpRateLimitUsageBy(ctx, e.id, d, 0)
	case ctrKindRequests:
		d := int64(math.Round(delta))
		return float64(d), bump.BumpRateLimitUsageBy(ctx, e.id, 0, d)
	}
	return 0, fmt.Errorf("unknown kind %q", e.kind)
}

// ---------------------------------------------------------------------------
// Database

func (c *counterSync) upsert(ctx context.Context, rows []ossyncUsageRow, seedOnly bool) error {
	for start := 0; start < len(rows); start += ctrBatch {
		chunk := rows[start:min(start+ctrBatch, len(rows))]
		var sb strings.Builder
		args := make([]any, 0, len(chunk)*6)
		if seedOnly {
			// A seed goes in only while the window has no rows at all: a replica
			// that already counts there carries the usage. The primary key keeps
			// two replicas seeding at once down to one seed.
			sb.WriteString("INSERT INTO astropods_ossync_usage (kind, limit_id, window_start, epoch, node_id, usage, updated_at) SELECT v.kind, v.limit_id, v.ws, v.ep, v.node, v.usage, now() FROM (VALUES ")
			for i, r := range chunk {
				if i > 0 {
					sb.WriteString(", ")
				}
				sb.WriteString("(?::text, ?::text, ?::bigint, ?::bigint, ?::text, ?::double precision)")
				args = append(args, r.Kind, r.LimitID, r.WindowStart, r.Epoch, r.NodeID, r.Usage)
			}
			sb.WriteString(") AS v(kind, limit_id, ws, ep, node, usage) WHERE NOT EXISTS (SELECT 1 FROM astropods_ossync_usage u WHERE u.kind = v.kind AND u.limit_id = v.limit_id AND u.window_start = v.ws AND u.epoch = v.ep)")
			sb.WriteString(" ON CONFLICT (kind, limit_id, window_start, epoch, node_id) DO NOTHING")
		} else {
			sb.WriteString("INSERT INTO astropods_ossync_usage (kind, limit_id, window_start, epoch, node_id, usage, updated_at) VALUES ")
			for i, r := range chunk {
				if i > 0 {
					sb.WriteString(", ")
				}
				sb.WriteString("(?, ?, ?, ?, ?, ?, now())")
				args = append(args, r.Kind, r.LimitID, r.WindowStart, r.Epoch, r.NodeID, r.Usage)
			}
			sb.WriteString(" ON CONFLICT (kind, limit_id, window_start, epoch, node_id) DO UPDATE SET usage = EXCLUDED.usage, updated_at = now()")
		}
		if err := c.db.WithContext(ctx).Exec(sb.String(), args...).Error; err != nil {
			return err
		}
	}
	return nil
}

// refreshRows reads every row (full) or the rows changed since the last read,
// with 10s of overlap: now() is a transaction's start, so a row can commit
// with an updated_at older than one already read.
func (c *counterSync) refreshRows(ctx context.Context, full bool) error {
	q := c.db.WithContext(ctx).Model(&ossyncUsageRow{})
	if !full {
		q = q.Where("updated_at > ?", c.watermark.Add(-10*time.Second))
	}
	var rows []ossyncUsageRow
	if err := q.Find(&rows).Error; err != nil {
		return err
	}
	if full {
		c.rows = make(map[ctrKey]map[string]float64, len(rows))
		c.lastFull = time.Now()
	}
	for _, r := range rows {
		c.addRow(ctrKey{r.Kind, r.LimitID, r.WindowStart, r.Epoch}, r.NodeID, r.Usage)
		if r.UpdatedAt.After(c.watermark) {
			c.watermark = r.UpdatedAt
		}
	}
	return nil
}

// refreshEpochs reads reset epochs. markReset queues a local memory reset for
// a budget whose epoch moved without this node applying the reset.
func (c *counterSync) refreshEpochs(ctx context.Context, full, markReset bool) error {
	q := c.db.WithContext(ctx).Model(&ossyncEpochRow{})
	if !full {
		q = q.Where("updated_at > ?", c.epochMark.Add(-10*time.Second))
	}
	var rows []ossyncEpochRow
	if err := q.Find(&rows).Error; err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, r := range rows {
		k := ctrName(r.Kind, r.LimitID)
		if r.Epoch > c.epochs[k] {
			c.epochs[k] = r.Epoch
			if markReset && r.Kind == ctrKindBudget {
				c.pendingReset[r.LimitID] = true
				logger.Info("ossync: counters: budget %s was reset on another replica; resetting here", r.LimitID)
			}
		}
		if r.UpdatedAt.After(c.epochMark) {
			c.epochMark = r.UpdatedAt
		}
	}
	return nil
}

// gc deletes rows of windows and epochs older than the ones each limit is in.
func (c *counterSync) gc(ctx context.Context) {
	type cur struct {
		kind, id      string
		window, epoch int64
	}
	all := make([]cur, 0, len(c.state))
	for k, st := range c.state {
		kind, id, _ := strings.Cut(k, "|")
		all = append(all, cur{kind, id, st.window, st.epoch})
	}
	removed := int64(0)
	for start := 0; start < len(all); start += ctrBatch {
		chunk := all[start:min(start+ctrBatch, len(all))]
		var sb strings.Builder
		args := make([]any, 0, len(chunk)*4)
		sb.WriteString("DELETE FROM astropods_ossync_usage u USING (VALUES ")
		for i, x := range chunk {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString("(?::text, ?::text, ?::bigint, ?::bigint)")
			args = append(args, x.kind, x.id, x.window, x.epoch)
		}
		sb.WriteString(") AS v(kind, limit_id, ws, ep) WHERE u.kind = v.kind AND u.limit_id = v.limit_id AND (u.window_start < v.ws OR (u.window_start = v.ws AND u.epoch < v.ep))")
		res := c.db.WithContext(ctx).Exec(sb.String(), args...)
		if res.Error != nil {
			logger.Warn("ossync: counters: gc: %v", res.Error)
			return
		}
		removed += res.RowsAffected
	}

	// Rows and epochs of limits that were deleted. This asks the governance
	// tables, not this node's memory, so a limit another node knows of and
	// this one has not loaded yet is never taken for deleted.
	budgets, rateLimits := tables.TableBudget{}.TableName(), tables.TableRateLimit{}.TableName()
	for _, stmt := range []struct {
		sql  string
		args []any
	}{
		{"DELETE FROM astropods_ossync_usage u WHERE u.kind = ? AND NOT EXISTS (SELECT 1 FROM " + budgets + " b WHERE b.id = u.limit_id)", []any{ctrKindBudget}},
		{"DELETE FROM astropods_ossync_usage u WHERE u.kind IN (?, ?) AND NOT EXISTS (SELECT 1 FROM " + rateLimits + " r WHERE r.id = u.limit_id)", []any{ctrKindTokens, ctrKindRequests}},
		{"DELETE FROM astropods_ossync_epoch e WHERE e.kind = ? AND NOT EXISTS (SELECT 1 FROM " + budgets + " b WHERE b.id = e.limit_id)", []any{ctrKindBudget}},
	} {
		res := c.db.WithContext(ctx).Exec(stmt.sql, stmt.args...)
		if res.Error != nil {
			logger.Warn("ossync: counters: gc deleted limits: %v", res.Error)
			continue
		}
		removed += res.RowsAffected
	}

	if removed > 0 {
		logger.Debug("ossync: counters: gc removed %d row(s)", removed)
		c.lastFull = time.Time{} // drop them from the cache on the next tick
	}
}
