package server

// OSS multi-node sync for governance state.
//
// Bifrost OSS keeps governance entities (virtual keys, teams, customers, model
// configs, budgets) in each process's memory and reads Postgres only at
// startup, so an entity created through the API on one node is unknown to every
// other node until it restarts. Enterprise solves this with gossip; this file
// does the same job with Postgres LISTEN/NOTIFY, which every multi-node
// deployment already has.
//
// How it works:
//
//   - ossyncCallbacks wraps the server's ServerCallbacks. After a local
//     Reload*/Remove* or budget reset succeeds it queues {node, kind, op, ids};
//     one sender goroutine delivers the queue with pg_notify, so a slow database
//     never holds up the admin request. Messages carry only IDs; peers re-read
//     the row themselves, so duplicates and reordering are harmless (the
//     database state always wins).
//   - Every node holds one dedicated LISTEN connection and applies peers'
//     messages by calling the same local, non-broadcasting methods.
//   - Catch-up (every 5s) reloads entities whose row is newer than the
//     in-memory copy. Its window reaches back to the moment the listener last
//     dropped, so a long outage is repaired too, not only the last 2 minutes.
//   - Reconcile (every 60s) compares every virtual key row with memory, reloads
//     stale ones, and removes keys deleted since its previous pass.
//   - Sweep (every 10m) compares all of memory with the database for virtual
//     keys, teams, customers and model configs, and fixes anything left.
//   - Health is exported as bifrost_ossync_* Prometheus metrics on /metrics.
//
// Not synced: provider configuration and keys (the providers handler edits the
// in-memory config and client directly), virtual MCPs, and live budget and
// rate-limit usage counters, which stay per node.
//
// Nothing here is on the request path: if the listener or the loops fail, the
// node keeps serving from memory exactly as upstream OSS does.
//
// Enabled with BIFROST_OSS_SYNC=1. Requires a Postgres config store.

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/maximhq/bifrost/framework/configstore/tables"
	"github.com/maximhq/bifrost/framework/postgresconn"
	"github.com/maximhq/bifrost/plugins/governance"
	"github.com/maximhq/bifrost/plugins/telemetry"
	"github.com/maximhq/bifrost/transports/v2/bifrost-http/handlers"
	"github.com/maximhq/bifrost/transports/v2/bifrost-http/lib"
	"github.com/prometheus/client_golang/prometheus"
	"gorm.io/gorm"
)

const (
	ossyncChannel = "bifrost_ossync"
	// Postgres caps a NOTIFY payload at 8000 bytes. A UUID costs ~40 bytes in
	// JSON, so 150 IDs per message leaves ample headroom.
	ossyncMaxIDsPerMessage = 150
	// Messages waiting for the sender. A full queue drops the message (peers
	// converge through catch-up) rather than block an admin request.
	ossyncQueueSize = 4096
	// Rows read per query by catch-up, reconcile and sweep.
	ossyncPageSize = 1000

	ossyncKindVirtualKey    = "virtual_key"
	ossyncKindTeam          = "team"
	ossyncKindCustomer      = "customer"
	ossyncKindModelConfig   = "model_config"
	ossyncKindBudgetReset   = "budget_reset"
	ossyncKindCalendarAlign = "calendar_align"

	ossyncOpReload = "reload"
	ossyncOpRemove = "remove"
	ossyncOpReset  = "reset"
	ossyncOpPing   = "ping"
)

type ossyncMessage struct {
	Node string   `json:"n"`
	Kind string   `json:"k,omitempty"`
	Op   string   `json:"o"`
	IDs  []string `json:"i,omitempty"`
	// Budget resets and calendar alignment: IDs holds the budget IDs, these the
	// owner the handler named and, for alignment, the rate limit IDs.
	OwnerKind  string   `json:"ok,omitempty"`
	OwnerID    string   `json:"oi,omitempty"`
	RateLimits []string `json:"r,omitempty"`
}

// OSSync owns the listener, the sender and the repair loops for one node.
type OSSync struct {
	s      *BifrostHTTPServer
	db     *gorm.DB
	nodeID string

	catchupEvery   time.Duration
	catchupWindow  time.Duration
	reconcileEvery time.Duration
	sweepEvery     time.Duration

	queue chan ossyncMessage

	listening atomic.Bool // LISTEN session is up
	verified  atomic.Bool // our own ping came back through Postgres
	pingToken string

	// gapSince is the unix-nano time from which this node may have missed
	// notifications: process start, or the last time the listener dropped.
	// 0 means no open gap. Catch-up reaches back to it and clears it once a
	// pass that started while listening succeeds.
	gapSince atomic.Int64

	// prevVKIDs is the virtual key ID set reconcile saw last time. Only the
	// reconcile goroutine touches it.
	prevVKIDs map[string]struct{}

	stats ossyncStats

	startOnce sync.Once
}

type ossyncStats struct {
	Published        atomic.Int64
	PublishErrors    atomic.Int64
	Received         atomic.Int64
	Applied          atomic.Int64
	ApplyErrors      atomic.Int64
	CatchupReloads   atomic.Int64
	ReconcileRemoves atomic.Int64
	ReconcileReloads atomic.Int64
	SweepRepairs     atomic.Int64
	ListenerDrops    atomic.Int64
	LastAppliedUnix  atomic.Int64
}

type ossyncStamp struct {
	ID        string
	UpdatedAt time.Time
}

// newOSSyncFromEnv returns nil unless BIFROST_OSS_SYNC is on and the config
// store is Postgres.
func newOSSyncFromEnv(s *BifrostHTTPServer) *OSSync {
	if on, _ := strconv.ParseBool(os.Getenv("BIFROST_OSS_SYNC")); !on {
		return nil
	}
	if s.Config == nil || s.Config.ConfigStore == nil {
		logger.Warn("ossync: BIFROST_OSS_SYNC is set but there is no config store; sync disabled")
		return nil
	}
	db := s.Config.ConfigStore.DB()
	if db == nil || db.Dialector.Name() != "postgres" {
		logger.Warn("ossync: BIFROST_OSS_SYNC needs a postgres config store; sync disabled")
		return nil
	}
	nodeID := os.Getenv("BIFROST_NODE_ID")
	if nodeID == "" {
		host, _ := os.Hostname()
		nodeID = host + "-" + randomHex(4)
	}
	o := &OSSync{
		s:              s,
		db:             db,
		nodeID:         nodeID,
		catchupEvery:   envDuration("BIFROST_OSS_SYNC_CATCHUP_INTERVAL", 5*time.Second),
		catchupWindow:  envDuration("BIFROST_OSS_SYNC_CATCHUP_WINDOW", 2*time.Minute),
		reconcileEvery: envDuration("BIFROST_OSS_SYNC_RECONCILE_INTERVAL", 60*time.Second),
		sweepEvery:     envDuration("BIFROST_OSS_SYNC_SWEEP_INTERVAL", 10*time.Minute),
		queue:          make(chan ossyncMessage, ossyncQueueSize),
		pingToken:      randomHex(8),
	}
	// Memory was loaded from the database before this point and nothing was
	// listening since, so the gap opens now.
	o.gapSince.Store(time.Now().UnixNano())
	return o
}

// Wrap returns callbacks that broadcast governance changes after applying them
// locally.
func (o *OSSync) Wrap() ServerCallbacks {
	return &ossyncCallbacks{BifrostHTTPServer: o.s, sync: o}
}

// Start launches the sender, the listener and the repair loops. They stop with
// ctx. Call it only after the routes are registered.
func (o *OSSync) Start(ctx context.Context) {
	o.startOnce.Do(func() {
		logger.Info("ossync: enabled, node=%s catchup=%s reconcile=%s sweep=%s", o.nodeID, o.catchupEvery, o.reconcileEvery, o.sweepEvery)
		go o.sendLoop(ctx)
		go o.listenLoop(ctx)
		go o.every(ctx, o.catchupEvery, o.catchup)
		go o.every(ctx, o.reconcileEvery, o.reconcile)
		go o.every(ctx, o.sweepEvery, o.sweep)
		go o.registerMetricsLoop(ctx)
	})
}

// ---------------------------------------------------------------------------
// Publishing

func (o *OSSync) publish(kind, op string, ids ...string) {
	ids = compactIDs(ids)
	for start := 0; start < len(ids); start += ossyncMaxIDsPerMessage {
		end := min(start+ossyncMaxIDsPerMessage, len(ids))
		o.enqueue(ossyncMessage{Node: o.nodeID, Kind: kind, Op: op, IDs: ids[start:end]})
	}
}

// publishBudgets queues a budget reset or calendar alignment. Rate limits ride
// in the first message only, so a long budget list cannot apply them twice.
func (o *OSSync) publishBudgets(kind string, owner handlers.BudgetUsageResetOwner, budgetIDs, rateLimitIDs []string) {
	budgetIDs, rateLimitIDs = compactIDs(budgetIDs), compactIDs(rateLimitIDs)
	if len(budgetIDs) == 0 && len(rateLimitIDs) == 0 {
		return
	}
	for start := 0; start == 0 || start < len(budgetIDs); start += ossyncMaxIDsPerMessage {
		end := min(start+ossyncMaxIDsPerMessage, len(budgetIDs))
		msg := ossyncMessage{Node: o.nodeID, Kind: kind, Op: ossyncOpReset, IDs: budgetIDs[start:end], OwnerKind: owner.Kind, OwnerID: owner.ID}
		if start == 0 {
			msg.RateLimits = rateLimitIDs
		}
		o.enqueue(msg)
	}
}

// enqueue never blocks: an admin request must not wait on peers.
func (o *OSSync) enqueue(msg ossyncMessage) {
	select {
	case o.queue <- msg:
	default:
		o.stats.PublishErrors.Add(1)
		logger.Warn("ossync: send queue full, dropping %s/%s (peers will catch up)", msg.Kind, msg.Op)
	}
}

// sendLoop delivers queued messages one at a time, so peers see them in the
// order this node made the changes.
func (o *OSSync) sendLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-o.queue:
			o.send(msg)
		}
	}
}

func (o *OSSync) send(msg ossyncMessage) {
	payload, err := json.Marshal(msg)
	if err != nil {
		o.stats.PublishErrors.Add(1)
		logger.Error("ossync: encode %s/%s: %v", msg.Kind, msg.Op, err)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := o.db.WithContext(ctx).Exec("SELECT pg_notify(?, ?)", ossyncChannel, string(payload)).Error; err != nil {
		// Catch-up and reconcile will still converge peers.
		o.stats.PublishErrors.Add(1)
		logger.Warn("ossync: publish %s/%s failed (peers will catch up): %v", msg.Kind, msg.Op, err)
		return
	}
	if msg.Op != ossyncOpPing {
		o.stats.Published.Add(1)
	}
}

// ---------------------------------------------------------------------------
// Listening

func (o *OSSync) listenLoop(ctx context.Context) {
	sqlDB, err := o.db.DB()
	if err != nil {
		logger.Error("ossync: no sql.DB behind the config store, listener disabled: %v", err)
		return
	}
	retry := time.Second
	for {
		listened := false
		err := o.listenOnce(ctx, sqlDB, func() { listened = true })
		o.listening.Store(false)
		o.verified.Store(false)
		if ctx.Err() != nil {
			return
		}
		// Anything published from now on may be missed until catch-up runs.
		o.gapSince.CompareAndSwap(0, time.Now().UnixNano())
		o.stats.ListenerDrops.Add(1)
		if listened {
			retry = time.Second
		}
		logger.Warn("ossync: LISTEN dropped, retrying in %s: %v", retry, err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(retry):
		}
		retry = min(retry*2, 30*time.Second)
	}
}

// listenOnce holds one dedicated connection (outside the pool, so it can never
// starve request-time queries) in LISTEN until it fails or ctx ends.
func (o *OSSync) listenOnce(ctx context.Context, sqlDB *sql.DB, listening func()) error {
	pc, err := ossyncListenConn(ctx, sqlDB)
	if err != nil {
		return err
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		_ = pc.Close(closeCtx)
	}()
	if _, err := pc.Exec(ctx, "LISTEN "+ossyncChannel); err != nil {
		return err
	}
	listening()
	o.listening.Store(true)
	logger.Info("ossync: listening on %q", ossyncChannel)

	// Verify end to end: behind PgBouncer in transaction mode LISTEN succeeds
	// but never receives anything. Our own ping proves the path works.
	go o.send(ossyncMessage{Node: o.nodeID, Op: ossyncOpPing, IDs: []string{o.pingToken}})
	// Repair the gap now instead of waiting for the next tick.
	go func() {
		cctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
		defer cancel()
		o.catchup(cctx)
	}()

	for {
		n, err := pc.WaitForNotification(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return err
		}
		o.handle(n.Payload)
	}
}

// ossyncListenConn opens a connection the pool does not manage, configured the
// way the pool opens its own (same approach as framework/queue/pgnotify.go).
func ossyncListenConn(ctx context.Context, sqlDB *sql.DB) (*pgx.Conn, error) {
	if pc, ok, err := postgresconn.ConnectUnpooled(ctx, sqlDB); ok {
		return pc, err
	}
	conn, err := sqlDB.Conn(ctx)
	if err != nil {
		return nil, err
	}
	var cfg *pgx.ConnConfig
	err = conn.Raw(func(driverConn any) error {
		sc, ok := driverConn.(*stdlib.Conn)
		if !ok {
			return fmt.Errorf("postgres driver connection is %T, not pgx", driverConn)
		}
		cfg = sc.Conn().Config()
		cfg.OnNotification = nil
		return nil
	})
	_ = conn.Close()
	if err != nil {
		return nil, err
	}
	return pgx.ConnectConfig(ctx, cfg)
}

// ---------------------------------------------------------------------------
// Applying peers' messages

func (o *OSSync) handle(payload string) {
	var msg ossyncMessage
	if err := json.Unmarshal([]byte(payload), &msg); err != nil {
		logger.Warn("ossync: ignoring malformed message: %v", err)
		return
	}
	if msg.Op == ossyncOpPing {
		if msg.Node == o.nodeID && len(msg.IDs) == 1 && msg.IDs[0] == o.pingToken {
			if !o.verified.Swap(true) {
				logger.Info("ossync: listener verified (own NOTIFY received)")
			}
		}
		return
	}
	if msg.Node == o.nodeID {
		return // already applied locally
	}
	o.stats.Received.Add(1)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := o.apply(ctx, msg); err != nil {
		o.stats.ApplyErrors.Add(1)
		logger.Warn("ossync: apply %s/%s from %s failed (repair loops will retry): %v", msg.Kind, msg.Op, msg.Node, err)
		return
	}
	o.stats.Applied.Add(1)
	o.stats.LastAppliedUnix.Store(time.Now().Unix())
	logger.Debug("ossync: applied %s/%s x%d from %s", msg.Kind, msg.Op, len(msg.IDs), msg.Node)
}

// apply calls the server's own (non-broadcasting) methods.
func (o *OSSync) apply(ctx context.Context, msg ossyncMessage) error {
	s := o.s
	switch msg.Kind {
	case ossyncKindVirtualKey:
		if msg.Op == ossyncOpRemove {
			return o.forEach(msg.IDs, "vk", func(id string) error { return s.RemoveVirtualKey(ctx, id) })
		}
		if !o.reloadVirtualKeys(ctx, msg.IDs) {
			return errors.New("one or more virtual keys failed to reload")
		}
		return nil
	case ossyncKindTeam, ossyncKindCustomer, ossyncKindModelConfig:
		e := o.entity(msg.Kind)
		if msg.Op == ossyncOpRemove {
			return o.forEach(msg.IDs, e.kind, func(id string) error { return e.remove(ctx, id) })
		}
		return o.forEach(msg.IDs, e.kind, func(id string) error { return e.reload(ctx, id) })
	case ossyncKindBudgetReset:
		// The peer's own reload of the owner, sent just before this, carried its
		// cached usage forward; this clears it, so its next dump cannot write the
		// old usage back over the reset.
		return s.ResetBudgetUsageInMemory(ctx, handlers.BudgetUsageResetOwner{Kind: msg.OwnerKind, ID: msg.OwnerID}, msg.IDs)
	case ossyncKindCalendarAlign:
		return s.AdoptCalendarAlignmentInMemory(ctx, handlers.BudgetUsageResetOwner{Kind: msg.OwnerKind, ID: msg.OwnerID}, msg.IDs, msg.RateLimits)
	default:
		return fmt.Errorf("unknown kind %q", msg.Kind)
	}
}

func (o *OSSync) forEach(ids []string, label string, fn func(id string) error) error {
	var errs []error
	for _, id := range ids {
		if err := fn(id); err != nil {
			errs = append(errs, fmt.Errorf("%s %s: %w", label, id, err))
		}
	}
	return errors.Join(errs...)
}

// reloadVirtualKeys reloads through the batched read, falling back per key.
// It reports whether every key loaded.
func (o *OSSync) reloadVirtualKeys(ctx context.Context, ids []string) bool {
	if len(ids) == 0 {
		return true
	}
	if len(ids) > 1 {
		if err := o.s.ReloadVirtualKeys(ctx, ids); err == nil {
			return true
		}
	}
	ok := true
	for _, id := range ids {
		if _, err := o.s.ReloadVirtualKey(ctx, id); err != nil {
			ok = false
			logger.Warn("ossync: reload virtual key %s: %v", id, err)
		}
	}
	return ok
}

// ---------------------------------------------------------------------------
// Entities other than virtual keys

type ossyncEntity struct {
	kind     string
	model    any
	inMemory func(d *governance.GovernanceData) map[string]time.Time
	reload   func(ctx context.Context, id string) error
	remove   func(ctx context.Context, id string) error
}

func (o *OSSync) entities() []ossyncEntity {
	s := o.s
	return []ossyncEntity{
		{
			kind:  ossyncKindTeam,
			model: &tables.TableTeam{},
			inMemory: func(d *governance.GovernanceData) map[string]time.Time {
				m := make(map[string]time.Time, len(d.Teams))
				for _, t := range d.Teams {
					if t != nil {
						m[t.ID] = t.UpdatedAt
					}
				}
				return m
			},
			reload: func(ctx context.Context, id string) error { _, err := s.ReloadTeam(ctx, id); return err },
			remove: s.RemoveTeam,
		},
		{
			kind:  ossyncKindCustomer,
			model: &tables.TableCustomer{},
			inMemory: func(d *governance.GovernanceData) map[string]time.Time {
				m := make(map[string]time.Time, len(d.Customers))
				for _, c := range d.Customers {
					if c != nil {
						m[c.ID] = c.UpdatedAt
					}
				}
				return m
			},
			reload: func(ctx context.Context, id string) error { _, err := s.ReloadCustomer(ctx, id); return err },
			remove: s.RemoveCustomer,
		},
		{
			kind:  ossyncKindModelConfig,
			model: &tables.TableModelConfig{},
			inMemory: func(d *governance.GovernanceData) map[string]time.Time {
				m := make(map[string]time.Time, len(d.ModelConfigs))
				for _, mc := range d.ModelConfigs {
					if mc != nil {
						m[mc.ID] = mc.UpdatedAt
					}
				}
				return m
			},
			reload: func(ctx context.Context, id string) error { _, err := s.ReloadModelConfig(ctx, id); return err },
			remove: s.RemoveModelConfig,
		},
	}
}

func (o *OSSync) entity(kind string) ossyncEntity {
	for _, e := range o.entities() {
		if e.kind == kind {
			return e
		}
	}
	panic("ossync: no entity " + kind)
}

// repairEntities reloads teams, customers and model configs whose row is newer
// than memory. With since nil it compares every row and also removes entities
// that are in memory but no longer in the database. data, when nil, is read
// lazily, so a pass that finds no changed rows does not snapshot memory.
func (o *OSSync) repairEntities(ctx context.Context, store governance.GovernanceStore, data *governance.GovernanceData, since *time.Time) (int, error) {
	repaired := 0
	var errs []error
	for _, e := range o.entities() {
		var rows []ossyncStamp
		if err := o.pageStamps(ctx, e.model, since, func(page []ossyncStamp) error {
			rows = append(rows, page...)
			return nil
		}); err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", e.kind, err))
			continue
		}
		if since != nil && len(rows) == 0 {
			continue
		}
		if data == nil {
			if data = store.GetGovernanceData(ctx); data == nil {
				return repaired, errors.New("governance data unavailable")
			}
		}
		mem := e.inMemory(data)
		inDB := make(map[string]struct{}, len(rows))
		for _, r := range rows {
			inDB[r.ID] = struct{}{}
			if at, ok := mem[r.ID]; ok && !at.Before(r.UpdatedAt) {
				continue
			}
			if err := e.reload(ctx, r.ID); err != nil {
				errs = append(errs, fmt.Errorf("%s %s: %w", e.kind, r.ID, err))
				continue
			}
			repaired++
		}
		if since != nil {
			continue
		}
		for id := range mem {
			if _, ok := inDB[id]; ok {
				continue
			}
			if err := e.remove(ctx, id); err != nil {
				errs = append(errs, fmt.Errorf("%s %s: %w", e.kind, id, err))
				continue
			}
			repaired++
		}
	}
	return repaired, errors.Join(errs...)
}

// ---------------------------------------------------------------------------
// Repair loops

func (o *OSSync) every(ctx context.Context, interval time.Duration, fn func(context.Context)) {
	if interval <= 0 {
		return
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			runCtx, cancel := context.WithTimeout(ctx, max(interval, time.Minute))
			fn(runCtx)
			cancel()
		}
	}
}

// pageStamps reads (id, updated_at) for a table in pages of ossyncPageSize and
// hands each page to fn. With since set it reads only rows updated after it,
// in (updated_at, id) order; otherwise every row, in id order. Paging means a
// burst of changes larger than one page is still read to the end.
func (o *OSSync) pageStamps(ctx context.Context, model any, since *time.Time, fn func([]ossyncStamp) error) error {
	var lastAt time.Time
	lastID := ""
	for first := true; ; first = false {
		q := o.db.WithContext(ctx).Model(model).Select("id, updated_at")
		if since != nil {
			if first {
				q = q.Where("updated_at > ?", *since)
			} else {
				q = q.Where("updated_at > ? OR (updated_at = ? AND id > ?)", lastAt, lastAt, lastID)
			}
			q = q.Order("updated_at, id")
		} else {
			if !first {
				q = q.Where("id > ?", lastID)
			}
			q = q.Order("id")
		}
		var rows []ossyncStamp
		if err := q.Limit(ossyncPageSize).Scan(&rows).Error; err != nil {
			return err
		}
		if len(rows) > 0 {
			if err := fn(rows); err != nil {
				return err
			}
			last := rows[len(rows)-1]
			lastAt, lastID = last.UpdatedAt, last.ID
		}
		if len(rows) < ossyncPageSize {
			return nil
		}
	}
}

// staleVirtualKeys returns the IDs of rows that memory lacks or holds an older
// copy of. It also reports every ID it saw, for reconcile.
func (o *OSSync) staleVirtualKeys(ctx context.Context, store governance.GovernanceStore, since *time.Time, seen map[string]struct{}) ([]string, error) {
	var stale []string
	err := o.pageStamps(ctx, &tables.TableVirtualKey{}, since, func(rows []ossyncStamp) error {
		for _, r := range rows {
			if seen != nil {
				seen[r.ID] = struct{}{}
			}
			vk, found := store.GetVirtualKeyByID(ctx, r.ID)
			if !found || vk.UpdatedAt.Before(r.UpdatedAt) {
				stale = append(stale, r.ID)
			}
		}
		return nil
	})
	return stale, err
}

// catchup reloads entities whose row is newer than memory. It looks back
// catchupWindow, or to the open gap if that is older, so a listener that was
// down for an hour still gets the hour's changes. The window tolerates clock
// skew between the nodes that stamp updated_at.
func (o *OSSync) catchup(ctx context.Context) {
	store, ok := o.governanceStore()
	if !ok {
		return
	}
	gap := o.gapSince.Load()
	listeningAtStart := o.listening.Load()
	since := time.Now().Add(-o.catchupWindow)
	if gap != 0 {
		if g := time.Unix(0, gap).Add(-o.catchupWindow); g.Before(since) {
			since = g
		}
	}

	clean := true
	stale, err := o.staleVirtualKeys(ctx, store, &since, nil)
	if err != nil {
		clean = false
		logger.Warn("ossync: catch-up query failed: %v", err)
	} else if len(stale) > 0 {
		o.stats.CatchupReloads.Add(int64(len(stale)))
		logger.Info("ossync: catch-up reloading %d virtual key(s) that missed a notification", len(stale))
		clean = o.reloadVirtualKeys(ctx, stale)
	}
	n, err := o.repairEntities(ctx, store, nil, &since)
	if n > 0 {
		o.stats.CatchupReloads.Add(int64(n))
		logger.Info("ossync: catch-up repaired %d team(s), customer(s) or model config(s)", n)
	}
	if err != nil {
		clean = false
		logger.Warn("ossync: catch-up: %v", err)
	}

	// Close the gap only when this pass covered all of it: it began while the
	// listener was up and everything it found loaded.
	if clean && listeningAtStart && gap != 0 && o.gapSince.CompareAndSwap(gap, 0) {
		logger.Info("ossync: caught up since %s", time.Unix(0, gap).UTC().Format(time.RFC3339))
	}
}

// reconcile compares every virtual key row with memory, reloads the stale or
// missing ones, and removes keys whose row disappeared since its previous pass.
// It reads only (id, updated_at), a page at a time.
func (o *OSSync) reconcile(ctx context.Context) {
	store, ok := o.governanceStore()
	if !ok {
		return
	}
	seen := make(map[string]struct{}, len(o.prevVKIDs))
	stale, err := o.staleVirtualKeys(ctx, store, nil, seen)
	if err != nil {
		logger.Warn("ossync: reconcile query failed: %v", err)
		return
	}
	if len(stale) > 0 {
		o.stats.ReconcileReloads.Add(int64(len(stale)))
		logger.Info("ossync: reconcile reloading %d stale or missing virtual key(s)", len(stale))
		o.reloadVirtualKeys(ctx, stale)
	}
	for id := range o.prevVKIDs {
		if _, ok := seen[id]; ok {
			continue
		}
		if err := o.s.RemoveVirtualKey(ctx, id); err != nil {
			logger.Warn("ossync: reconcile remove %s: %v", id, err)
			continue
		}
		o.stats.ReconcileRemoves.Add(1)
		logger.Info("ossync: reconcile removed virtual key %s (deleted on another node)", id)
	}
	o.prevVKIDs = seen
}

// sweep compares all of memory with the database: virtual keys memory holds
// whose row is gone (including any created and deleted between two reconcile
// passes), and every team, customer and model config. It is the only pass that
// snapshots the whole governance state, so it runs rarely.
func (o *OSSync) sweep(ctx context.Context) {
	store, ok := o.governanceStore()
	if !ok {
		return
	}
	// Snapshot memory before reading the database, so an entity created in
	// between is never mistaken for a deleted one.
	data := store.GetGovernanceData(ctx)
	if data == nil {
		return
	}
	inDB := make(map[string]struct{}, len(data.VirtualKeys))
	if err := o.pageStamps(ctx, &tables.TableVirtualKey{}, nil, func(rows []ossyncStamp) error {
		for _, r := range rows {
			inDB[r.ID] = struct{}{}
		}
		return nil
	}); err != nil {
		logger.Warn("ossync: sweep query failed: %v", err)
		return
	}
	repaired := 0
	for _, vk := range data.VirtualKeys {
		if vk == nil {
			continue
		}
		if _, ok := inDB[vk.ID]; ok {
			continue
		}
		if err := o.s.RemoveVirtualKey(ctx, vk.ID); err != nil {
			logger.Warn("ossync: sweep remove %s: %v", vk.ID, err)
			continue
		}
		repaired++
	}
	n, err := o.repairEntities(ctx, store, data, nil)
	repaired += n
	if err != nil {
		logger.Warn("ossync: sweep: %v", err)
	}
	if repaired > 0 {
		o.stats.SweepRepairs.Add(int64(repaired))
		logger.Info("ossync: sweep repaired %d entit(ies) the other loops missed", repaired)
	}
}

func (o *OSSync) governanceStore() (governance.GovernanceStore, bool) {
	p, err := o.s.getGovernancePlugin()
	if err != nil || p == nil {
		return nil, false
	}
	store := p.GetGovernanceStore()
	return store, store != nil
}

// ---------------------------------------------------------------------------
// Prometheus metrics
//
// Served on Bifrost's existing /metrics (the telemetry plugin's registry), so
// they follow its enable switch and auth, and reach its push gateway. The
// collector reads the live counters at scrape time; nothing is duplicated.
//
// Suggested alerts:
//   bifrost_ossync_listener_connected == 0 for 30s     -> node is not hearing peers
//   bifrost_ossync_catchup_gap_open == 1 for 5m         -> node cannot repair a gap
//   rate(bifrost_ossync_catchup_reloads_total[15m]) > 0 -> notifications are being lost
//   rate(bifrost_ossync_sweep_repairs_total[1h]) > 0    -> catch-up and reconcile are missing changes

type ossyncMetric struct {
	desc  *prometheus.Desc
	kind  prometheus.ValueType
	value func(o *OSSync) float64
}

func boolMetric(b *atomic.Bool) float64 {
	if b.Load() {
		return 1
	}
	return 0
}

// ossyncCollector implements prometheus.Collector over an OSSync's counters.
type ossyncCollector struct {
	o       *OSSync
	metrics []ossyncMetric
}

func newOSSyncCollector(o *OSSync) *ossyncCollector {
	labels := prometheus.Labels{"node": o.nodeID}
	m := func(name, help string, kind prometheus.ValueType, value func(o *OSSync) float64) ossyncMetric {
		return ossyncMetric{desc: prometheus.NewDesc("bifrost_ossync_"+name, help, nil, labels), kind: kind, value: value}
	}
	gauge, counter := prometheus.GaugeValue, prometheus.CounterValue
	return &ossyncCollector{o: o, metrics: []ossyncMetric{
		m("listener_connected", "1 while this node's LISTEN connection is up.", gauge,
			func(o *OSSync) float64 { return boolMetric(&o.listening) }),
		m("listener_verified", "1 once this node has received its own NOTIFY ping on the current connection.", gauge,
			func(o *OSSync) float64 { return boolMetric(&o.verified) }),
		m("listener_drops_total", "LISTEN connections lost and retried.", counter,
			func(o *OSSync) float64 { return float64(o.stats.ListenerDrops.Load()) }),
		m("catchup_gap_open", "1 while this node may have missed notifications that catch-up has not repaired yet.", gauge,
			func(o *OSSync) float64 {
				if o.gapSince.Load() != 0 {
					return 1
				}
				return 0
			}),
		m("send_queue_length", "Change notifications waiting to be sent.", gauge,
			func(o *OSSync) float64 { return float64(len(o.queue)) }),
		m("messages_published_total", "Change notifications this node sent.", counter,
			func(o *OSSync) float64 { return float64(o.stats.Published.Load()) }),
		m("publish_errors_total", "Change notifications not sent: a failed send or a full queue (peers converge via catch-up).", counter,
			func(o *OSSync) float64 { return float64(o.stats.PublishErrors.Load()) }),
		m("messages_received_total", "Change notifications received from other nodes.", counter,
			func(o *OSSync) float64 { return float64(o.stats.Received.Load()) }),
		m("messages_applied_total", "Received notifications applied to memory.", counter,
			func(o *OSSync) float64 { return float64(o.stats.Applied.Load()) }),
		m("apply_errors_total", "Received notifications that failed to apply (retried by the repair loops).", counter,
			func(o *OSSync) float64 { return float64(o.stats.ApplyErrors.Load()) }),
		m("catchup_reloads_total", "Entities reloaded by catch-up because a notification was missed.", counter,
			func(o *OSSync) float64 { return float64(o.stats.CatchupReloads.Load()) }),
		m("reconcile_removes_total", "Virtual keys removed by reconcile because their delete was missed.", counter,
			func(o *OSSync) float64 { return float64(o.stats.ReconcileRemoves.Load()) }),
		m("reconcile_reloads_total", "Virtual keys reloaded by reconcile because memory was stale or missing them.", counter,
			func(o *OSSync) float64 { return float64(o.stats.ReconcileReloads.Load()) }),
		m("sweep_repairs_total", "Entities fixed by the sweep that catch-up and reconcile had missed.", counter,
			func(o *OSSync) float64 { return float64(o.stats.SweepRepairs.Load()) }),
		m("last_applied_timestamp_seconds", "Unix time of the last notification applied from another node (0 if none).", gauge,
			func(o *OSSync) float64 { return float64(o.stats.LastAppliedUnix.Load()) }),
	}}
}

func (c *ossyncCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, m := range c.metrics {
		ch <- m.desc
	}
}

func (c *ossyncCollector) Collect(ch chan<- prometheus.Metric) {
	for _, m := range c.metrics {
		ch <- prometheus.MustNewConstMetric(m.desc, m.kind, m.value(c.o))
	}
}

// registerMetricsLoop registers the collector with the telemetry plugin's
// registry, and again whenever the plugin is reloaded with a fresh registry.
// Without the telemetry plugin there is no /metrics, and this does nothing.
func (o *OSSync) registerMetricsLoop(ctx context.Context) {
	collector := newOSSyncCollector(o)
	var current *prometheus.Registry
	try := func() {
		plugin, err := lib.FindPluginAs[*telemetry.PrometheusPlugin](o.s.Config, telemetry.PluginName)
		if err != nil || plugin == nil {
			return
		}
		reg := plugin.GetRegistry()
		if reg == nil || reg == current {
			return
		}
		if err := reg.Register(collector); err != nil {
			var already prometheus.AlreadyRegisteredError
			if !errors.As(err, &already) {
				logger.Warn("ossync: register metrics: %v", err)
				return
			}
		}
		current = reg
		logger.Info("ossync: metrics registered on /metrics")
	}
	try()
	t := time.NewTicker(10 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			try()
		}
	}
}

// ---------------------------------------------------------------------------
// Callback wrapper

// ossyncCallbacks embeds the server, so every ServerCallbacks method (and any
// optional interface handlers type-assert for) is promoted unchanged. Only the
// governance mutations are overridden to broadcast after the local apply.
type ossyncCallbacks struct {
	*BifrostHTTPServer
	sync *OSSync
}

func (c *ossyncCallbacks) ReloadVirtualKey(ctx context.Context, id string) (*tables.TableVirtualKey, error) {
	vk, err := c.BifrostHTTPServer.ReloadVirtualKey(ctx, id)
	if err == nil {
		c.sync.publish(ossyncKindVirtualKey, ossyncOpReload, id)
	}
	return vk, err
}

func (c *ossyncCallbacks) ReloadVirtualKeys(ctx context.Context, ids []string) error {
	err := c.BifrostHTTPServer.ReloadVirtualKeys(ctx, ids)
	if err == nil {
		c.sync.publish(ossyncKindVirtualKey, ossyncOpReload, ids...)
	}
	return err
}

func (c *ossyncCallbacks) RemoveVirtualKey(ctx context.Context, id string) error {
	err := c.BifrostHTTPServer.RemoveVirtualKey(ctx, id)
	if err == nil {
		c.sync.publish(ossyncKindVirtualKey, ossyncOpRemove, id)
	}
	return err
}

func (c *ossyncCallbacks) ReloadTeam(ctx context.Context, id string) (*tables.TableTeam, error) {
	t, err := c.BifrostHTTPServer.ReloadTeam(ctx, id)
	if err == nil {
		c.sync.publish(ossyncKindTeam, ossyncOpReload, id)
	}
	return t, err
}

func (c *ossyncCallbacks) RemoveTeam(ctx context.Context, id string) error {
	err := c.BifrostHTTPServer.RemoveTeam(ctx, id)
	if err == nil {
		c.sync.publish(ossyncKindTeam, ossyncOpRemove, id)
	}
	return err
}

func (c *ossyncCallbacks) ReloadCustomer(ctx context.Context, id string) (*tables.TableCustomer, error) {
	cu, err := c.BifrostHTTPServer.ReloadCustomer(ctx, id)
	if err == nil {
		c.sync.publish(ossyncKindCustomer, ossyncOpReload, id)
	}
	return cu, err
}

func (c *ossyncCallbacks) RemoveCustomer(ctx context.Context, id string) error {
	err := c.BifrostHTTPServer.RemoveCustomer(ctx, id)
	if err == nil {
		c.sync.publish(ossyncKindCustomer, ossyncOpRemove, id)
	}
	return err
}

func (c *ossyncCallbacks) ReloadModelConfig(ctx context.Context, id string) (*tables.TableModelConfig, error) {
	mc, err := c.BifrostHTTPServer.ReloadModelConfig(ctx, id)
	if err == nil {
		c.sync.publish(ossyncKindModelConfig, ossyncOpReload, id)
	}
	return mc, err
}

func (c *ossyncCallbacks) RemoveModelConfig(ctx context.Context, id string) error {
	err := c.BifrostHTTPServer.RemoveModelConfig(ctx, id)
	if err == nil {
		c.sync.publish(ossyncKindModelConfig, ossyncOpRemove, id)
	}
	return err
}

// ResetBudgetUsageInMemory must reach peers: each keeps the old usage in
// memory, and its next dump would write it back over the reset in Postgres.
func (c *ossyncCallbacks) ResetBudgetUsageInMemory(ctx context.Context, owner handlers.BudgetUsageResetOwner, budgetIDs []string) error {
	err := c.BifrostHTTPServer.ResetBudgetUsageInMemory(ctx, owner, budgetIDs)
	if err == nil {
		c.sync.publishBudgets(ossyncKindBudgetReset, owner, budgetIDs, nil)
	}
	return err
}

func (c *ossyncCallbacks) AdoptCalendarAlignmentInMemory(ctx context.Context, owner handlers.BudgetUsageResetOwner, budgetIDs []string, rateLimitIDs []string) error {
	err := c.BifrostHTTPServer.AdoptCalendarAlignmentInMemory(ctx, owner, budgetIDs, rateLimitIDs)
	if err == nil {
		c.sync.publishBudgets(ossyncKindCalendarAlign, owner, budgetIDs, rateLimitIDs)
	}
	return err
}

// ---------------------------------------------------------------------------
// Helpers

func compactIDs(ids []string) []string {
	seen := make(map[string]struct{}, len(ids))
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		if id == "" {
			continue
		}
		if _, dup := seen[id]; dup {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	return out
}

func envDuration(name string, def time.Duration) time.Duration {
	if raw := os.Getenv(name); raw != "" {
		if d, err := time.ParseDuration(raw); err == nil {
			return d
		}
		logger.Warn("ossync: invalid %s=%q, using %s", name, raw, def)
	}
	return def
}

func randomHex(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return strconv.FormatInt(time.Now().UnixNano(), 16)
	}
	return hex.EncodeToString(b)
}
