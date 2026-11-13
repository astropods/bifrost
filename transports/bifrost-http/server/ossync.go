package server

// OSS multi-node sync (proof of concept).
//
// Bifrost OSS keeps governance entities (virtual keys, teams, customers, model
// configs) in each process's memory and reads Postgres only at startup, so an
// entity created through the API on one node is unknown to every other node
// until it restarts. Enterprise solves this with gossip; this file does the
// same job with Postgres LISTEN/NOTIFY, which every multi-node deployment
// already has.
//
// How it works:
//
//   - ossyncCallbacks wraps the server's ServerCallbacks. After a local
//     Reload*/Remove* succeeds it publishes {node, kind, op, ids} with
//     pg_notify. Messages carry only IDs; peers re-read the row themselves, so
//     duplicates and reordering are harmless (the database state always wins).
//   - Every node holds one dedicated LISTEN connection and applies peers'
//     messages by calling the same local, non-broadcasting methods.
//   - A catch-up loop reloads virtual keys whose updated_at is newer than the
//     in-memory copy, covering lost notifications and listener downtime.
//   - A reconcile loop compares in-memory virtual key IDs with the table and
//     drops keys that were deleted while a notification was missed.
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
	"github.com/valyala/fasthttp"
	"gorm.io/gorm"
)

const (
	ossyncChannel = "bifrost_ossync"
	// Postgres caps a NOTIFY payload at 8000 bytes. A UUID costs ~40 bytes in
	// JSON, so 150 IDs per message leaves ample headroom.
	ossyncMaxIDsPerMessage = 150

	ossyncKindVirtualKey  = "virtual_key"
	ossyncKindTeam        = "team"
	ossyncKindCustomer    = "customer"
	ossyncKindModelConfig = "model_config"

	ossyncOpReload = "reload"
	ossyncOpRemove = "remove"
	ossyncOpPing   = "ping"
)

type ossyncMessage struct {
	Node string   `json:"n"`
	Kind string   `json:"k,omitempty"`
	Op   string   `json:"o"`
	IDs  []string `json:"i,omitempty"`
}

// OSSync owns the listener and the safety loops for one node.
type OSSync struct {
	s      *BifrostHTTPServer
	db     *gorm.DB
	nodeID string

	catchupEvery   time.Duration
	catchupWindow  time.Duration
	reconcileEvery time.Duration

	listening atomic.Bool // LISTEN session is up
	verified  atomic.Bool // our own ping came back through Postgres
	pingToken string

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
	ListenerDrops    atomic.Int64
	LastAppliedUnix  atomic.Int64
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
	return &OSSync{
		s:              s,
		db:             db,
		nodeID:         nodeID,
		catchupEvery:   envDuration("BIFROST_OSS_SYNC_CATCHUP_INTERVAL", 5*time.Second),
		catchupWindow:  envDuration("BIFROST_OSS_SYNC_CATCHUP_WINDOW", 2*time.Minute),
		reconcileEvery: envDuration("BIFROST_OSS_SYNC_RECONCILE_INTERVAL", 60*time.Second),
		pingToken:      randomHex(8),
	}
}

// Wrap returns callbacks that broadcast governance changes after applying them
// locally.
func (o *OSSync) Wrap() ServerCallbacks {
	return &ossyncCallbacks{BifrostHTTPServer: o.s, sync: o}
}

// Start launches the listener and the safety loops. They stop with ctx.
func (o *OSSync) Start(ctx context.Context) {
	o.startOnce.Do(func() {
		logger.Info("ossync: enabled, node=%s catchup=%s reconcile=%s", o.nodeID, o.catchupEvery, o.reconcileEvery)
		go o.listenLoop(ctx)
		go o.every(ctx, o.catchupEvery, o.catchup)
		go o.every(ctx, o.reconcileEvery, o.reconcile)
	})
}

// ---------------------------------------------------------------------------
// Publishing

func (o *OSSync) publish(kind, op string, ids ...string) {
	ids = compactIDs(ids)
	if len(ids) == 0 {
		return
	}
	for start := 0; start < len(ids); start += ossyncMaxIDsPerMessage {
		end := min(start+ossyncMaxIDsPerMessage, len(ids))
		o.send(ossyncMessage{Node: o.nodeID, Kind: kind, Op: op, IDs: ids[start:end]})
	}
}

func (o *OSSync) send(msg ossyncMessage) {
	payload, err := json.Marshal(msg)
	if err != nil {
		o.stats.PublishErrors.Add(1)
		logger.Error("ossync: encode %s/%s: %v", msg.Kind, msg.Op, err)
		return
	}
	// Detached from the request context: the local change already happened, and
	// a client disconnect must not stop peers from hearing about it.
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
	// Anything published while we were not listening is picked up here
	// instead of waiting for the next tick.
	go func() {
		cctx, cancel := context.WithTimeout(ctx, 30*time.Second)
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
		logger.Warn("ossync: apply %s/%s from %s failed (catch-up will retry): %v", msg.Kind, msg.Op, msg.Node, err)
		return
	}
	o.stats.Applied.Add(1)
	o.stats.LastAppliedUnix.Store(time.Now().Unix())
	logger.Debug("ossync: applied %s/%s x%d from %s", msg.Kind, msg.Op, len(msg.IDs), msg.Node)
}

// apply calls the server's own (non-broadcasting) methods.
func (o *OSSync) apply(ctx context.Context, msg ossyncMessage) error {
	s := o.s
	var errs []error
	switch msg.Kind {
	case ossyncKindVirtualKey:
		switch msg.Op {
		case ossyncOpReload:
			if len(msg.IDs) > 1 {
				if err := s.ReloadVirtualKeys(ctx, msg.IDs); err == nil {
					return nil
				}
			}
			for _, id := range msg.IDs {
				if _, err := s.ReloadVirtualKey(ctx, id); err != nil {
					errs = append(errs, fmt.Errorf("vk %s: %w", id, err))
				}
			}
		case ossyncOpRemove:
			for _, id := range msg.IDs {
				if err := s.RemoveVirtualKey(ctx, id); err != nil {
					errs = append(errs, fmt.Errorf("vk %s: %w", id, err))
				}
			}
		}
	case ossyncKindTeam:
		for _, id := range msg.IDs {
			var err error
			if msg.Op == ossyncOpRemove {
				err = s.RemoveTeam(ctx, id)
			} else {
				_, err = s.ReloadTeam(ctx, id)
			}
			if err != nil {
				errs = append(errs, fmt.Errorf("team %s: %w", id, err))
			}
		}
	case ossyncKindCustomer:
		for _, id := range msg.IDs {
			var err error
			if msg.Op == ossyncOpRemove {
				err = s.RemoveCustomer(ctx, id)
			} else {
				_, err = s.ReloadCustomer(ctx, id)
			}
			if err != nil {
				errs = append(errs, fmt.Errorf("customer %s: %w", id, err))
			}
		}
	case ossyncKindModelConfig:
		for _, id := range msg.IDs {
			var err error
			if msg.Op == ossyncOpRemove {
				err = s.RemoveModelConfig(ctx, id)
			} else {
				_, err = s.ReloadModelConfig(ctx, id)
			}
			if err != nil {
				errs = append(errs, fmt.Errorf("model config %s: %w", id, err))
			}
		}
	default:
		return fmt.Errorf("unknown kind %q", msg.Kind)
	}
	return errors.Join(errs...)
}

// ---------------------------------------------------------------------------
// Safety loops

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
			runCtx, cancel := context.WithTimeout(ctx, interval)
			fn(runCtx)
			cancel()
		}
	}
}

type ossyncVKStamp struct {
	ID        string
	UpdatedAt time.Time
}

// catchup reloads virtual keys whose row is newer than the in-memory copy.
// A sliding window (rather than a watermark) tolerates clock skew between the
// nodes that stamp updated_at.
func (o *OSSync) catchup(ctx context.Context) {
	store, ok := o.governanceStore()
	if !ok {
		return
	}
	var rows []ossyncVKStamp
	err := o.db.WithContext(ctx).
		Model(&tables.TableVirtualKey{}).
		Select("id, updated_at").
		Where("updated_at > ?", time.Now().Add(-o.catchupWindow)).
		Order("updated_at").
		Limit(5000).
		Scan(&rows).Error
	if err != nil {
		logger.Warn("ossync: catch-up query failed: %v", err)
		return
	}
	var stale []string
	for _, r := range rows {
		vk, found := store.GetVirtualKeyByID(ctx, r.ID)
		if !found || vk.UpdatedAt.Before(r.UpdatedAt) {
			stale = append(stale, r.ID)
		}
	}
	if len(stale) == 0 {
		return
	}
	o.stats.CatchupReloads.Add(int64(len(stale)))
	logger.Info("ossync: catch-up reloading %d virtual key(s) that missed a notification", len(stale))
	o.reloadLocal(ctx, stale)
}

// reconcile drops in-memory virtual keys whose row is gone and loads rows the
// node has never seen. Memory is snapshotted before the query, so a key
// created in between is never mistaken for a deleted one.
func (o *OSSync) reconcile(ctx context.Context) {
	store, ok := o.governanceStore()
	if !ok {
		return
	}
	data := store.GetGovernanceData(ctx)
	if data == nil {
		return
	}
	inMemory := make(map[string]struct{}, len(data.VirtualKeys))
	for _, vk := range data.VirtualKeys {
		if vk != nil {
			inMemory[vk.ID] = struct{}{}
		}
	}
	var ids []string
	if err := o.db.WithContext(ctx).Model(&tables.TableVirtualKey{}).Pluck("id", &ids).Error; err != nil {
		logger.Warn("ossync: reconcile query failed: %v", err)
		return
	}
	inDB := make(map[string]struct{}, len(ids))
	var missing []string
	for _, id := range ids {
		inDB[id] = struct{}{}
		if _, ok := inMemory[id]; !ok {
			missing = append(missing, id)
		}
	}
	for id := range inMemory {
		if _, ok := inDB[id]; ok {
			continue
		}
		if err := o.s.RemoveVirtualKey(ctx, id); err != nil {
			logger.Warn("ossync: reconcile remove %s: %v", id, err)
			continue
		}
		o.stats.ReconcileRemoves.Add(1)
		logger.Info("ossync: reconcile removed virtual key %s (deleted on another node)", id)
	}
	if len(missing) > 0 {
		o.stats.ReconcileReloads.Add(int64(len(missing)))
		logger.Info("ossync: reconcile loading %d virtual key(s) missing from memory", len(missing))
		o.reloadLocal(ctx, missing)
	}
}

func (o *OSSync) reloadLocal(ctx context.Context, ids []string) {
	if err := o.s.ReloadVirtualKeys(ctx, ids); err == nil {
		return
	}
	for _, id := range ids {
		if _, err := o.s.ReloadVirtualKey(ctx, id); err != nil {
			logger.Warn("ossync: reload %s: %v", id, err)
		}
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
// Status endpoint

// Status is served at GET /api/ossync/status.
func (o *OSSync) Status() map[string]any {
	last := o.stats.LastAppliedUnix.Load()
	var lastApplied any
	if last > 0 {
		lastApplied = time.Unix(last, 0).UTC().Format(time.RFC3339)
	}
	return map[string]any{
		"node":              o.nodeID,
		"listening":         o.listening.Load(),
		"verified":          o.verified.Load(),
		"published":         o.stats.Published.Load(),
		"publish_errors":    o.stats.PublishErrors.Load(),
		"received":          o.stats.Received.Load(),
		"applied":           o.stats.Applied.Load(),
		"apply_errors":      o.stats.ApplyErrors.Load(),
		"catchup_reloads":   o.stats.CatchupReloads.Load(),
		"reconcile_removes": o.stats.ReconcileRemoves.Load(),
		"reconcile_reloads": o.stats.ReconcileReloads.Load(),
		"listener_drops":    o.stats.ListenerDrops.Load(),
		"last_applied_at":   lastApplied,
	}
}

func (o *OSSync) statusHandler(ctx *fasthttp.RequestCtx) {
	body, _ := json.Marshal(o.Status())
	ctx.SetContentType("application/json")
	ctx.SetBody(body)
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
