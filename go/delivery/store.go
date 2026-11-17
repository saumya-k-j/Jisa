package delivery

// Durable alert store, SPEC 3.15. MongoDB replaces the in-process queue and
// in-memory dedupe of D-051: an accepted alert is on a majority of replica
// set nodes before the caller sees 202, and delivery state survives process
// death. The hot path is untouched; this sidecar remains POST-ALERT ONLY.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readconcern"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"
)

// Delivery states, SPEC 3.15 state machine.
const (
	statePending   = "pending"
	stateInFlight  = "in_flight"
	stateDelivered = "delivered"
	stateFailed    = "failed"
)

// StoreConfig wires a Store. Zero values fall back to production defaults.
type StoreConfig struct {
	URI         string // from MONGODB_URI, never committed
	Database    string
	Subscribers []string
	TTLSeconds  int32         // expireAfterSeconds on delivered_at
	Lease       time.Duration // in_flight older than this is reclaimable
	MaxAttempts int
	Backoff     *Backoff
	DeadLetter  io.Writer
	Now         func() time.Time
}

// Store owns the alerts and stream_state collections. All writes use
// majority write concern: a positive answer means the data survives the
// loss of any single replica set node.
type Store struct {
	cfg    StoreConfig
	client *mongo.Client
	alerts *mongo.Collection
	tokens *mongo.Collection
	deadMu sync.Mutex
}

// Claimed is one alert handed to exactly one worker by Claim.
type Claimed struct {
	ID         bson.ObjectID `bson:"_id"`
	Key        string        `bson:"idempotency_key"`
	DeliveryID string        `bson:"delivery_id"`
	Body       []byte        `bson:"body"`
	Attempts   int32         `bson:"attempts"`
	Pending    []string      `bson:"pending_subscribers"`
}

// OpenStore connects, applies defaults and ensures the SPEC 3.15 indexes:
// unique idempotency_key, {state,next_attempt_at} for claims, and a TTL on
// delivered_at. delivered_at is only ever set on delivered documents, so the
// TTL expires delivered alerts and nothing else.
func OpenStore(ctx context.Context, cfg StoreConfig) (*Store, error) {
	if cfg.URI == "" {
		return nil, errors.New("store: URI is required (MONGODB_URI)")
	}
	if cfg.Database == "" {
		cfg.Database = "jisa"
	}
	if cfg.TTLSeconds <= 0 {
		cfg.TTLSeconds = 604800 // 7 days
	}
	if cfg.Lease <= 0 {
		cfg.Lease = 30 * time.Second
	}
	if cfg.MaxAttempts < 1 {
		cfg.MaxAttempts = 6
	}
	if cfg.Backoff == nil {
		cfg.Backoff = NewBackoff(500*time.Millisecond, 30*time.Second, time.Now().UnixNano())
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}

	opts := options.Client().
		ApplyURI(cfg.URI).
		SetWriteConcern(writeconcern.Majority()).
		SetReadConcern(readconcern.Majority())
	client, err := mongo.Connect(opts)
	if err != nil {
		return nil, fmt.Errorf("store: connect: %w", err)
	}

	s := &Store{
		cfg:    cfg,
		client: client,
		alerts: client.Database(cfg.Database).Collection("alerts"),
		tokens: client.Database(cfg.Database).Collection("stream_state"),
	}
	if err := s.ensureIndexes(ctx); err != nil {
		_ = client.Disconnect(ctx)
		return nil, err
	}
	return s, nil
}

func (s *Store) ensureIndexes(ctx context.Context) error {
	models := []mongo.IndexModel{
		{
			Keys:    bson.D{{Key: "idempotency_key", Value: 1}},
			Options: options.Index().SetUnique(true),
		},
		{
			Keys: bson.D{{Key: "state", Value: 1}, {Key: "next_attempt_at", Value: 1}},
		},
		{
			Keys:    bson.D{{Key: "delivered_at", Value: 1}},
			Options: options.Index().SetExpireAfterSeconds(s.cfg.TTLSeconds),
		},
	}
	_, err := s.alerts.Indexes().CreateMany(ctx, models)
	if err != nil && strings.Contains(err.Error(), "IndexOptionsConflict") {
		// The TTL period changed between runs; collMod updates it in place.
		err = s.client.Database(s.cfg.Database).RunCommand(ctx, bson.D{
			{Key: "collMod", Value: "alerts"},
			{Key: "index", Value: bson.D{
				{Key: "keyPattern", Value: bson.D{{Key: "delivered_at", Value: 1}}},
				{Key: "expireAfterSeconds", Value: s.cfg.TTLSeconds},
			}},
		}).Err()
	}
	if err != nil {
		return fmt.Errorf("store: ensure indexes: %w", err)
	}
	return nil
}

// Close disconnects the client.
func (s *Store) Close(ctx context.Context) error {
	return s.client.Disconnect(ctx)
}

// Accept makes an alert durable and reports whether its key was already
// accepted. The insert carries majority write concern; when it returns, a
// 202 is safe. A duplicate-key error on idempotency_key is the "already
// accepted" signal (SPEC 3.15), answered with the original delivery id read
// from the stored document -- it is not a failure.
func (s *Store) Accept(ctx context.Context, key string, body []byte) (string, bool, error) {
	now := s.cfg.Now().UTC()
	id := deliveryID(key)
	doc := bson.D{
		{Key: "idempotency_key", Value: key},
		{Key: "delivery_id", Value: id},
		{Key: "body", Value: body},
		{Key: "state", Value: statePending},
		{Key: "attempts", Value: int32(0)},
		{Key: "next_attempt_at", Value: now},
		{Key: "pending_subscribers", Value: s.cfg.Subscribers},
		{Key: "created_at", Value: now},
	}
	_, err := s.alerts.InsertOne(ctx, doc)
	if err == nil {
		return id, false, nil
	}
	if !mongo.IsDuplicateKeyError(err) {
		return "", false, fmt.Errorf("store: accept: %w", err)
	}
	var existing struct {
		DeliveryID string `bson:"delivery_id"`
	}
	if ferr := s.alerts.FindOne(ctx,
		bson.D{{Key: "idempotency_key", Value: key}}).Decode(&existing); ferr != nil {
		return "", false, fmt.Errorf("store: accept: duplicate key but original not readable: %w", ferr)
	}
	return existing.DeliveryID, true, nil
}

// Claim hands the next due alert to exactly one caller, or returns (nil, nil)
// when nothing is due. It is a single findOneAndUpdate -- atomic on one
// document -- which is the whole concurrency story: two workers can race on
// the same filter, but only one of them gets the post-image (SPEC 3.15).
// Claimable work is pending and due, or in_flight whose lease expired
// because its worker died mid-delivery.
func (s *Store) Claim(ctx context.Context, worker string) (*Claimed, error) {
	now := s.cfg.Now().UTC()
	filter := bson.D{{Key: "$or", Value: bson.A{
		bson.D{
			{Key: "state", Value: statePending},
			{Key: "next_attempt_at", Value: bson.D{{Key: "$lte", Value: now}}},
		},
		bson.D{
			{Key: "state", Value: stateInFlight},
			{Key: "claimed_at", Value: bson.D{{Key: "$lte", Value: now.Add(-s.cfg.Lease)}}},
		},
	}}}
	update := bson.D{
		{Key: "$set", Value: bson.D{
			{Key: "state", Value: stateInFlight},
			{Key: "claimed_at", Value: now},
			{Key: "worker", Value: worker},
		}},
		{Key: "$inc", Value: bson.D{{Key: "attempts", Value: int32(1)}}},
	}
	opts := options.FindOneAndUpdate().
		SetSort(bson.D{{Key: "next_attempt_at", Value: 1}}).
		SetReturnDocument(options.After)

	var c Claimed
	err := s.alerts.FindOneAndUpdate(ctx, filter, update, opts).Decode(&c)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("store: claim: %w", err)
	}
	return &c, nil
}

// Delivered marks a claimed alert fully delivered. delivered_at both records
// the moment and arms the TTL index that expires the document.
func (s *Store) Delivered(ctx context.Context, c *Claimed) error {
	now := s.cfg.Now().UTC()
	_, err := s.alerts.UpdateByID(ctx, c.ID, bson.D{{Key: "$set", Value: bson.D{
		{Key: "state", Value: stateDelivered},
		{Key: "delivered_at", Value: now},
		{Key: "pending_subscribers", Value: bson.A{}},
	}}})
	if err != nil {
		return fmt.Errorf("store: delivered: %w", err)
	}
	return nil
}

// Reschedule returns a claimed alert to pending for the subscribers that
// have not accepted it yet, due after the 3.13 full-jitter backoff for this
// attempt. Subscribers already delivered are dropped from
// pending_subscribers so a later claim never re-posts to them.
func (s *Store) Reschedule(ctx context.Context, c *Claimed, remaining []string) error {
	now := s.cfg.Now().UTC()
	delay := s.cfg.Backoff.Delay(int(c.Attempts) - 1)
	_, err := s.alerts.UpdateByID(ctx, c.ID, bson.D{{Key: "$set", Value: bson.D{
		{Key: "state", Value: statePending},
		{Key: "next_attempt_at", Value: now.Add(delay)},
		{Key: "pending_subscribers", Value: remaining},
	}}})
	if err != nil {
		return fmt.Errorf("store: reschedule: %w", err)
	}
	return nil
}

// Failed is terminal: permanent 4xx or attempt budget exhausted. One
// dead-letter JSONL record is appended for the alert (3.13 field shape,
// with the still-undelivered subscribers listed), then the state flips to
// failed. Nothing is dropped silently.
func (s *Store) Failed(ctx context.Context, c *Claimed, cause error) error {
	s.deadLetter(c, cause)
	now := s.cfg.Now().UTC()
	_, err := s.alerts.UpdateByID(ctx, c.ID, bson.D{{Key: "$set", Value: bson.D{
		{Key: "state", Value: stateFailed},
		{Key: "failed_at", Value: now},
	}}})
	if err != nil {
		return fmt.Errorf("store: failed: %w", err)
	}
	return nil
}

func (s *Store) deadLetter(c *Claimed, cause error) {
	if s.cfg.DeadLetter == nil {
		return
	}
	reason := ""
	if cause != nil {
		reason = cause.Error()
	}
	record := map[string]any{
		"idempotency_key": c.Key,
		"subscribers":     c.Pending,
		"attempts":        c.Attempts,
		"reason":          reason,
		"recorded_at":     s.cfg.Now().UTC().Format(time.RFC3339),
	}
	if json.Valid(c.Body) {
		record["body"] = json.RawMessage(c.Body)
	} else {
		record["body_raw"] = string(c.Body)
	}
	line, err := json.Marshal(record)
	if err != nil {
		return
	}
	s.deadMu.Lock()
	defer s.deadMu.Unlock()
	_, _ = s.cfg.DeadLetter.Write(append(line, '\n'))
}

// Watch follows the alerts change stream, calling onInsert for every insert
// event, and persists the resume token to stream_state after each event so
// a restarted watcher continues where it stopped (SPEC 3.15). An expired or
// invalid stored token falls back to a fresh stream: the ticker sweep in the
// worker is the correctness backstop, the stream is the latency path.
func (s *Store) Watch(ctx context.Context, onInsert func()) error {
	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: bson.D{{Key: "operationType", Value: "insert"}}}},
	}

	open := func(resumeAfter bson.Raw) (*mongo.ChangeStream, error) {
		opts := options.ChangeStream()
		if resumeAfter != nil {
			opts = opts.SetResumeAfter(resumeAfter)
		}
		return s.alerts.Watch(ctx, pipeline, opts)
	}

	cs, err := open(s.loadResumeToken(ctx))
	if err != nil {
		// Stored token no longer resolvable (e.g. oplog rolled over):
		// start fresh rather than failing the worker.
		cs, err = open(nil)
	}
	if err != nil {
		return fmt.Errorf("store: watch: %w", err)
	}
	defer cs.Close(context.WithoutCancel(ctx))

	for cs.Next(ctx) {
		onInsert()
		s.saveResumeToken(ctx, cs.ResumeToken())
	}
	if err := cs.Err(); err != nil && ctx.Err() == nil {
		return fmt.Errorf("store: watch: %w", err)
	}
	return ctx.Err()
}

func (s *Store) loadResumeToken(ctx context.Context) bson.Raw {
	var doc struct {
		Token bson.Raw `bson:"token"`
	}
	err := s.tokens.FindOne(ctx, bson.D{{Key: "_id", Value: "alerts_watch"}}).Decode(&doc)
	if err != nil {
		return nil
	}
	return doc.Token
}

func (s *Store) saveResumeToken(ctx context.Context, token bson.Raw) {
	if token == nil {
		return
	}
	_, _ = s.tokens.ReplaceOne(ctx,
		bson.D{{Key: "_id", Value: "alerts_watch"}},
		bson.D{
			{Key: "_id", Value: "alerts_watch"},
			{Key: "token", Value: token},
			{Key: "updated_at", Value: s.cfg.Now().UTC()},
		},
		options.Replace().SetUpsert(true))
}
