// Package oxia implements a Kine server.Backend using Apache Oxia as the storage backend.
// Register with KINE_ENDPOINT=oxia://host:port/namespace to select this backend.
package oxia

import (
	"context"
	"encoding/binary"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/k3s-io/kine/pkg/drivers"
	"github.com/k3s-io/kine/pkg/server"
	oxiaclient "github.com/oxia-db/oxia/oxia"
	"github.com/sirupsen/logrus"
)

const (
	revKey           = "\x00kine/rev"
	keyPrefix        = "k/"
	maxKeySentinel   = "k/\xff\xff\xff\xff\xff\xff\xff\xff"
	valueHeaderSize  = 32
	oxiaSystemNS     = "oxia-system" // namespace for cluster-scoped resources and revision counter
)

// clusterScopedResources are etcd key path segments (parts[2]) that denote cluster-scoped resources.
// Keys like /registry/<this>/<name> go to oxia-system; others with 5+ segments use parts[3] as namespace.
var clusterScopedResources = map[string]struct{}{
	"nodes": {}, "namespaces": {}, "clusterroles": {}, "clusterrolebindings": {},
	"apiservices": {}, "managedclusters": {}, // cluster-scoped; namespaced resources use path /registry/<resource>/<namespace>/<name>
}

func init() {
	drivers.Register("oxia", New)
}

// New creates a Kine backend that uses Oxia for storage.
// cfg.DataSourceName is the authority after stripping oxia:// (e.g. "host:port" or "host:port/namespace").
func New(ctx context.Context, wg *sync.WaitGroup, cfg *drivers.Config) (bool, server.Backend, error) {
	backend, err := newBackend(ctx, cfg.DataSourceName)
	if err != nil {
		return false, nil, err
	}
	if wg != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-ctx.Done()
			logrus.Info("Closing Oxia clients...")
			backend.closeAllClients()
		}()
	}
	return true, backend, nil
}

// Backend implements server.Backend using Oxia with optional namespace isolation:
// namespaced K8s keys (/registry/<resource>/<namespace>/<name>) map to Oxia namespace = K8s namespace;
// cluster-scoped keys map to Oxia namespace "oxia-system". This gives storage-level multi-tenancy.
// When a namespace is first seen, the driver ensures it exists in Oxia (warns if not) and creates a client.
type Backend struct {
	hostPort     string
	baseOpts     []oxiaclient.ClientOption
	systemClient oxiaclient.SyncClient   // oxia-system: revision counter and cluster-scoped keys
	adminClient  oxiaclient.AdminClient // optional; used to check namespace existence when first seen
	clientsMu    sync.RWMutex
	clients      map[string]oxiaclient.SyncClient // namespace -> client
	revMu        sync.Mutex
}

func newBackend(ctx context.Context, dataSourceName string) (*Backend, error) {
	hostPort, _ := parseEndpoint(dataSourceName)
	baseOpts := []oxiaclient.ClientOption{
		oxiaclient.WithRequestTimeout(30 * time.Second),
	}
	systemClient, err := oxiaclient.NewSyncClient(hostPort, append(baseOpts, oxiaclient.WithNamespace(oxiaSystemNS))...)
	if err != nil {
		return nil, err
	}
	b := &Backend{
		hostPort:     hostPort,
		baseOpts:     baseOpts,
		systemClient: systemClient,
		clients:      map[string]oxiaclient.SyncClient{oxiaSystemNS: systemClient},
	}
	// Optional admin client: when we first see a namespace we check/warn if it's missing in Oxia.
	if adminClient, err := oxiaclient.NewAdminClient(hostPort, nil, nil); err == nil {
		b.adminClient = adminClient
	} else {
		logrus.Debugf("Oxia admin client not available (namespace check on first use disabled): %v", err)
	}
	return b, nil
}

// getOxiaContext maps a Kine/etcd key to (Oxia namespace, storage key).
// Cluster-scoped: /registry/nodes/node-1 -> ("oxia-system", key unchanged).
// Namespaced: /registry/pods/tenant-a/vllm-01 -> ("tenant-a", "/registry/pods/vllm-01") to isolate tenants.
func getOxiaContext(key string) (oxiaNamespace, storageKey string) {
	parts := strings.Split(key, "/")
	// Must be /registry/<resource>/...
	if len(parts) < 4 || parts[1] != "registry" {
		return oxiaSystemNS, key
	}
	resource := parts[2]
	if _, clusterScoped := clusterScopedResources[resource]; clusterScoped {
		return oxiaSystemNS, key
	}
	// Namespaced: /registry/<resource>/<namespace>/<name...>
	if len(parts) < 5 {
		return oxiaSystemNS, key
	}
	k8sNamespace := parts[3]
	if k8sNamespace == "" {
		return oxiaSystemNS, key
	}
	trimmedKey := "/registry/" + resource + "/" + strings.Join(parts[4:], "/")
	return k8sNamespace, trimmedKey
}

// fullKeyFromStorage reconstructs the full Kine key from Oxia namespace + storage key.
func fullKeyFromStorage(oxiaNamespace, storageKey string) string {
	if oxiaNamespace == oxiaSystemNS {
		return storageKey
	}
	parts := strings.Split(storageKey, "/")
	if len(parts) < 4 || parts[1] != "registry" {
		return storageKey
	}
	resource := parts[2]
	namePart := strings.Join(parts[3:], "/")
	return "/registry/" + resource + "/" + oxiaNamespace + "/" + namePart
}

// ensureNamespaceExistsWhenFirstSeen checks that the Oxia namespace exists (via Admin API) and logs a warning if not.
// Oxia namespaces are normally created via coordinator config; this is best-effort visibility when we first see a namespace.
func (b *Backend) ensureNamespaceExistsWhenFirstSeen(namespace string) {
	if namespace == oxiaSystemNS {
		return
	}
	if b.adminClient == nil {
		return
	}
	res := b.adminClient.ListNamespaces()
	if res.Error != nil {
		logrus.Debugf("Oxia ListNamespaces failed (namespace %q): %v", namespace, res.Error)
		return
	}
	for _, n := range res.Namespaces {
		if n == namespace {
			return
		}
	}
	logrus.Warnf("Oxia namespace %q not found in cluster. Add it to the Oxia coordinator config (see https://oxia-db.github.io/docs/features/namespaces) or the first write may fail.", namespace)
}

func (b *Backend) getClient(namespace string) (oxiaclient.SyncClient, error) {
	b.clientsMu.RLock()
	c, ok := b.clients[namespace]
	b.clientsMu.RUnlock()
	if ok {
		return c, nil
	}
	b.clientsMu.Lock()
	defer b.clientsMu.Unlock()
	if c, ok = b.clients[namespace]; ok {
		return c, nil
	}
	// When we first see a namespace: ensure it exists in Oxia (warn if not), then create client.
	b.ensureNamespaceExistsWhenFirstSeen(namespace)
	opts := append([]oxiaclient.ClientOption{}, b.baseOpts...)
	opts = append(opts, oxiaclient.WithNamespace(namespace))
	client, err := oxiaclient.NewSyncClient(b.hostPort, opts...)
	if err != nil {
		return nil, err
	}
	b.clients[namespace] = client
	return client, nil
}

func (b *Backend) closeAllClients() {
	if b.adminClient != nil {
		_ = b.adminClient.Close()
		b.adminClient = nil
	}
	b.clientsMu.Lock()
	defer b.clientsMu.Unlock()
	for _, c := range b.clients {
		c.Close()
	}
	b.clients = nil
}

func parseEndpoint(endpoint string) (hostPort, namespace string) {
	s := strings.TrimSpace(endpoint)
	if idx := strings.Index(s, "/"); idx >= 0 && idx+1 < len(s) {
		return s[:idx], s[idx+1:]
	}
	if _, _, err := net.SplitHostPort(s); err != nil {
		if strings.Contains(err.Error(), "missing port") {
			s = net.JoinHostPort(s, "6648")
		}
	}
	return s, ""
}

// Start implements server.Backend. No-op for Oxia.
func (b *Backend) Start(ctx context.Context) error {
	return nil
}

func (b *Backend) oxiaStorageKey(storageKey string) string {
	return keyPrefix + storageKey
}

func (b *Backend) nextRevision(ctx context.Context) (int64, error) {
	b.revMu.Lock()
	defer b.revMu.Unlock()
	_, data, ver, err := b.systemClient.Get(ctx, revKey)
	if err != nil && err != oxiaclient.ErrKeyNotFound {
		return 0, err
	}
	var rev int64
	if err == nil && len(data) >= 8 {
		rev = int64(binary.BigEndian.Uint64(data))
	}
	rev++
	revBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(revBytes, uint64(rev))
	if err == oxiaclient.ErrKeyNotFound {
		_, _, err = b.systemClient.Put(ctx, revKey, revBytes, oxiaclient.ExpectedRecordNotExists())
	} else {
		_, _, err = b.systemClient.Put(ctx, revKey, revBytes, oxiaclient.ExpectedVersionId(ver.VersionId))
	}
	if err != nil {
		return 0, err
	}
	return rev, nil
}

func (b *Backend) getRevision(ctx context.Context) (int64, error) {
	_, data, _, err := b.systemClient.Get(ctx, revKey)
	if err == oxiaclient.ErrKeyNotFound {
		return 0, nil
	}
	if err != nil || len(data) < 8 {
		return 0, err
	}
	return int64(binary.BigEndian.Uint64(data)), nil
}

func encodeValue(createRev, modRev, version, leaseID int64, value []byte) []byte {
	buf := make([]byte, valueHeaderSize+len(value))
	binary.BigEndian.PutUint64(buf[0:8], uint64(createRev))
	binary.BigEndian.PutUint64(buf[8:16], uint64(modRev))
	binary.BigEndian.PutUint64(buf[16:24], uint64(version))
	binary.BigEndian.PutUint64(buf[24:32], uint64(leaseID))
	copy(buf[32:], value)
	return buf
}

func decodeValue(data []byte) (createRev, modRev, version, leaseID int64, value []byte) {
	if len(data) < valueHeaderSize {
		return 0, 0, 0, 0, nil
	}
	createRev = int64(binary.BigEndian.Uint64(data[0:8]))
	modRev = int64(binary.BigEndian.Uint64(data[8:16]))
	version = int64(binary.BigEndian.Uint64(data[16:24]))
	leaseID = int64(binary.BigEndian.Uint64(data[24:32]))
	value = data[32:]
	return
}

func (b *Backend) getOne(ctx context.Context, key string) (bool, *server.KeyValue, error) {
	ns, storageKey := getOxiaContext(key)
	client, err := b.getClient(ns)
	if err != nil {
		return false, nil, err
	}
	ok := b.oxiaStorageKey(storageKey)
	_, data, _, err := client.Get(ctx, ok)
	if err == oxiaclient.ErrKeyNotFound {
		return false, nil, nil
	}
	if err != nil {
		return false, nil, err
	}
	createRev, modRev, version, leaseID, value := decodeValue(data)
	return true, &server.KeyValue{
		Key:            key,
		Value:          value,
		CreateRevision: createRev,
		ModRevision:    modRev,
		Version:        version,
		Lease:          leaseID,
	}, nil
}

// Get implements server.Backend.
func (b *Backend) Get(ctx context.Context, key, rangeEnd string, limit, revision int64, keysOnly bool) (int64, *server.KeyValue, error) {
	rev, err := b.getRevision(ctx)
	if err != nil {
		return 0, nil, err
	}
	if rangeEnd == "" {
		ok, kv, err := b.getOne(ctx, key)
		if err != nil || !ok {
			return rev, nil, err
		}
		if keysOnly {
			kv.Value = nil
		}
		return rev, kv, nil
	}
	ns, storageKey := getOxiaContext(key)
	client, err := b.getClient(ns)
	if err != nil {
		return 0, nil, err
	}
	minOxia := b.oxiaStorageKey(storageKey)
	maxOxia := maxKeySentinel
	if rangeEnd != "\x00" && rangeEnd != "" {
		_, endStorage := getOxiaContext(rangeEnd)
		maxOxia = b.oxiaStorageKey(endStorage)
	}
	keys, err := client.List(ctx, minOxia, maxOxia)
	if err != nil {
		return 0, nil, err
	}
	if len(keys) == 0 {
		return rev, nil, nil
	}
	firstStorageKey := strings.TrimPrefix(keys[0], keyPrefix)
	firstKey := fullKeyFromStorage(ns, firstStorageKey)
	ok, kv, err := b.getOne(ctx, firstKey)
	if err != nil || !ok {
		return rev, nil, err
	}
	if keysOnly {
		kv.Value = nil
	}
	return rev, kv, nil
}

// Create implements server.Backend.
func (b *Backend) Create(ctx context.Context, key string, value []byte, lease int64) (int64, error) {
	rev, err := b.nextRevision(ctx)
	if err != nil {
		return 0, err
	}
	ns, storageKey := getOxiaContext(key)
	client, err := b.getClient(ns)
	if err != nil {
		return 0, err
	}
	payload := encodeValue(rev, rev, 1, lease, value)
	_, _, err = client.Put(ctx, b.oxiaStorageKey(storageKey), payload, oxiaclient.ExpectedRecordNotExists())
	if err != nil {
		return 0, err
	}
	return rev, nil
}

// Update implements server.Backend.
func (b *Backend) Update(ctx context.Context, key string, value []byte, revision, lease int64) (int64, *server.KeyValue, bool, error) {
	rev, err := b.nextRevision(ctx)
	if err != nil {
		return 0, nil, false, err
	}
	ns, storageKey := getOxiaContext(key)
	client, err := b.getClient(ns)
	if err != nil {
		return 0, nil, false, err
	}
	ok := b.oxiaStorageKey(storageKey)
	var createRev, version int64 = rev, 1
	_, data, _, err := client.Get(ctx, ok)
	if err == nil {
		createRev, _, version, _, _ = decodeValue(data)
		version++
	}
	payload := encodeValue(createRev, rev, version, lease, value)
	_, _, err = client.Put(ctx, ok, payload)
	if err != nil {
		return 0, nil, false, err
	}
	kv := &server.KeyValue{Key: key, Value: value, CreateRevision: createRev, ModRevision: rev, Version: version, Lease: lease}
	return rev, kv, true, nil
}

// Delete implements server.Backend.
func (b *Backend) Delete(ctx context.Context, key string, revision int64) (int64, *server.KeyValue, bool, error) {
	rev, err := b.nextRevision(ctx)
	if err != nil {
		return 0, nil, false, err
	}
	ns, storageKey := getOxiaContext(key)
	client, err := b.getClient(ns)
	if err != nil {
		return 0, nil, false, err
	}
	ok := b.oxiaStorageKey(storageKey)
	_, data, _, err := client.Get(ctx, ok)
	var prevKV *server.KeyValue
	if err == nil {
		createRev, modRev, version, leaseID, value := decodeValue(data)
		prevKV = &server.KeyValue{Key: key, Value: value, CreateRevision: createRev, ModRevision: modRev, Version: version, Lease: leaseID}
	}
	err = client.Delete(ctx, ok)
	if err != nil && err != oxiaclient.ErrKeyNotFound {
		return 0, nil, false, err
	}
	return rev, prevKV, err == nil, nil
}

// List implements server.Backend. Lists within the Oxia namespace derived from prefix.
func (b *Backend) List(ctx context.Context, prefix, startKey string, limit, revision int64, keysOnly bool) (int64, []*server.KeyValue, error) {
	rev, err := b.getRevision(ctx)
	if err != nil {
		return 0, nil, err
	}
	ns, trimmedPrefix := getOxiaContext(prefix)
	client, err := b.getClient(ns)
	if err != nil {
		return 0, nil, err
	}
	minOxia := b.oxiaStorageKey(trimmedPrefix)
	if startKey != "" {
		_, startStorage := getOxiaContext(startKey)
		minOxia = b.oxiaStorageKey(startStorage)
	}
	keys, err := client.List(ctx, minOxia, maxKeySentinel)
	if err != nil {
		return 0, nil, err
	}
	var kvs []*server.KeyValue
	for i, k := range keys {
		if limit > 0 && int64(i) >= limit {
			break
		}
		storageKey := strings.TrimPrefix(k, keyPrefix)
		fullKey := fullKeyFromStorage(ns, storageKey)
		if prefix != "" && !strings.HasPrefix(fullKey, prefix) {
			continue
		}
		ok, kv, err := b.getOne(ctx, fullKey)
		if err != nil || !ok {
			continue
		}
		if keysOnly {
			kv.Value = nil
		}
		kvs = append(kvs, kv)
	}
	return rev, kvs, nil
}

// Count implements server.Backend.
func (b *Backend) Count(ctx context.Context, prefix, startKey string, revision int64) (int64, int64, error) {
	rev, kvs, err := b.List(ctx, prefix, startKey, 0, revision, true)
	if err != nil {
		return 0, 0, err
	}
	return rev, int64(len(kvs)), nil
}

// Watch implements server.Backend. Uses Oxia notifications in the namespace derived from key.
func (b *Backend) Watch(ctx context.Context, key string, revision int64) server.WatchResult {
	events := make(chan []*server.Event, 16)
	errorc := make(chan error, 1)
	rev, _ := b.getRevision(ctx)
	ns, storageKeyPrefix := getOxiaContext(key)
	client, err := b.getClient(ns)
	if err != nil {
		errorc <- err
		close(events)
		close(errorc)
		return server.WatchResult{Events: events, Errorc: errorc}
	}
	prefixOxia := keyPrefix + storageKeyPrefix
	go func() {
		defer close(events)
		defer close(errorc)
		notifications, err := client.GetNotifications()
		if err != nil {
			errorc <- err
			return
		}
		defer notifications.Close()
		for n := range notifications.Ch() {
			if key != "" && !strings.HasPrefix(n.Key, prefixOxia) {
				continue
			}
			storageKey := strings.TrimPrefix(n.Key, keyPrefix)
			fullKey := fullKeyFromStorage(ns, storageKey)
			switch n.Type {
			case oxiaclient.KeyCreated, oxiaclient.KeyModified:
				_, kv, _ := b.getOne(context.Background(), fullKey)
				if kv != nil {
					rev++
					events <- []*server.Event{{Create: n.Type == oxiaclient.KeyCreated, KV: kv}}
				}
			case oxiaclient.KeyDeleted:
				rev++
				events <- []*server.Event{{Delete: true, KV: &server.KeyValue{Key: fullKey}}}
			}
		}
	}()
	return server.WatchResult{CurrentRevision: rev, CompactRevision: 0, Events: events, Errorc: errorc}
}

// DbSize implements server.Backend. Oxia doesn't expose size; return 0.
func (b *Backend) DbSize(ctx context.Context) (int64, error) {
	return 0, nil
}

// CurrentRevision implements server.Backend.
func (b *Backend) CurrentRevision(ctx context.Context) (int64, error) {
	return b.getRevision(ctx)
}

// Compact implements server.Backend. No-op (we don't keep revision history).
func (b *Backend) Compact(ctx context.Context, revision int64) (int64, error) {
	return 0, nil
}

var _ server.Backend = (*Backend)(nil)
