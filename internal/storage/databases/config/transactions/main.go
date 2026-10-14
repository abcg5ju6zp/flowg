package transactions

import (
	"encoding/binary"
	"fmt"

	"link-society.com/flowg/internal/storage/generic/kv"
)

// Version metadata keys. Data items live at "<itemType>/<name>"; the global
// configuration version and the per-item version stamps live under the "meta"
// prefix so backups carry them alongside the data.
var (
	configVersionKey  = kv.Key{"meta", "config-version"}
	itemVersionPrefix = kv.Key{"meta", "item-version"}
)

// itemVersionKey returns the key of the version stamp for one config item.
func itemVersionKey(itemType string, name string) kv.Key {
	return append(append(kv.Key{}, itemVersionPrefix...), itemType, name)
}

// itemTypeVersionPrefix returns the prefix shared by every item version of the
// given type: "meta/item-version/<itemType>".
func itemTypeVersionPrefix(itemType string) kv.Key {
	return append(append(kv.Key{}, itemVersionPrefix...), itemType)
}

// encodeVersion serialises a configuration version as 8-byte big-endian.
func encodeVersion(version uint64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, version)
	return buf
}

// DecodeVersion parses a serialised version; a missing or short value means 0
// (a database that never went through a versioned write).
func DecodeVersion(raw []byte) uint64 {
	if len(raw) < 8 {
		return 0
	}
	return binary.BigEndian.Uint64(raw)
}

// ReadVersion returns the global configuration version seen by txn, 0 when it
// was never initialised.
func ReadVersion(txn kv.QueryTx) (uint64, error) {
	raw, err := txn.Get(configVersionKey)
	if err != nil {
		return 0, fmt.Errorf("failed to read config version: %w", err)
	}
	return DecodeVersion(raw), nil
}

// ReadItemVersion returns the version at which the named item last changed, 0
// when the item has no version stamp (e.g. legacy data).
func ReadItemVersion(txn kv.QueryTx, itemType string, name string) (uint64, error) {
	raw, err := txn.Get(itemVersionKey(itemType, name))
	if err != nil {
		return 0, fmt.Errorf("failed to read %s %q version: %w", itemType, name, err)
	}
	return DecodeVersion(raw), nil
}

// ListItemVersions maps every stamped item name of the given type to its
// version; unstamped (legacy) items are absent from the map.
func ListItemVersions(txn kv.QueryTx, itemType string) (map[string]uint64, error) {
	versions := make(map[string]uint64)

	for pair := range txn.IterPairs(itemTypeVersionPrefix(itemType), kv.KeyRange{}) {
		key := pair.Key()
		versions[key[len(key)-1]] = DecodeVersion(pair.Value())
	}

	return versions, nil
}

// BumpGlobalVersion increments the global configuration version by one and
// returns the new value, without stamping any item. It is used by item deletes
// and by full restores; callers are expected to stamp the affected items with
// [StampItemVersion] in the same transaction when relevant.
func BumpGlobalVersion(txn kv.MutationTx) (uint64, error) {
	version, err := ReadVersion(txn)
	if err != nil {
		return 0, err
	}

	version++

	if err := txn.Set(configVersionKey, encodeVersion(version)); err != nil {
		return 0, fmt.Errorf("failed to write config version: %w", err)
	}

	return version, nil
}

// StampItemVersion records version as the item's last-change version.
func StampItemVersion(txn kv.MutationTx, itemType string, name string, version uint64) error {
	if err := txn.Set(itemVersionKey(itemType, name), encodeVersion(version)); err != nil {
		return fmt.Errorf("failed to stamp %s %q version: %w", itemType, name, err)
	}
	return nil
}

// BumpVersion increments the global configuration version and stamps the named
// item with the resulting value in a single transaction, returning it.
func BumpVersion(txn kv.MutationTx, itemType string, name string) (uint64, error) {
	version, err := BumpGlobalVersion(txn)
	if err != nil {
		return 0, err
	}

	if err := StampItemVersion(txn, itemType, name, version); err != nil {
		return 0, err
	}

	return version, nil
}

// ClearItemVersion removes the version stamp of the named item; it does not
// touch the global version, which [BumpGlobalVersion] already advanced.
func ClearItemVersion(txn kv.MutationTx, itemType string, name string) error {
	if err := txn.Clear(itemVersionKey(itemType, name)); err != nil {
		return fmt.Errorf("failed to clear %s %q version: %w", itemType, name, err)
	}
	return nil
}

// ListItems returns the names of every item of the given type by scanning the
// "<itemType>" prefix; only the keys carry the names, so values are skipped.
func ListItems(txn kv.QueryTx, itemType string) ([]string, error) {
	items := []string{}

	for key := range txn.IterKeys(kv.Key{itemType}, kv.KeyRange{}) {
		itemName := key[len(key)-1]
		items = append(items, itemName)
	}

	return items, nil
}

// ReadItem returns the raw, already-serialized value stored at
// "<itemType>:<name>", wrapping a missing key as a typed not-found error.
func ReadItem(txn kv.QueryTx, itemType string, name string) ([]byte, error) {
	content, err := txn.Get(kv.Key{itemType, name})
	if err != nil {
		return nil, fmt.Errorf("failed to read %s %q: %w", itemType, name, err)
	}

	return content, nil
}

// WriteItem stores content verbatim at "<itemType>:<name>", creating or
// overwriting the item.
func WriteItem(txn kv.MutationTx, itemType string, name string, content []byte) error {
	err := txn.Set(kv.Key{itemType, name}, content)
	if err != nil {
		return fmt.Errorf("failed to write %s %q: %w", itemType, name, err)
	}

	return nil
}

// DeleteItem removes the "<itemType>:<name>" key; deleting a key that does not
// exist is a no-op.
func DeleteItem(txn kv.MutationTx, itemType string, name string) error {
	err := txn.Clear(kv.Key{itemType, name})
	if err != nil {
		return fmt.Errorf("failed to delete %s %q: %w", itemType, name, err)
	}

	return nil
}
