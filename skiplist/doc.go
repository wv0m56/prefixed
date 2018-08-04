// Package skiplist implements skiplists for use with the prefixed cache.
// It comes in two flavors, a default one which only allows unique keys
// and a modified one called DupList which allows duplicate keys.
// All flavors are not thread safe and should be protected by RWMutex
// when used concurrently.
package skiplist
