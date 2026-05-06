package kv

import (
	"sync"
	"testing"
)

func checkPutPrev(t *testing.T, ds *DataStore, k string, v string, prev string, hasPrev bool) {
	t.Helper()
	prevVal, ok := ds.Put(k, v)
	if hasPrev != ok || prevVal != prev {
		t.Errorf("prevVal=%s, ok=%v; want %s,%v", prevVal, ok, prev, hasPrev)
	}
}

func checkGet(t *testing.T, ds *DataStore, k string, v string, found bool) {
	t.Helper()
	gotV, ok := ds.Get(k)
	if found != ok || v != gotV {
		t.Errorf("gotV=%s, ok=%v; want %s,%v", gotV, ok, v, found)
	}
}

func checkCAS(t *testing.T, ds *DataStore, k string, comp string, v string, prev string, found bool) {
	t.Helper()
	gotPrev, gotFound := ds.CAS(k, comp, v)
	if found != gotFound || prev != gotPrev {
		t.Errorf("gotPrev=%s, gotFound=%v; want %s,%v", gotPrev, gotFound, prev, found)
	}
}

func checkAppend(t *testing.T, ds *DataStore, k string, v string, prev string, found bool) {
	t.Helper()
	gotPrev, gotFound := ds.Append(k, v)
	if found != gotFound || prev != gotPrev {
		t.Errorf("gotPrev=%s, gotFound=%v; want %s,%v", gotPrev, gotFound, prev, found)
	}
}

func TestGetPut(t *testing.T) {
	ds := NewDataStore()

	checkGet(t, ds, "foo", "", false)
	checkPutPrev(t, ds, "foo", "bar", "", false)
	checkGet(t, ds, "foo", "bar", true)
	checkPutPrev(t, ds, "foo", "baz", "bar", true)
	checkGet(t, ds, "foo", "baz", true)
	checkPutPrev(t, ds, "nix", "hard", "", false)
}

func TestAppendBasic(t *testing.T) {
	ds := NewDataStore()

	checkAppend(t, ds, "foo", "bar", "", false)
	checkGet(t, ds, "foo", "bar", true)
	checkAppend(t, ds, "foo", "baz", "bar", true)
	checkGet(t, ds, "foo", "barbaz", true)

	checkAppend(t, ds, "nix", "hard", "", false)
	checkGet(t, ds, "nix", "hard", true)
}

func TestCASBasic(t *testing.T) {
	ds := NewDataStore()
	ds.Put("foo", "bar")
	ds.Put("sun", "beam")

	checkCAS(t, ds, "foo", "mex", "bro", "bar", true)
	checkCAS(t, ds, "foo", "bar", "bro", "bar", true)
	checkGet(t, ds, "foo", "bro", true)

	checkCAS(t, ds, "goa", "mm", "vv", "", false)
	checkGet(t, ds, "goa", "", false)

	ds.Put("goa", "tva")
	checkCAS(t, ds, "goa", "mm", "vv", "tva", true)
	checkCAS(t, ds, "goa", "mm", "vv", "tva", true)
}

func TestCASConcurrent(t *testing.T) {
	ds := NewDataStore()
	ds.Put("foo", "bar")
	ds.Put("sun", "beam")

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		for range 2000 {
			ds.CAS("foo", "bar", "baz")
		}
	}()
	go func() {
		defer wg.Done()
		for range 2000 {
			ds.CAS("foo", "baz", "bar")
		}
	}()

	wg.Wait()

	v, _ := ds.Get("foo")
	if v != "bar" && v != "baz" {
		t.Errorf("got v=%s, want bar or baz", v)
	}
}
