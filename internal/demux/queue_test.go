package demux

import "testing"

// TestQueueSustainedChurnBoundsBacking pins the compaction invariant:
// a queue that is never empty must not grow its backing array without
// bound. Before compaction existed, every push under sustained churn
// appended one entry that was never reclaimed, so a long-lived
// subscription draining at exactly its arrival rate leaked entry
// headers forever.
func TestQueueSustainedChurnBoundsBacking(t *testing.T) {
	var q queue[int]
	q.push(0, 1)
	next, want := 1, 0
	for range 100_000 {
		q.push(next, 1)
		next++
		msg, size := q.pop()
		if msg != want || size != 1 {
			t.Fatalf("pop = (%d, %d), want (%d, 1): FIFO order broken", msg, size, want)
		}
		want++
	}
	if got := len(q.entries); got > 128 {
		t.Fatalf("backing array holds %d entries after churn with one live entry, want a small bound", got)
	}
	if q.len() != 1 || q.bytes != 1 {
		t.Fatalf("(len, bytes) = (%d, %d), want (1, 1)", q.len(), q.bytes)
	}
	if msg, _ := q.pop(); msg != want {
		t.Fatalf("final pop = %d, want %d", msg, want)
	}
}

// TestQueueCompactionReleasesStaleReferences asserts the slid-over
// slots are zeroed: a retained duplicate entry would keep its payload
// reachable past its pop.
func TestQueueCompactionReleasesStaleReferences(t *testing.T) {
	var q queue[*int]
	live := new(int)
	for i := range 100 {
		v := new(int)
		*v = i
		q.push(v, 1)
	}
	for range 99 {
		q.pop()
	}
	q.push(live, 1) // depth 2 across many compactions
	for range 200 {
		q.push(live, 1)
		q.pop()
	}
	for i, e := range q.entries {
		if i >= q.head+q.len() && (e.msg != nil || e.size != 0) {
			t.Fatalf("entry %d past the live tail retains %+v", i, e)
		}
	}
}
