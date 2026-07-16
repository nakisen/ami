package demux

// queue is a bounded FIFO of routed messages with byte accounting. The
// machine checks capacity before every push (reserve-or-terminate);
// the queue itself is mechanical.
type queue[T any] struct {
	entries []qentry[T]
	head    int
	bytes   int
}

type qentry[T any] struct {
	msg  T
	size int
}

func (q *queue[T]) len() int {
	return len(q.entries) - q.head
}

func (q *queue[T]) push(msg T, size int) {
	q.entries = append(q.entries, qentry[T]{msg: msg, size: size})
	q.bytes += size
}

// pop removes the oldest entry and returns it with its charge. It must
// not be called on an empty queue.
func (q *queue[T]) pop() (T, int) {
	if q.len() == 0 {
		violated("pop from an empty queue")
	}
	e := q.entries[q.head]
	q.entries[q.head] = qentry[T]{} // release the payload reference
	q.head++
	q.bytes -= e.size
	if q.head == len(q.entries) {
		q.entries = q.entries[:0]
		q.head = 0
	}
	return e.msg, e.size
}

// reset discards every queued entry, drops the backing array, and
// returns the released byte charge.
func (q *queue[T]) reset() int {
	released := q.bytes
	q.entries = nil
	q.head = 0
	q.bytes = 0
	return released
}
