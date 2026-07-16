package demux

import "testing"

// Conformance target 2: fuzzed envelope and control-plane streams. The
// fuzzer drives the same full-surface driver as the randomized tests,
// with every choice drawn from the input bytes: arbitrary sequences
// must never panic, never violate totality, and never leak a charge,
// record, ticket, or branch — asserted by the driver's quiescence
// check on every run.
func FuzzMachineOps(f *testing.F) {
	// One byte per choice; opcodes are consumed modulo 16. The seeds
	// walk the main lifecycles so the fuzzer starts near the
	// interesting interleavings.
	f.Add([]byte{1, 3, 7, 0, 5, 11, 13})                            // request round-trip with a subscription
	f.Add([]byte{2, 3, 6, 5, 7, 9, 11, 12, 13})                     // list: buffer, respond, adopt, take, close
	f.Add([]byte{1, 3, 4, 7, 5, 6, 14})                             // abandon, absorb, expire
	f.Add([]byte{1, 1, 2, 3, 3, 3, 7, 8, 9, 9, 10, 15})             // adoption and a kill
	f.Add([]byte{0, 0, 5, 5, 5, 6, 6, 6, 2, 3, 6, 6, 7, 13, 13})    // fan-out heavy
	f.Add([]byte{2, 3, 5, 4, 7, 8, 14, 14})                         // list abandon with marks
	f.Add([]byte{7, 7, 7})                                          // stray responses (fatality paths)
	f.Add([]byte{1, 3, 7, 1, 3, 7, 1, 3, 7, 1, 3, 4, 5, 6, 7, 8})   // pending churn
	f.Add([]byte{2, 2, 2, 3, 3, 3, 6, 6, 6, 6, 7, 7, 9, 9, 13, 13}) // several lists interleaved

	f.Fuzz(func(t *testing.T, data []byte) {
		driveMachine(t, &byteChooser{data: data})
	})
}
