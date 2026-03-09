package shadow

import "reflect"

// computeDelta returns a map containing keys from desired whose values differ
// from the corresponding values in reported. For nested maps, the diff is
// computed recursively so that only differing leaf keys appear in the result
// (with their full path preserved). Arrays and scalar values are compared
// atomically. Keys present in desired but absent in reported are included.
// Keys present only in reported are ignored.
// Returns nil if there are no differences.
func computeDelta(desired, reported map[string]any) map[string]any {
	if len(desired) == 0 {
		return nil
	}

	var delta map[string]any

	for k, dv := range desired {
		rv, ok := reported[k]
		if !ok {
			if delta == nil {
				delta = make(map[string]any)
			}

			delta[k] = dv

			continue
		}

		// Both are maps: recurse
		dm, dvIsMap := dv.(map[string]any)
		rm, rvIsMap := rv.(map[string]any)

		if dvIsMap && rvIsMap {
			sub := computeDelta(dm, rm)
			if sub != nil {
				if delta == nil {
					delta = make(map[string]any)
				}

				delta[k] = sub
			}

			continue
		}

		// Scalar or array: atomic comparison
		if !reflect.DeepEqual(dv, rv) {
			if delta == nil {
				delta = make(map[string]any)
			}

			delta[k] = dv
		}
	}

	return delta
}
