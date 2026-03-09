package shadow

// buildMetadataForValue creates metadata for a value.
// For maps, it recursively builds metadata for each key.
// For scalars and arrays, it creates a leaf with the timestamp.
func buildMetadataForValue(v any, ts int64) any {
	if m, ok := v.(map[string]any); ok {
		return buildMetadata(m, ts)
	}

	return map[string]any{"timestamp": ts}
}

// filterMetadataForDelta extracts metadata entries for keys present in the delta.
// The delta comes from desired-reported diff, so metadata is taken from desired metadata.
func filterMetadataForDelta(delta, desiredMetadata map[string]any) map[string]any {
	if len(delta) == 0 || len(desiredMetadata) == 0 {
		return nil
	}

	result := make(map[string]any, len(delta))

	for k, dv := range delta {
		md, ok := desiredMetadata[k]
		if !ok {
			continue
		}

		if subDelta, isDeltaMap := dv.(map[string]any); isDeltaMap {
			if subMeta, isMetaMap := md.(map[string]any); isMetaMap {
				filtered := filterMetadataForDelta(subDelta, subMeta)
				if filtered != nil {
					result[k] = filtered
				}

				continue
			}
		}

		result[k] = md
	}

	if len(result) == 0 {
		return nil
	}

	return result
}

// buildMetadata creates metadata for a state map where each leaf
// gets a timestamp entry. Nested maps are traversed recursively.
func buildMetadata(state map[string]any, ts int64) map[string]any {
	if len(state) == 0 {
		return nil
	}

	meta := make(map[string]any, len(state))
	for k, v := range state {
		if v == nil {
			continue
		}

		meta[k] = buildMetadataForValue(v, ts)
	}

	if len(meta) == 0 {
		return nil
	}

	return meta
}
