// Package shadow provides Device Shadow functionality for MQTT v5.
//
// Shadows maintain a virtual representation of a device's state using desired
// and reported states, automatically computing deltas when they differ.
//
// The package provides both server-side (Handler) and client-side (ShadowClient)
// components:
//
//   - Handler processes shadow operations (get/update/delete) from MQTT messages
//     and can be used directly from REST APIs or other external systems.
//   - Client provides fire-and-forget methods for devices to interact with
//     their shadows over MQTT.
//   - TypedClient provides blocking, context-aware methods with automatic
//     request/response correlation, response subscriptions, and typed results.
//
// Classic (unnamed) shadow topics:
//
//	$things/{clientID}/shadow/update              - publish desired/reported state
//	$things/{clientID}/shadow/update/accepted     - update succeeded
//	$things/{clientID}/shadow/update/rejected     - update failed
//	$things/{clientID}/shadow/update/delta        - desired differs from reported
//	$things/{clientID}/shadow/update/documents    - full document after change
//	$things/{clientID}/shadow/get                 - request current shadow
//	$things/{clientID}/shadow/get/accepted        - current shadow document
//	$things/{clientID}/shadow/get/rejected        - shadow not found
//	$things/{clientID}/shadow/delete              - delete shadow
//	$things/{clientID}/shadow/delete/accepted     - delete succeeded
//	$things/{clientID}/shadow/delete/rejected     - shadow not found
//
// Named shadow topics:
//
//	$things/{clientID}/shadow/name/{shadowName}/update
//	$things/{clientID}/shadow/name/{shadowName}/update/accepted
//	$things/{clientID}/shadow/name/{shadowName}/update/rejected
//	$things/{clientID}/shadow/name/{shadowName}/update/delta
//	$things/{clientID}/shadow/name/{shadowName}/update/documents
//	$things/{clientID}/shadow/name/{shadowName}/get
//	$things/{clientID}/shadow/name/{shadowName}/get/accepted
//	$things/{clientID}/shadow/name/{shadowName}/get/rejected
//	$things/{clientID}/shadow/name/{shadowName}/delete
//	$things/{clientID}/shadow/name/{shadowName}/delete/accepted
//	$things/{clientID}/shadow/name/{shadowName}/delete/rejected
//
// Shared classic shadow topics:
//
//	$things/$shared/{groupName}/shadow/update
//	$things/$shared/{groupName}/shadow/update/accepted
//	$things/$shared/{groupName}/shadow/update/rejected
//	$things/$shared/{groupName}/shadow/update/delta
//	$things/$shared/{groupName}/shadow/update/documents
//	$things/$shared/{groupName}/shadow/get
//	$things/$shared/{groupName}/shadow/get/accepted
//	$things/$shared/{groupName}/shadow/get/rejected
//	$things/$shared/{groupName}/shadow/delete
//	$things/$shared/{groupName}/shadow/delete/accepted
//	$things/$shared/{groupName}/shadow/delete/rejected
//
// Shared named shadow topics:
//
//	$things/$shared/{groupName}/shadow/name/{shadowName}/update
//	$things/$shared/{groupName}/shadow/name/{shadowName}/update/accepted
//	$things/$shared/{groupName}/shadow/name/{shadowName}/update/rejected
//	$things/$shared/{groupName}/shadow/name/{shadowName}/update/delta
//	$things/$shared/{groupName}/shadow/name/{shadowName}/update/documents
//	$things/$shared/{groupName}/shadow/name/{shadowName}/get
//	$things/$shared/{groupName}/shadow/name/{shadowName}/get/accepted
//	$things/$shared/{groupName}/shadow/name/{shadowName}/get/rejected
//	$things/$shared/{groupName}/shadow/name/{shadowName}/delete
//	$things/$shared/{groupName}/shadow/name/{shadowName}/delete/accepted
//	$things/$shared/{groupName}/shadow/name/{shadowName}/delete/rejected
//
// List named shadows topics:
//
//	$things/{clientID}/shadow/list                   - request list of named shadows
//	$things/{clientID}/shadow/list/accepted           - list result with shadow names
//	$things/{clientID}/shadow/list/rejected            - list failed
//
// # Update merge semantics
//
// Updates use merge semantics: each key in the update payload is merged into
// the existing shadow document. Keys set to null are deleted from the document.
// Keys not present in the update are left unchanged.
//
// Example: if the current reported state is {"temp": 22, "mode": "auto"} and
// an update sends {"reported": {"temp": 25}}, the resulting reported state is
// {"temp": 25, "mode": "auto"}. Sending {"reported": {"mode": null}} removes
// the "mode" key, resulting in {"temp": 25}.
//
// To clear an entire state section, set it to null in the JSON payload:
// {"state": {"desired": null}} removes all desired state.
//
// # Delta clearing
//
// Delta is automatically recomputed on every update. When a device reports
// values matching the desired state, those keys are removed from delta.
// When all desired keys match reported, delta becomes nil and no delta
// message is published.
//
// # Accepted response
//
// The update/accepted response contains only the fields from the request,
// not the full shadow document. This allows the client to correlate which
// keys were accepted.
//
// # Reconnect delta delivery
//
// Use [Handler.PublishDelta] to push the current pending delta to subscribers
// when a device reconnects. This ensures devices receive any state changes
// that occurred while they were offline.
//
// # Key restrictions
//
// Key names in state objects must not start with the reserved prefix "$".
// Key names must not exceed the configured MaxKeyLength (default: 128).
// The total number of keys across all nesting levels must not exceed
// MaxTotalKeys (default: 500) per state section (desired or reported).
package shadow
