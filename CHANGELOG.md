# Changelog

## [0.1.0](https://github.com/vitalvas/mqttv5/compare/v0.1.0...v0.1.0) (2026-01-13)


### ⚠ BREAKING CHANGES

* Dial() and DialContext() signatures changed. The addr parameter has been removed. Servers must now be configured via WithServers() or WithServerResolver() options.

### Features

* add broker bridging with P2MP support ([a24b549](https://github.com/vitalvas/mqttv5/commit/a24b549b7b69771e61d667d72b295abbddb0c331))
* add Exists method to MessageStore and RetainedStore interfaces ([2bdf351](https://github.com/vitalvas/mqttv5/commit/2bdf351d723a222bab056c91f2d2672424284371))
* add HTTP CONNECT and SOCKS5 proxy support for client connections ([ed931e0](https://github.com/vitalvas/mqttv5/commit/ed931e01d6565b69592fa889a4e9d2d538bb6b13))
* add mTLS support with certificate identity mapping ([1f5061c](https://github.com/vitalvas/mqttv5/commit/1f5061c57c8fbd883692f657473b707337b4fa27))
* add multi-server support with dynamic service discovery ([a56df55](https://github.com/vitalvas/mqttv5/commit/a56df554d90f587dc5dbba13de40eda3ec9a3c21))
* add router extension with conditional message filtering ([fd7a478](https://github.com/vitalvas/mqttv5/commit/fd7a4782a3bda655a96af0d796707ac91e59806f))
* **ci:** add golang tests ([8e45ce8](https://github.com/vitalvas/mqttv5/commit/8e45ce86737e042b35bbca3cbc7e706357030258))
* **ci:** create release-please ([cd0852d](https://github.com/vitalvas/mqttv5/commit/cd0852ddd855eaaa9844cd41d2b6525bfe46fca8))
* enforce namespace isolation across all store interfaces ([0ae244b](https://github.com/vitalvas/mqttv5/commit/0ae244b9b2f9b4594ad3bf44f29a7fe4835f30d2))
* enhance MQTT v5 strict compliance and WebSocket support ([9885635](https://github.com/vitalvas/mqttv5/commit/9885635c728544c67ee19e30ddb550b819986eac))
* implement data types and encoding ([3a05588](https://github.com/vitalvas/mqttv5/commit/3a055882125f9f1667e8027319446cef8b35ee77))
* implement MQTT v5.0 properties system ([df535bb](https://github.com/vitalvas/mqttv5/commit/df535bb64874697e5be3510e7a34fae6aa7f13b6))
* implement MQTT v5.0 reason codes ([aa746a5](https://github.com/vitalvas/mqttv5/commit/aa746a5303663a66129522998892bfd1a4e84149))
* initialize Go module with dependencies ([360848e](https://github.com/vitalvas/mqttv5/commit/360848ed1f018fb060e058580e6360413cfe0716))


### Bug Fixes

* **ci:** ignore examples from reports ([f5baa79](https://github.com/vitalvas/mqttv5/commit/f5baa79512184eb556eab2117a37ddb56a91b893))
* harden retained handling and reconnect resend; add helper tests ([ed084b5](https://github.com/vitalvas/mqttv5/commit/ed084b5c0387dd90fa362eb262f262039e03f2e6))
* **test:** use Eventually for client count check in CI ([eeacfe4](https://github.com/vitalvas/mqttv5/commit/eeacfe479eb1633d6a11df01523d4f5c00a7e95b))


### Performance Improvements

* optimize memory allocations and add overflow protection ([1f233e6](https://github.com/vitalvas/mqttv5/commit/1f233e6545cd9e5557374daca9eee9d168af49cb))


### Miscellaneous Chores

* release 0.1.0 ([857d0d2](https://github.com/vitalvas/mqttv5/commit/857d0d24f58450a35d7d36992fe97224b3c68f35))
