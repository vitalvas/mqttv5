# Changelog

## [0.5.0](https://github.com/vitalvas/mqttv5/compare/v0.4.0...v0.5.0) (2026-03-12)


### Features

* add $SYS topic publisher extension with packet counters and topic metrics ([7c975a1](https://github.com/vitalvas/mqttv5/commit/7c975a140c26f70e64fbe574e6a37ffd75a96c95))
* add device shadow extension with typed client SDK ([13eaf69](https://github.com/vitalvas/mqttv5/commit/13eaf697464a2ff27e72dd1950ea0bc019f9856c))
* add file delivery extension with block-based transfer over MQTT ([26116d8](https://github.com/vitalvas/mqttv5/commit/26116d8b5713065896c0be237c9f77e2d0442cb9))
* add lifecycle extension for client event publishing ([b187012](https://github.com/vitalvas/mqttv5/commit/b18701231462dbf0804edd5df30bc0f928f64494))
* add Prometheus metrics export extension via expvar ([b363abc](https://github.com/vitalvas/mqttv5/commit/b363abce967d7e81776fdffc633d8b2908f170ac))


### Bug Fixes

* publish $SYS topics to all namespaces with subscriber filtering ([f121df2](https://github.com/vitalvas/mqttv5/commit/f121df25ff55200eab6693afbb891996c820cb59))

## [0.4.0](https://github.com/vitalvas/mqttv5/compare/v0.3.1...v0.4.0) (2026-03-08)


### Features

* add client inflight message inspection and fix Server.Clients namespace bug ([e67fc8d](https://github.com/vitalvas/mqttv5/commit/e67fc8d2f38bd34fac628a319e784e725fa0922e))
* add per-topic subscription QoS to router ([3edc862](https://github.com/vitalvas/mqttv5/commit/3edc8621eba85b64ce99ee9f99aeb14fe9196765))


### Bug Fixes

* add integration tests for router Subscribe with real MQTT client ([87f115f](https://github.com/vitalvas/mqttv5/commit/87f115f9d24bbfae652c67f61f5c04ab7af6c2cd))

## [0.3.1](https://github.com/vitalvas/mqttv5/compare/v0.3.0...v0.3.1) (2026-03-08)


### Bug Fixes

* remove client from map before OnDisconnect callback fires ([02eecfa](https://github.com/vitalvas/mqttv5/commit/02eecfad7e16b4a0348ca11a59a4fd56806ccf53))

## [0.3.0](https://github.com/vitalvas/mqttv5/compare/v0.2.0...v0.3.0) (2026-03-07)


### Features

* add per-topic message and subscription metrics ([49c8725](https://github.com/vitalvas/mqttv5/commit/49c87251098a0f35a89496e91b54cd7fb6c0d32e))
* add server introspection for clients and subscriptions ([ac3815e](https://github.com/vitalvas/mqttv5/commit/ac3815e9b3882f0b8daef1cfad4856fa7cc364d6))

## [0.2.0](https://github.com/vitalvas/mqttv5/compare/v0.1.2...v0.2.0) (2026-03-07)


### Features

* add connection and message rate limiting ([ca93ef3](https://github.com/vitalvas/mqttv5/commit/ca93ef3ac0a6b218341833e88f8e8807a8ab4447))

## [0.1.2](https://github.com/vitalvas/mqttv5/compare/v0.1.1...v0.1.2) (2026-03-04)


### Bug Fixes

* migrate websocket from gorilla to kasper ([5fe6422](https://github.com/vitalvas/mqttv5/commit/5fe64229ce2d568883552db065f05aad206a752e))
* rename rpc package to mqttrpc to avoid stdlib name conflict ([7942f90](https://github.com/vitalvas/mqttv5/commit/7942f9069c1931d9f0e04f6090618a7a7eaff593))

## [0.1.1](https://github.com/vitalvas/mqttv5/compare/v0.1.0...v0.1.1) (2026-01-14)


### Bug Fixes

* add license ([5617ab6](https://github.com/vitalvas/mqttv5/commit/5617ab6455237153b40b9e4e9d94712cf49b586a))

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
