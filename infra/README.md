# Local platform environment

`docker-compose.platform.yml` runs PostgreSQL, persistent Redis, the HTTP API, three WebSocket replicas behind nginx, an OpenTelemetry Collector, Jaeger, and Prometheus.

```powershell
docker compose -f docker-compose.platform.yml up --build -d
Invoke-RestMethod http://localhost:8000/readyz
```

- WebSocket gateway: `ws://localhost:8000/ws`
- HTTP API: `http://localhost:3000`
- Jaeger: `http://localhost:16686`
- Prometheus: `http://localhost:9090`

The Compose secret values are local-only. Set production values through the deployment platform. Redis AOF is configured with `everysec`, so the local durability window can include roughly the latest second during an abrupt Redis host failure.

Redis 6 or newer is required because chat recovery uses Streams. The Compose environment uses Redis 7.4.

The browser retains unacknowledged reliable chat events for five minutes by default. If `VITE_WS_PENDING_TTL_MS` is changed, keep it lower than the server's `DEDUP_TTL_MS` so a very old retry cannot outlive its idempotency record. Reconnect replay is bounded by `CHAT_REPLAY_LIMIT`; clients receive `resync-required` if retention or that safety limit is exceeded.

For Kubernetes, build and push `infra/docker/ws.Dockerfile`, replace the image in `infra/k8s/base/deployment.yaml`, create `skyscope-secrets` from a secure source using `infra/k8s/secret.example.yaml` as the key-name reference, then apply with `kubectl apply -k infra/k8s/base`. The manifests expect externally reachable PostgreSQL and Redis services plus an nginx ingress controller.
