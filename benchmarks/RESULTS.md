# WebSocket benchmark results

No performance result has been measured yet. This file intentionally separates target thresholds from observed results.

## Reproduction

1. Start the platform: `docker compose -f docker-compose.platform.yml up --build -d`.
2. Wait for `http://localhost:8000/readyz` to return HTTP 200.
3. Run a short smoke load: `k6 run -e CONNECTIONS=100 -e DURATION=2m --summary-export benchmarks/results/smoke-100.json benchmarks/k6/websocket.js`.
4. Run 100, 500, and 1,000-connection tests three times each. Use the durations and workload from `docs/platform-engineering-roadmap.md` for publishable tests.
5. Save `docker stats`, host specifications, the Git SHA, server metrics, and the k6 summary alongside every result. Report `connection_failure_rate` separately from `logical_failure_rate`.

## Results

| Date | Git SHA | Scenario | Replicas | Joined sockets | Chat RTT p50 | Chat RTT p95 | Logical failure rate | Raw result |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | --- |
| Pending | Pending | Baseline | 1 | Not measured | Not measured | Not measured | Not measured | Pending |
| Pending | Pending | Distributed steady state | 3 | Not measured | Not measured | Not measured | Not measured | Pending |
| Pending | Pending | Replica terminated | 3 → 2 | Not measured | Not measured | Not measured | Not measured | Pending |

Do not replace `Not measured` with configured virtual users. Use the server-observed joined gauge and report failed upgrades/joins separately.
