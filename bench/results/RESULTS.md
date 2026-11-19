# Benchmark results (50,000 records)

Sizes: {'companies': 5000, 'contacts': 40000, 'deals': 5000}. Machine: {'python': '3.11.18', 'platform': 'Linux-6.18.44-fc-v653-x86_64-with-glibc2.39', 'cpu': 'x86_64'}.
Note: fake HubSpot server on localhost, same process; not real HubSpot latencies.

| Scenario | What | API calls | Wall time (s) | Notes |
|---|---|---:|---:|---|
| `initial` | Initial load, empty CRM | 2,000 | 37.0 | verified 50,000 rows: 0 mismatches, 0 duplicates |
| `echo_noop` | Re-run right after the load (own writes echo back via search) | 252 | 4.99 |  |
| `steady_noop` | Re-run, nothing changed | 3 | 2.8 |  |
| `incremental` | 1% warehouse edits + 200 rep edits in HubSpot | 22 | 3.25 | verified 50,000 rows: 0 mismatches, 0 duplicates |
| `faults` | Initial load with 429s/5xx/lost responses + 25 poison rows | 2,429 | 51.86 | 25 dead letters; 49,975 other rows: 0 mismatches, 0 duplicates; injected {'5xx': 28, '429': 43, 'records_written': 50688, 'applied_then_5xx': 16}; 87 retries, 171 bisections |
| `throttled` | Initial load, fake enforces 100 req/10 s + 5 search/s | 2,000 | 204.48 | verified 50,000 rows: 0 mismatches, 0 duplicates; server 429s: 0 |
| `naive` | Baseline: one request per record and per association | 150,000 | 237.11 | projected 4.17 h at 100 req/10 s |
