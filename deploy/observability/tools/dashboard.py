"""Generates grafana/dashboards/flagforge.json. Run: python3 deploy/observability/tools/dashboard.py"""
import json
import os

PROM = {"type": "prometheus", "uid": "prometheus"}
TEMPO = {"type": "tempo", "uid": "tempo"}
panels = []
y = 0


def row(title):
    global y
    panels.append({"type": "row", "title": title, "collapsed": False, "gridPos": {"h": 1, "w": 24, "x": 0, "y": y}, "panels": []})
    y += 1


def ts(title, targets, x, w, unit="short", h=8, description="", stack=False):
    panels.append({
        "type": "timeseries", "title": title, "description": description, "datasource": PROM,
        "gridPos": {"h": h, "w": w, "x": x, "y": y},
        "fieldConfig": {"defaults": {"unit": unit, "custom": {"lineWidth": 2, "fillOpacity": 10,
                                                               "stacking": {"mode": "normal" if stack else "none"}}},
                        "overrides": []},
        "options": {"legend": {"displayMode": "list", "placement": "bottom"}, "tooltip": {"mode": "multi"}},
        "targets": [{"datasource": PROM, "expr": e, "legendFormat": l, "refId": chr(65 + i)} for i, (e, l) in enumerate(targets)],
    })


def stat(title, expr, x, w, unit="short", h=8, description=""):
    panels.append({
        "type": "stat", "title": title, "description": description, "datasource": PROM,
        "gridPos": {"h": h, "w": w, "x": x, "y": y},
        "fieldConfig": {"defaults": {"unit": unit, "decimals": 0}, "overrides": []},
        "options": {"reduceOptions": {"calcs": ["lastNotNull"]}, "colorMode": "value", "graphMode": "area"},
        "targets": [{"datasource": PROM, "expr": expr, "refId": "A"}],
    })


def q(p, metric, by=""):
    grouping = f"le{', ' + by if by else ''}"
    return f"histogram_quantile({p}, sum by ({grouping}) (rate({metric}_bucket[$__rate_interval])))"


row("Flag delivery: admin API → outbox → Kafka → flag-delivery → SDKs")
stat("SSE connections", "sum(flagforge_delivery_sse_connections)", 0, 4, description="SDKs connected to all flag-delivery instances")
ts("Propagation latency (commit → applied in flag-delivery)",
   [(q(0.5, "flagforge_delivery_propagation_seconds"), "p50"),
    (q(0.95, "flagforge_delivery_propagation_seconds"), "p95"),
    (q(0.99, "flagforge_delivery_propagation_seconds"), "p99")],
   4, 10, unit="s", description="From the outbox row being written in admin-api to the change being applied and pushed to SSE clients")
ts("Change events consumed", [("sum by (outcome) (rate(flagforge_delivery_events_total[$__rate_interval]))", "{{outcome}}")], 14, 10, unit="ops")
y += 8
ts("Outbox: published / s", [("sum(rate(flagforge_outbox_published_total[$__rate_interval]))", "published")], 0, 8, unit="ops")
ts("Outbox lag (commit → Kafka ack)",
   [(q(0.5, "flagforge_outbox_lag_seconds"), "p50"), (q(0.99, "flagforge_outbox_lag_seconds"), "p99")], 8, 8, unit="s")
ts("Change feed offset", [("max by (service_name) (flagforge_delivery_offset)", "{{service_name}}")], 16, 8)
y += 8

row("Experiments: SDK → event-ingest → Kafka → stats-engine")
ts("Ingested events / s", [("sum by (outcome) (rate(flagforge_ingest_events_total[$__rate_interval]))", "{{outcome}}")], 0, 8, unit="ops", stack=True)
ts("Rejected batches / s", [("sum by (reason) (rate(flagforge_ingest_rejected_total[$__rate_interval]))", "{{reason}}")], 8, 8, unit="ops",
   description="invalid, rate_limited (429), unavailable (Kafka down), malformed")
ts("Stored by stats-engine / s", [("sum by (kind) (rate(flagforge_stats_stored_total[$__rate_interval]))", "{{kind}}")], 16, 8, unit="ops", stack=True)
y += 8

row("HTTP")
ts("Requests / s", [("sum by (service_name) (rate(http_server_request_duration_seconds_count[$__rate_interval]))", "{{service_name}}")], 0, 12, unit="reqps")
ts("Latency p95", [(q(0.95, "http_server_request_duration_seconds", "service_name"), "{{service_name}}")], 12, 12, unit="s")
y += 8

row("Traces")
panels.append({
    "type": "table", "title": "Recent flag changes (click a trace id to follow it from the API call to flag-delivery)",
    "datasource": TEMPO, "gridPos": {"h": 10, "w": 24, "x": 0, "y": y},
    "targets": [{"datasource": TEMPO, "refId": "A", "queryType": "traceql", "query": '{ name = "change-feed apply" }', "limit": 20}],
})

for i, p in enumerate(panels):
    p["id"] = i + 1

dashboard = {
    "uid": "flagforge", "title": "flagforge", "tags": ["flagforge"], "timezone": "browser",
    "schemaVersion": 39, "version": 1, "refresh": "5s", "time": {"from": "now-15m", "to": "now"},
    "panels": panels,
}
out = os.path.join(os.path.dirname(__file__), "..", "grafana", "dashboards", "flagforge.json")
with open(out, "w") as f:
    json.dump(dashboard, f, indent=2)
print(f"wrote {len(panels)} panels to {os.path.normpath(out)}")
