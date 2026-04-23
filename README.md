# cestorage

`cestorage` is a cardinality estimator that receives Prometheus remote write streams
and exposes approximate time series cardinality as metrics (TODO: support remote write).

It is useful for tracking how many unique time series are flowing through across all metrics, metric name, or broken down by specific labels.

## How it works

Running:
```
go run ./app/cestorage/... -config=streams.yaml -httpListenAddr=:8490
```

Configuration:

```yaml
streams:
  # Track total cardinality with no grouping.
  - interval: '1h'

  # Track cardinality grouped by metric name.
  - interval: '1h'
    group: ["__name__"]

  # Track cardinality grouped by job label.
  - interval: '1m'
    group: ["job"]

  # Track cardinality grouped by tenant info
  - group: ["vm_account_id", "vm_project_id"]

  # Track cardinality of jobs, with extra labels on the output metrics.
  - group: ["job"]
    labels:
      region: 'eu-central-1'
      env: 'production'
```

Fields:
- `group` (optional): list of label names to split cardinality by; each distinct combination gets its own estimate
- `labels` (optional): extra labels attached to all output metrics for this estimator
- `interval` (optional): how often to rotate (reset) counters; defaults to `5m`

Cardinality generator:

```
go run ./app/cegen/main.go -cardI=100 -cardY=20 -template="foo{instance=\"127.0.0.[cardI]\",job=\"ametric[cardY]\"}"
```


## Metrics

Cardinality estimates are exposed at `/cardinality/metrics` in Prometheus text format.

All metrics include `interval`, `group_by_keys`, and `group_by_values` labels. Extra labels from the `labels` config field are inserted between `interval` and `group_by_keys` (sorted alphabetically).

**Without grouping** (`group_by_keys` is `__global` and `group_by_values` is not set):
```
cardinality_estimate{interval="1h0m0s",group_by_keys="__global__"} 142300
```

**With grouping** — one summary line (total distinct group count) plus one line per distinct label value combination:
```
cardinality_estimate{interval="5m0s",group_by_keys="__group__",group_by_values="instance,job"} 2
cardinality_estimate{interval="5m0s",group_by_keys="instance,job",group_by_values="host1:9090,prometheus"} 312
cardinality_estimate{interval="5m0s",group_by_keys="instance,job",group_by_values="host2:9100,node"} 87
```

**With extra labels:**
```
cardinality_estimate{interval="5m0s",env="production",region="eu-central-1",group_by_keys="job",group_by_values="prometheus"} 312
```

## Dashboard

There are Grafana dashboards available in `dashboards` directory:

<img width="1512" height="862" alt="Screenshot 2026-04-23 at 09 47 38" src="https://github.com/user-attachments/assets/2bd6a930-1eb5-40ef-8006-8196c1c12397" />
