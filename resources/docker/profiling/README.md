Profiling
=========

This docker compose sets a Bento instance up with a custom config, [Prometheus][prometheus], [Grafana][grafana] and [Jaeger][jaeger] in order to observe its performance under different configurations.

# Set up

- Run Grafana and Prometheus with `docker-compose up`.
- Edit `config.yaml` and add whatever components you want to profile with.
- Run Bento with `bento -c ./config.yaml`.
- Open up Grafana at [http://localhost:3000/d/PHrVlmniz/bento-dash](http://localhost:3000/d/PHrVlmniz/bento-dash)
- Go to [http://localhost:16686](http://localhost:16686) in order to observe opentracing events with Jaeger.
- Use `go tool pprof http://localhost:4195/debug/pprof/profile` and similar endpoints to get profiling data.

[prometheus]: https://prometheus.io/
[grafana]: https://grafana.com/
[jaeger]: https://www.jaegertracing.io/
