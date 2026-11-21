# Задание 5. Circuit breaker в Istio

Проверено на minikube (Kubernetes v1.37, Istio 1.32.0).

## Установка

```bash
# Запрос памяти istiod уменьшен: 2Gi по умолчанию не планируются на узел minikube.
istioctl install --set profile=default \
  --set values.pilot.resources.requests.cpu=100m \
  --set values.pilot.resources.requests.memory=256Mi \
  --set values.pilot.resources.limits.memory=1Gi -y

kubectl label namespace cinemaabyss istio-injection=enabled --overwrite
kubectl -n cinemaabyss rollout restart deployment \
  monolith movies-service proxy-service events-service
```

## Circuit breaker

```bash
kubectl apply -f src/kubernetes/circuit-breaker-config.yaml
```

Два `DestinationRule` (`monolith`, `movies-service`): `connectionPool` ограничивает
одновременные запросы к поду, `outlierDetection` выводит под из балансировки.

## Нагрузка Fortio

```bash
kubectl apply -f src/kubernetes/fortio.yaml

POD=$(kubectl -n cinemaabyss get pod -l app=fortio -o jsonpath='{.items[0].metadata.name}')

kubectl -n cinemaabyss exec $POD -c fortio -- \
  fortio load -c 50 -qps 0 -n 500 http://movies-service:8081/api/movies
kubectl -n cinemaabyss exec $POD -c fortio -- \
  fortio load -c 50 -qps 0 -n 500 http://monolith:8080/api/movies
```

| Сервис | 200 | 503 |
|---|---|---|
| movies-service | 10 | 490 |
| monolith | 10 | 490 |

## Метрика

```bash
kubectl -n cinemaabyss exec $POD -c istio-proxy -- \
  pilot-agent request GET stats | grep '<cluster>' | grep upstream_rq_pending
```

```
cluster.outbound|8081||movies-service.cinemaabyss.svc.cluster.local;.upstream_rq_pending_overflow: 269
cluster.outbound|8081||movies-service.cinemaabyss.svc.cluster.local;.upstream_rq_pending_total: 11
cluster.outbound|8080||monolith.cinemaabyss.svc.cluster.local;.upstream_rq_pending_overflow: 278
```

`upstream_rq_pending_overflow` — запросы, отброшенные Envoy из-за переполнения
очереди к сервису.
