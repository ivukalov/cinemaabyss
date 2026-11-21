# Задание 5. Circuit breaker в Istio

Проверено на minikube (Kubernetes v1.37, Istio 1.32.0).

## Что установлено

```bash
# Istio (профиль default: istiod + istio-ingressgateway)
# На 8-ГБ Mac под minikube уменьшен запрос памяти у istiod, иначе под
# не планируется: 2Gi запроса не помещаются на узел.
istioctl install --set profile=default \
  --set values.pilot.resources.requests.cpu=100m \
  --set values.pilot.resources.requests.memory=256Mi \
  --set values.pilot.resources.limits.memory=1Gi -y

# Автоматическая инъекция sidecar в namespace приложения
kubectl label namespace cinemaabyss istio-injection=enabled --overwrite
kubectl -n cinemaabyss rollout restart deployment \
  monolith movies-service proxy-service events-service
```

## Circuit breaker

`src/kubernetes/circuit-breaker-config.yaml` — два `DestinationRule`
(`monolith` и `movies-service`): `connectionPool` ограничивает число
одновременных запросов к каждому поду, `outlierDetection` выводит под из
балансировки после ошибок.

```bash
kubectl apply -f src/kubernetes/circuit-breaker-config.yaml
kubectl -n cinemaabyss get destinationrule
```

## Нагрузочный тест Fortio

```bash
kubectl apply -f src/kubernetes/fortio.yaml

POD=$(kubectl -n cinemaabyss get pod -l app=fortio -o jsonpath='{.items[0].metadata.name}')

# 50 одновременных соединений, без ограничения QPS, 500 запросов
kubectl -n cinemaabyss exec $POD -c fortio -- \
  fortio load -c 50 -qps 0 -n 500 http://movies-service:8081/api/movies
kubectl -n cinemaabyss exec $POD -c fortio -- \
  fortio load -c 50 -qps 0 -n 500 http://monolith:8080/api/movies
```

Результат (машина с ограниченными ресурсами, обычная нагрузка не превышает
лимиты connectionPool, поэтому срабатывает защита):

| Сервис         | 200  | 503   |
|----------------|------|-------|
| movies-service | 10   | 490   |
| monolith       | 10   | 490   |

## Метрика `upstream_rq_pending_overflow`

```bash
kubectl -n cinemaabyss exec $POD -c istio-proxy -- \
  pilot-agent request GET stats | grep '<cluster-имя>' | grep upstream_rq_pending
```

Пример вывода:

```
cluster.outbound|8081||movies-service.cinemaabyss.svc.cluster.local;.upstream_rq_pending_overflow: 269
cluster.outbound|8081||movies-service.cinemaabyss.svc.cluster.local;.upstream_rq_pending_total: 11
cluster.outbound|8080||monolith.cinemaabyss.svc.cluster.local;.upstream_rq_pending_overflow: 278
```

Ненулевой `upstream_rq_pending_overflow` — это запросы, которые Envoy
отбросил из-за переполнения очереди к сервису, то есть circuit breaker
сработал.
