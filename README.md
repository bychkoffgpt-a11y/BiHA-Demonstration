# BiHA-Demonstration

## Приложение для мониторинга кластера: подробная инструкция по настройке и запуску

Ниже — практический пошаговый гайд для локального и «боевого» запуска приложения мониторинга Kubernetes-кластера.

---

## 1) Что понадобится заранее

### Обязательные компоненты

1. **Docker** (или другой OCI-рантайм)
2. **kubectl**
3. **Kubernetes-кластер**
   - локально: `kind` или `minikube`
   - удалённо: любой managed/self-hosted кластер
4. **Helm** (если деплой через chart)

### Проверка инструментов

```bash
docker --version
kubectl version --client
helm version
```

Если запускаете локально через `kind`, дополнительно:

```bash
kind version
```

---

## 2) Подготовка кластера

### Вариант A: локальный кластер (kind)

```bash
kind create cluster --name biha-monitoring
kubectl cluster-info
kubectl get nodes
```

### Вариант B: существующий кластер

Проверьте текущий контекст и доступ:

```bash
kubectl config current-context
kubectl auth can-i create namespace
```

---

## 3) Базовые namespaces и доступ

```bash
kubectl create namespace monitoring
kubectl create namespace biha-system
```

> Если namespaces уже существуют, команда вернёт ошибку `AlreadyExists` — это нормально.

---

## 4) Установка стека мониторинга (Prometheus + Grafana)

Если в проекте ещё нет своего chart, самый быстрый путь — `kube-prometheus-stack`.

### 4.1 Добавить репозиторий Helm

```bash
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo update
```

### 4.2 Установить chart

```bash
helm upgrade --install monitoring prometheus-community/kube-prometheus-stack \
  --namespace monitoring \
  --set grafana.adminPassword='admin12345'
```

### 4.3 Дождаться готовности

```bash
kubectl get pods -n monitoring
kubectl rollout status deploy/monitoring-kube-prometheus-stack-operator -n monitoring
```

---

## 5) Разворачивание вашего приложения мониторинга

Ниже — универсальный шаблон. Подставьте ваш образ и манифесты.

### 5.1 Сборка и публикация образа

```bash
docker build -t <registry>/biha-monitoring-app:latest .
docker push <registry>/biha-monitoring-app:latest
```

### 5.2 Пример Deployment + Service

Создайте файл `k8s/app.yaml`:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: biha-monitoring-app
  namespace: biha-system
spec:
  replicas: 1
  selector:
    matchLabels:
      app: biha-monitoring-app
  template:
    metadata:
      labels:
        app: biha-monitoring-app
      annotations:
        prometheus.io/scrape: "true"
        prometheus.io/port: "8080"
        prometheus.io/path: "/metrics"
    spec:
      containers:
        - name: app
          image: <registry>/biha-monitoring-app:latest
          ports:
            - containerPort: 8080
          env:
            - name: LOG_LEVEL
              value: "info"
---
apiVersion: v1
kind: Service
metadata:
  name: biha-monitoring-app
  namespace: biha-system
spec:
  selector:
    app: biha-monitoring-app
  ports:
    - name: http
      port: 8080
      targetPort: 8080
```

Применить:

```bash
kubectl apply -f k8s/app.yaml
kubectl get pods -n biha-system
kubectl get svc -n biha-system
```

---

## 6) Подключение метрик в Prometheus

Если у вас включен `ServiceMonitor` CRD (в `kube-prometheus-stack` он обычно есть), создайте `k8s/servicemonitor.yaml`:

```yaml
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: biha-monitoring-app
  namespace: monitoring
  labels:
    release: monitoring
spec:
  selector:
    matchLabels:
      app: biha-monitoring-app
  namespaceSelector:
    matchNames:
      - biha-system
  endpoints:
    - port: http
      path: /metrics
      interval: 15s
```

Применить и проверить:

```bash
kubectl apply -f k8s/servicemonitor.yaml
kubectl get servicemonitor -n monitoring
```

---

## 7) Доступ к Grafana и Prometheus

### Grafana

```bash
kubectl port-forward -n monitoring svc/monitoring-grafana 3000:80
```

Открыть: `http://localhost:3000`
- login: `admin`
- password: `admin12345` (или ваш, если меняли)

### Prometheus

```bash
kubectl port-forward -n monitoring svc/monitoring-kube-prometheus-stack-prometheus 9090:9090
```

Открыть: `http://localhost:9090`

---

## 8) Проверка, что всё работает

### Базовые проверки

```bash
kubectl get pods -A
kubectl get svc -A
kubectl get ingress -A
```

### Проверка метрик приложения

```bash
kubectl port-forward -n biha-system svc/biha-monitoring-app 8080:8080
curl -s http://localhost:8080/metrics | head
```

Если метрики отдаются — Prometheus сможет их собирать.

---

## 9) Частые проблемы и решения

1. **Prometheus не видит target**
   - Проверьте labels в `Service` и `ServiceMonitor`
   - Убедитесь, что порт в `endpoints.port` совпадает с именем порта в `Service`

2. **Grafana не открывается**
   - Проверьте `port-forward`
   - Убедитесь, что под Grafana в статусе `Running`

3. **Метрики пустые**
   - Проверьте путь `/metrics`
   - Проверьте, что приложение реально экспортирует метрики

4. **Нет прав в кластере**
   - Проверьте RBAC и текущий `kubectl` context

---

## 10) Рекомендуемый порядок запуска в новой среде

1. Поднять/подключить Kubernetes-кластер
2. Установить `kube-prometheus-stack`
3. Задеплоить приложение мониторинга
4. Подключить `ServiceMonitor`
5. Проверить targets в Prometheus
6. Импортировать дашборд в Grafana

---

## 11) Полезные команды для эксплуатации

```bash
# Смотреть события в namespace
kubectl get events -n biha-system --sort-by=.metadata.creationTimestamp

# Перезапустить deployment
kubectl rollout restart deployment/biha-monitoring-app -n biha-system

# Проверить логи
kubectl logs -n biha-system deploy/biha-monitoring-app --tail=200 -f

# Проверить состояние релиза Helm
helm list -n monitoring
```

---

Если нужно, могу в следующем шаге добавить:
- готовые манифесты `k8s/` в репозиторий,
- Helm chart для вашего приложения,
- шаблон Grafana dashboard + набор алертов Alertmanager.
