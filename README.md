# BiHA-Demonstration

## Что это за проект

Это **демо-приложение на Streamlit** для мониторинга и тестирования отказоустойчивости PostgreSQL-кластера.
Приложение:
- подключается к нескольким узлам PostgreSQL по DSN;
- показывает текущую роль узла (master/slave) и базовые метрики;
- генерирует read/write-нагрузку по выбранному профилю;
- позволяет запускать failover-сценарии через SSH (`start/stop/restart` сервиса PostgreSQL).

> В этом репозитории **не требуется Kubernetes, Helm, Prometheus и Grafana**.

---

## Быстрый старт

### 1) Требования

- Python 3.10+
- доступ к PostgreSQL-узлам (master/slave)
- (опционально) SSH-доступ до хостов БД для кнопок управления сервисом

### 2) Установка зависимостей

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 3) Подготовка конфига

Скопируйте пример и заполните ваши значения DSN/SSH:

```bash
cp config/cluster.example.json config/cluster.json
```

### 4) Запуск приложения

```bash
streamlit run app/cluster_demo.py
```

Откройте URL, который покажет Streamlit (обычно `http://localhost:8501`).

---

## Формат конфигурации

Файл конфигурации — JSON со списком узлов. Пример (`config/cluster.example.json`):

```json
{
  "poll_interval_sec": 2,
  "nodes": [
    {
      "name": "node1-master",
      "dsn": "host=10.10.10.11 port=5432 dbname=postgres user=postgres password=postgres",
      "role_hint": "master",
      "control_via_ssh": true,
      "ssh_host": "10.10.10.11",
      "ssh_user": "postgres",
      "service_name": "postgrespro"
    },
    {
      "name": "node2-slave",
      "dsn": "host=10.10.10.12 port=5432 dbname=postgres user=postgres password=postgres",
      "role_hint": "slave",
      "control_via_ssh": true,
      "ssh_host": "10.10.10.12",
      "ssh_user": "postgres",
      "service_name": "postgrespro"
    }
  ]
}
```

Пояснения:
- `role_hint` влияет на распределение нагрузки в выбранном режиме;
- фактическая роль (master/slave) определяется запросом `pg_is_in_recovery()`;
- если `control_via_ssh=false`, кнопки stop/start/restart для узла не будут работать.

---

## Профили нагрузки

- `single-node` — весь трафик на master;
- `dual-read` — запись на master, чтение на произвольные узлы;
- `master-rw-slave-r` — запись на master, чтение преимущественно со slave.

Параметры в UI:
- `TPS` — целевая интенсивность транзакций;
- `Read ratio` — доля чтений (0.0..1.0).

---

## Типичный сценарий проверки кластера

1. Запустите приложение.
2. Убедитесь, что узлы видны в таблице `Cluster state`.
3. Запустите нагрузку (`Start load`).
4. Выполните отказ узла (`Stop <node>`), затем восстановление (`Start <node>`).
5. Проверьте, что:
   - кластер остаётся доступным;
   - роли узлов в UI обновляются;
   - счётчики ошибок не растут аномально.

---

## Частые проблемы

1. **`status=down` у узла**
   - проверьте DSN и сетевую доступность;
   - убедитесь, что пользователь БД имеет доступ.

2. **Не работают кнопки failover через SSH**
   - проверьте `control_via_ssh`, `ssh_host`, `ssh_user`;
   - проверьте sudo-права на `systemctl` для `service_name`.

3. **Много ошибок генератора нагрузки**
   - снизьте `TPS`;
   - проверьте лимиты подключений PostgreSQL и стабильность сети.

---

## Важное уточнение

Предыдущая версия README ошибочно описывала Kubernetes/Grafana-стек как обязательный.
Для данного проекта это избыточно: достаточно Python-приложения, доступа к PostgreSQL и (опционально) SSH.
