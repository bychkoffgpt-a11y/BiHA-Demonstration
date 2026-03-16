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
      "ssh_port": 22,
      "ssh_identity_file": "/home/appuser/.ssh/id_ed25519",
      "ssh_legacy_algorithms": false,
      "ssh_extra_options": ["ServerAliveInterval=15", "ServerAliveCountMax=3"],
      "service_name": "postgrespro"
    },
    {
      "name": "node2-slave",
      "dsn": "host=10.10.10.12 port=5432 dbname=postgres user=postgres password=postgres",
      "role_hint": "slave",
      "control_via_ssh": true,
      "ssh_host": "10.10.10.12",
      "ssh_user": "postgres",
      "ssh_port": 22,
      "ssh_identity_file": "/home/appuser/.ssh/id_ed25519",
      "ssh_legacy_algorithms": false,
      "ssh_extra_options": ["ServerAliveInterval=15", "ServerAliveCountMax=3"],
      "service_name": "postgrespro"
    }
  ]
}
```

Пояснения:
- `role_hint` влияет на распределение нагрузки в выбранном режиме;
- фактическая роль (master/slave) определяется запросом `pg_is_in_recovery()`;
- `ssh_port`, `ssh_identity_file`, `ssh_extra_options` помогают стабилизировать SSH-подключение между разными дистрибутивами;
- `ssh_legacy_algorithms=true` включает совместимость с устаревшими SSH-алгоритмами на старых хостах;
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
   - убедитесь, что пользователь БД имеет доступ;
   - посмотрите файл логов приложения `logs/biha_demo.log` (ошибки подключения и traceback пишутся туда автоматически).

2. **Не работают кнопки failover через SSH**
   - проверьте `control_via_ssh`, `ssh_host`, `ssh_user`;
   - проверьте `ssh_port` и путь `ssh_identity_file` у пользователя, под которым запущен Streamlit;
   - если на ALT Linux используются старые SSH-алгоритмы, временно включите `ssh_legacy_algorithms=true`;
   - проверьте sudo-права на `systemctl` для `service_name`.

---

## Рекомендации по настройке SSH для RED OS 8.0.2 -> ALT Linux 10.2.1

На **каждом ALT Linux узле**:

1. Установите и включите OpenSSH-сервер:

```bash
sudo apt-get update
sudo apt-get install -y openssh-server
sudo systemctl enable --now sshd
```

2. Добавьте публичный ключ пользователя приложения (с RED OS) в `~/.ssh/authorized_keys` целевого SSH-пользователя.

3. Проверьте базовые параметры `/etc/openssh/sshd_config`:
   - `PubkeyAuthentication yes`
   - `PasswordAuthentication no` (рекомендуется для прод)
   - `PermitRootLogin prohibit-password` (или `no`)

4. Если нет возможности обновить SSH-стек на ALT Linux, разрешите совместимость (временно):
   - `HostKeyAlgorithms +ssh-rsa`
   - `PubkeyAcceptedAlgorithms +ssh-rsa`
   - `KexAlgorithms +diffie-hellman-group14-sha1`

5. Перезапустите sshd и проверьте:

```bash
sudo systemctl restart sshd
sudo sshd -T | egrep '^(pubkeyauthentication|passwordauthentication|permitrootlogin|kexalgorithms|hostkeyalgorithms)'
```

На **хосте приложения (RED OS)**:

1. Убедитесь, что ключ доступен пользователю процесса Streamlit:

```bash
ls -l ~/.ssh
ssh -i ~/.ssh/id_ed25519 -p 22 postgres@10.10.10.11 whoami
```

2. Если handshake падает из-за алгоритмов, в конфиге узла включите:
   - `"ssh_legacy_algorithms": true`

3. Для диагностики вручную используйте:

```bash
ssh -vvv -o BatchMode=yes -o ConnectTimeout=5 -i ~/.ssh/id_ed25519 postgres@10.10.10.11 whoami
```

3. **`ModuleNotFoundError: No module named 'psycopg'`**
   - активируйте виртуальное окружение, в котором запускаете Streamlit;
   - установите зависимости заново: `pip install -r requirements.txt`;
   - если ошибка сохраняется, установите драйвер напрямую: `pip install "psycopg[binary]>=3.2"`.

4. **Много ошибок генератора нагрузки**
   - снизьте `TPS`;
   - проверьте лимиты подключений PostgreSQL и стабильность сети.

---

## Важное уточнение

Предыдущая версия README ошибочно описывала Kubernetes/Grafana-стек как обязательный.
Для данного проекта это избыточно: достаточно Python-приложения, доступа к PostgreSQL и (опционально) SSH.
