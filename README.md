# BiHA-Demonstration

## Что это за проект

Это **демо-приложение на Streamlit** для мониторинга и тестирования отказоустойчивости PostgreSQL-кластера.
Приложение:
- подключается к нескольким узлам PostgreSQL по DSN;
- показывает текущую роль узла (master/slave) и базовые метрики;
- генерирует pgbench-like read/write-нагрузку по выбранному профилю и количеству клиентских сессий;
- инициализирует и наполняет БД до целевого размера в ГБ на отдельной странице с прогрессом и ETA;
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
- `Client sessions` — количество одновременных клиентских сессий;
- `Read ratio` — доля чтений (0.0..1.0);
- `Expected DB size, GB` — целевой размер базы для pgbench-like сценариев.

---


## Инициализация БД (новая страница)

Страница **"Инициализация и наполнение БД"** выполняет подготовку схемы и генерацию данных по аналогии с `pgbench` (без вызова утилиты `pgbench`):

- создаёт таблицы `pgbench_accounts`, `pgbench_branches`, `pgbench_tellers`, `pgbench_history`;
- очищает их перед новой генерацией;
- заполняет данными до целевого объёма, вычисленного из параметра размера БД в ГБ;
- показывает прогресс выполнения и примерное оставшееся время (ETA).

На странице `SQL команды инициализации и нагрузки` отображаются все SQL-скрипты:
- создания/очистки/наполнения таблиц;
- чтения и чтение-запись для генератора нагрузки.

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

## Логи: куда пишутся и что в них искать

### Файл логов

Все технические логи приложения пишутся в один файл:

- `logs/biha_demo.log` (относительно каталога, из которого запущен `streamlit run app/cluster_demo.py`).

Если каталога `logs/` нет, он создаётся автоматически при старте приложения.

### Ротация логов

Используется ротация файла:

- размер одного файла: до ~2 MB;
- хранится до 5 архивных файлов;
- итоговый набор: `biha_demo.log` + `biha_demo.log.1` ... `biha_demo.log.5`.

Это позволяет не терять историю ошибок при длительном запуске, но и не раздувать лог бесконечно.

### Логи SSH (проверки и failover-команды)

В `logs/biha_demo.log` фиксируются:

- запуск SSH-проверок (страница `SSH Access Check`):
  - узел, хост, таймаут и точная команда;
- завершение SSH-проверок:
  - `returncode`, время выполнения, `stdout/stderr`;
- ошибки SSH:
  - timeout;
  - недоступность ключа;
  - проблемы аутентификации;
  - ошибки `sudo/systemctl` при `start/stop/restart`.

Если в UI failover-кнопки возвращают ошибку, первым делом смотрите последние строки `logs/biha_demo.log` — там будет полный контекст выполнения SSH-команды.

### Логи SQL/БД (ошибки запросов)

В этот же `logs/biha_demo.log` пишутся ошибки SQL-операций и подключения к БД:

- ошибки запроса метрик к узлам (`DB metrics fetch failed ...`), включая traceback;
- ошибки транзакций генератора нагрузки (`Workload transaction failed ...`), с указанием:
  - режима нагрузки;
  - узла;
  - типа транзакции (`read`/`write`);
  - текста ошибки;
  - `sqlstate`, `detail`, `hint` (если PostgreSQL их вернул).

Если ошибка проявилась только в UI (например, рост счётчика ошибок), детали SQL-причины берите из `logs/biha_demo.log`.

### Быстрые команды диагностики логов

```bash
# последние 100 строк
tail -n 100 logs/biha_demo.log

# следить за логом в реальном времени
tail -f logs/biha_demo.log

# только SSH-события
rg "SSH check|SSH action|SSH command timeout" logs/biha_demo.log

# только SQL/БД-ошибки
rg "DB metrics fetch failed|Workload transaction failed|sqlstate=" logs/biha_demo.log
```

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

## Разбор ошибки при логине на узлы кластера

Если вы видите одновременно:

- `Warning: Identity file /home/appuser/.ssh/id_ed25519 not accessible: No such file or directory`
- `sudo: a password is required`

это значит, что есть **две отдельные проблемы**: не найден SSH-ключ и не настроен `sudo` без пароля.

### Шаг 1. Исправьте путь к SSH-ключу

На хосте, где запускается Streamlit:

1. Проверьте пользователя процесса:

```bash
whoami
ps -ef | egrep 'streamlit|cluster_demo' | grep -v grep
```

2. Проверьте, существует ли ключ:

```bash
ls -la /home/appuser/.ssh
ls -la /home/appuser/.ssh/id_ed25519 /home/appuser/.ssh/id_ed25519.pub
```

3. Если ключа нет — создайте его от имени нужного пользователя:

```bash
sudo -u appuser ssh-keygen -t ed25519 -f /home/appuser/.ssh/id_ed25519 -N '' -C 'biha-demo'
```

4. Выставьте корректные права:

```bash
sudo chown -R appuser:appuser /home/appuser/.ssh
sudo chmod 700 /home/appuser/.ssh
sudo chmod 600 /home/appuser/.ssh/id_ed25519
sudo chmod 644 /home/appuser/.ssh/id_ed25519.pub
```

5. Пропишите актуальный путь в `config/cluster.json` (поле `ssh_identity_file`) и убедитесь, что этот путь виден именно пользователю процесса приложения.

### Шаг 2. Добавьте публичный ключ на каждый узел БД

На хосте приложения:

```bash
ssh-copy-id -i /home/appuser/.ssh/id_ed25519.pub postgres@10.10.10.11
ssh-copy-id -i /home/appuser/.ssh/id_ed25519.pub postgres@10.10.10.12
```

Либо вручную добавьте содержимое `id_ed25519.pub` в `~postgres/.ssh/authorized_keys` на целевых узлах.

Права на узле должны быть такими:

```bash
chmod 700 ~/.ssh
chmod 600 ~/.ssh/authorized_keys
```

### Шаг 3. Настройте `sudo` без пароля для управления PostgreSQL

Ошибка `sudo: a password is required` возникает, когда приложение по SSH выполняет `sudo systemctl ...`, а удалённый пользователь не имеет `NOPASSWD`-прав.

На **каждом узле кластера** выполните:

```bash
sudo visudo -f /etc/sudoers.d/postgres-service
```

Добавьте правило (адаптируйте пользователя и имя сервиса):

```sudoers
postgres ALL=(root) NOPASSWD: /bin/systemctl start postgrespro, /bin/systemctl stop postgrespro, /bin/systemctl restart postgrespro, /bin/systemctl status postgrespro
```

> Рекомендуется ограничивать права только нужными командами `systemctl`, а не давать полный `NOPASSWD: ALL`.

Проверьте:

```bash
sudo -l -U postgres
ssh -i /home/appuser/.ssh/id_ed25519 postgres@10.10.10.11 'sudo -n systemctl status postgrespro --no-pager | head -n 5'
```

Ключевой момент: используйте `sudo -n` для безпарольной автоматизации. Если права не настроены, команда сразу вернёт ошибку.

### Шаг 4. Проверьте конфигурацию приложения

Фрагмент узла в `config/cluster.json`:

```json
{
  "name": "node1-master",
  "control_via_ssh": true,
  "ssh_host": "10.10.10.11",
  "ssh_user": "postgres",
  "ssh_port": 22,
  "ssh_identity_file": "/home/appuser/.ssh/id_ed25519",
  "service_name": "postgrespro"
}
```

Если на узле устаревшие алгоритмы SSH, дополнительно:

```json
"ssh_legacy_algorithms": true
```

### Шаг 5. Диагностика (обязательно выполнить)

С хоста приложения:

```bash
ssh -vvv -o BatchMode=yes -o ConnectTimeout=5 -i /home/appuser/.ssh/id_ed25519 postgres@10.10.10.11 whoami
ssh -vvv -o BatchMode=yes -o ConnectTimeout=5 -i /home/appuser/.ssh/id_ed25519 postgres@10.10.10.11 'sudo -n systemctl restart postgrespro'
```

Если первая команда не проходит — проблема в SSH/ключах.
Если первая проходит, а вторая нет — проблема в `sudoers`.

### Краткий чек-лист

- [ ] Файл `/home/appuser/.ssh/id_ed25519` существует.
- [ ] Права на `.ssh` и ключи выставлены корректно.
- [ ] Публичный ключ добавлен на все целевые узлы.
- [ ] В `config/cluster.json` указан правильный `ssh_identity_file`.
- [ ] На узлах настроен `NOPASSWD` для нужных `systemctl`-команд.
- [ ] Ручные проверки `ssh ... whoami` и `ssh ... 'sudo -n systemctl ...'` успешны.


## Важное уточнение

Предыдущая версия README ошибочно описывала Kubernetes/Grafana-стек как обязательный.
Для данного проекта это избыточно: достаточно Python-приложения, доступа к PostgreSQL и (опционально) SSH.
