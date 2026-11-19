# Що робимо

Шукаємо широкі подвійні зірки: пари, які близькі на небі й мають однаковий паралакс і власний рух.
Обробка йде в кілька етапів на Apache Spark (PySpark): фільтрація, пошук пар у комірках неба, відкидання скупчень, об'єднання пар у системи, оцінка випадкових збігів і перевірка за каталогом El-Badry et al. (2021).
Детальний опис, оцінки й результати є в [report.md](report.md).

## Дані

Gaia DR3, таблиця `gaia_source` (ESA): https://cdn.gea.esac.esa.int/Gaia/gdr3/gaia_source/

Беремо ділянку неба навколо зеніту над Києвом опівночі 31 січня 2026 (RA 130,7°, Dec +50,5°, радіус приблизно 24°):

| | |
|---|---|
| Файлів | 22 з 3 386 |
| Розмір `.csv.gz` | 5,16 ГБ |
| Розмір без стиснення | 14,88 ГБ |
| Записів | 11 682 760 |
| Колонок | 152 (використовуємо 12) |

Повний датасет у репозиторій не входить. Список файлів і контрольні суми є в `dataset/`:
- `dataset/MD5SUM.txt` - MD5 усіх 22 файлів;
- `dataset/manifest.json` - файли, MD5, розміри й діапазони пікселів HEALPix;
- `dataset/measurements.json` - кількість записів і розмір без стиснення для кожного файлу;
- `dataset/elbadry_manifest.json` - розмір і MD5 каталогу El-Badry для етапу 7.

Посилання на кожен файл: `https://cdn.gea.esac.esa.int/Gaia/gdr3/gaia_source/<назва файлу>`.

## Що потрібно

Конвеєр працює в контейнері, тож операційна система хоста не важлива: потрібні лише Docker або Podman з Compose.

| | Навіщо |
|---|---|
| Docker з Compose v2 (Docker Desktop на macOS і Windows) або Podman з `podman compose` | контейнер зі Spark |
| GNU make | короткі команди з `Makefile`; без нього ті самі команди можна запускати напряму (див. нижче) |
| Python 3.9 або новіший на хості | завантаження даних, вибірка, допоміжні скрипти; лише стандартна бібліотека |
| Оперативна пам'ять від 8 ГБ | драйверу Spark виділено до 6 ГБ (`conf/spark-defaults.conf`); для вибірки вистачає менше |
| Диск ≈7 ГБ для повних даних і ще кілька ГБ тимчасово | 5,16 ГБ Gaia, 1,42 ГБ каталог El-Badry, проміжні Parquet в `output/`; для вибірки не потрібен |

Усе інше (Spark 4.2.0, Java, Python 3.10 з numpy, pandas, pyarrow, healpy, astropy, matplotlib) є в образі.
Базовий образ `apache/spark:4.2.0`, версії Python-пакетів зафіксовані в `requirements.txt`.

Перевірено на Fedora 44 з Podman 5.8.9 і podman-compose 1.6.0. На інших системах я проєкт не запускав, тож нижче лише те, що відомо про відмінності:

| Система | Що встановити | Примітки |
|---|---|---|
| Fedora, RHEL | `sudo dnf install podman podman-compose make` | |
| Ubuntu, Debian | Docker Engine з плагіном compose або `sudo apt install podman podman-compose`; `sudo apt install make` | у Ubuntu 22.04 Podman 3.4 без підкоманди `podman compose`: тоді `COMPOSE=podman-compose` |
| macOS | Docker Desktop; `make` і `python3` є після `xcode-select --install` | на Apple Silicon образ збирається під arm64, це я не перевіряв; у налаштуваннях Docker Desktop дати VM від 8 ГБ пам'яті для повного запуску |
| Windows | Docker Desktop з WSL2; команди виконувати в терміналі WSL (Ubuntu), там `sudo apt install make python3` | у Git Bash теж працює, якщо встановлено make, а Python викликається як `python`: `make ... PYTHON=python` |

`make` сам вибирає `podman compose`, якщо Podman встановлено, інакше `docker compose`. Примусово: `make ... COMPOSE="docker compose"`.
Файл `.gitattributes` фіксує закінчення рядків LF, тож після клонування у Windows `Makefile` і скрипти не ламаються.

## Встановлення

Усі команди виконуються з кореня проєкту. Список коротких команд і поточні шляхи: `make help`.

### 1. Шляхи до даних (лише для повного запуску)

За замовчуванням дані лежать у проєкті: Gaia в `data/raw/`, каталог El-Badry в `data/elbadry/` (`data/` не входить у git).
Щоб тримати їх деінде, скопіюйте `.env.example` у `.env` і вкажіть свої папки:

```bash
cp .env.example .env
```

```
GAIA_RAW=/media/data/big_data/gaia/raw
ELBADRY=/media/data/big_data/elbadry
```

`.env` читають і `docker compose` / `podman compose`, і `make`, тож шляхи однакові для всіх команд.
У контейнері ці папки змонтовано як `/data/raw` і `/data/elbadry`.

### 2. Зібрати образ

```bash
make build
```

Потрібен інтернет, займає кілька хвилин. Далі все працює без інтернету.
Перед збиранням `make init` створює `output/`, `spark-events/` і папки з даними: Podman не монтує папку, якої немає.
`output/` і `spark-events/` отримують права 777, бо в контейнері код працює від користувача `spark` з іншим uid, і без цього не зможе туди писати.

### 3. Завантажити дані Gaia (лише для повного запуску)

Для запуску на вибірці цей крок не потрібен: вибірка вже є в `sample/`.

```bash
make download       # у папку над GAIA_RAW, файли потрапляють у GAIA_RAW
```

Скрипт сам вибирає 22 файли навколо зеніту, завантажує їх, перевіряє MD5 кожного файлу і записує `manifest.json` і `measurements.json`.
Сервер ESA віддає ≈0,6 МБ/с, тож завантаження 5,16 ГБ у мене тривало близько 2 годин. Якщо перервати (Ctrl+C), повторний запуск продовжить з місця зупинки.
Наприкінці має бути:

```
Measured: 11,682,760 rows, 14.88 GB uncompressed
```

Перевірити вже завантажені файли за контрольними сумами з репозиторію (`dataset/MD5SUM.txt`):

```bash
make verify         # 22 рядки "OK"
```

### 4. Завантажити каталог El-Badry (лише для етапу 7)

```bash
make download-elbadry
```

Файл `all_columns_catalog.fits.gz` (1,42 ГБ) береться із Zenodo (https://zenodo.org/records/4435257), MD5 перевіряється.
Без нього працюють етапи 1-6, не працює лише етап 7.

## Запуск

### Вибірка (≈30-50 с)

```bash
make check          # перевірка пошуку пар без Spark, наприкінці "OK"
make run-sample     # конвеєр на sample/, етапи 1-6
```

Вибірка - круг радіусом 2° у центрі ділянки, 54 243 рядки (23,8 МБ), формат той самий, що й у повних файлів. Очікуваний результат:

```
stage1: stars_out: 10141
stage2: pairs_out: 302
stage3: pairs_out: 302
stage4: system_sizes: {3: 4, 2: 293}
stage5: chance_pairs_out: 10
stage6: таблиця з 6 бінами, у біні 31 623-100 000 а.о. real 3, chance 2.0
```

Поки застосунок працює, Spark UI доступний на http://localhost:4040.
Вибірку можна створити заново з повних даних: `make sample`.

### Повний датасет (≈10 хв)

```bash
make history        # History Server у фоні -> http://localhost:18080
make run-full       # етапи 1-6 на /data/raw
make validate       # етап 7: порівняння з каталогом El-Badry (≈10 с, перший раз ≈40 с)
```

Очікуваний результат (запуск `local-1794941788737`, i5-9300H, 16 ГБ RAM, дані на HDD):

| Етап | Результат | Час |
|---|---|---:|
| 1 | 11 682 760 рядків -> 1 782 969 зірок | ≈2 хв |
| 2 | 51 274 пари | ≈3 хв |
| 3 | 51 155 пар після відкидання скупчень | 4 с |
| 4 | 7 ітерацій, 48 992 пари, 779 трійок, 32 четвірки | 12 с |
| 5 | 2 252 випадкові пари | ≈4 хв |
| 6 | частка випадкових збігів 0,6% (3 162-10 000 а.о.), 9,0% (10 000-31 623 а.о.), 41,4% (31 623-100 000 а.о.) | 1 с |
| 7 | 46 959 пар El-Badry у ділянці, повнота 100%, чистота 91,8% | 10 с |

Перший запуск етапу 7 перетворює потрібні колонки каталогу в `output/elbadry_catalog.parquet`. Для цього `.fits.gz` тимчасово розпаковується в `output/`, тож потрібно кілька ГБ вільного місця.

Лише вибрані етапи (решта береться з Parquet в `output/`):

```bash
make run-stages STAGES=3-6
```

Те саме без `make` (замість `podman compose` може бути `docker compose`; папки з `make init` мають існувати):

```bash
mkdir -p output spark-events data/raw data/elbadry && chmod 777 output spark-events
podman compose build
podman compose up -d history
podman compose run --rm --service-ports spark spark-submit pipeline/run_all.py --input /work/sample
podman compose run --rm --service-ports spark spark-submit pipeline/run_all.py --input /data/raw
podman compose run --rm --service-ports spark spark-submit pipeline/run_all.py --input /data/raw --stages 7
```

Параметри `pipeline/run_all.py`:
- `--input` - папка з `.csv.gz` (`/work/sample` або `/data/raw`);
- `--stages` - етапи, наприклад `1-6` (за замовчуванням), `2-3` або `7`;
- `--no-measure` - без додаткових job-ів вимірювання, для чистих замірів часу;
- `--all-neighbours` - варіант B експерименту: етап 2 копіює зірки в усі 8 сусідніх комірок.

### Що з'являється після запуску

| Де | Що |
|---|---|
| `output/stage*_*.parquet` | результати етапів 1-5 |
| `output/stage6_stats.csv` | пари й випадкові збіги за бінами відстані |
| `output/stage7_validation.csv` | повнота й чистота за бінами відстані |
| `output/metrics.json`, `output/runs/<app_id>.json` | час етапів, кількість записів, відкинуті рядки, розподіл ключів |
| `output/plans/*.txt` | плани виконання `explain("formatted")` кожного етапу |
| `spark-events/` | журнали подій для History Server |

### Експеримент (≈30 хв)

Порівнюються два способи копіювати зірки в сусідні комірки на етапі 2: лише в комірки з меншим номером (A) і в усі 8 сусідів з `dropDuplicates` (B).

```bash
make history
make experiment     # 8 запусків етапу 2: A, B, A, B, ...; перша пара - прогрів
make metrics        # таблиця з усіма замірами й медіаною з History Server
```

Під час замірів інші важкі програми краще закрити. Результат мого запуску: `evidence/experiment/results.md`.

### Допоміжні команди

```bash
make dirty          # приклади відкинутих рядків -> docs/dirty_examples.md (без Spark)
make plots          # рисунки з результатів повного запуску в evidence/ -> output/figures/
make skew           # перекіс на кожному shuffle зі збереженого журналу в evidence/ (без Spark, потрібна утиліта zstd)
make history-down   # зупинити History Server
```

### Збережений повний запуск без повторного запуску

Журнал подій остаточного повного запуску лежить в `evidence/spark-events/`. Щоб переглянути його в History Server без даних Gaia:

```bash
cp -r evidence/spark-events/eventlog_v2_local-1794941788737 spark-events/
make history        # http://localhost:18080, застосунок local-1794941788737
```

### Без інтернету

Інтернет потрібен лише для `make build` і завантаження даних. Після цього вибірка, повний запуск, History Server і всі допоміжні скрипти працюють офлайн.

## Структура

```
Makefile                     короткі команди (make help)
.env.example                 шляхи до даних на хості (скопіювати в .env)
docker-compose.yml           Spark 4.2.0 (local[*]) + History Server
Dockerfile                   apache/spark:4.2.0 + пакети з requirements.txt
requirements.txt             Python-пакети контейнера з фіксованими версіями
conf/                        spark-defaults.conf, log4j2.properties
pipeline/                    конвеєр: config, geometry, metrics, stage1..stage7, run_all
tests/check_geometry.py      перевірка пошуку пар без Spark
scripts/download_gaia.py     вибір файлів Gaia, завантаження, вимірювання розміру
scripts/download_elbadry.py  завантаження каталогу El-Badry
scripts/verify_md5.py        перевірка файлів Gaia за dataset/MD5SUM.txt
scripts/make_sample.py       вибірка для запуску за 1-2 хв
scripts/filter_fraction.py   частка зірок після фільтра для оцінок (без Spark)
scripts/dirty_examples.py    приклади відкинутих рядків (без Spark)
scripts/collect_metrics.py   таблиця замірів експерименту з History Server
scripts/shuffle_skew.py      перекіс shuffle з журналу подій (без Spark)
scripts/check_missed.py      діагностика пар El-Badry, яких немає на етапі 2
scripts/plot_results.py      рисунки з результатів етапів 6 і 7
sample/                      вибірка (входить у здачу)
dataset/                     MD5, список файлів і заміри повного датасету
evidence/                    докази повного запуску: журнал Spark, плани, метрики, експеримент
docs/logs/                   скриншоти запусків
report.md                    звіт
output/figures/              рисунки для звіту
output/, spark-events/       результати й журнали запусків (не в git, крім output/figures/)
data/                        дані за замовчуванням (не в git)
```

## Джерела

- Gaia Collaboration, 2022, Gaia DR3, https://doi.org/10.5270/esa-qa4lep3
- El-Badry, Rix & Heintz (2021), MNRAS 506, 2269, https://arxiv.org/abs/2101.05282; каталог: https://zenodo.org/records/4435257 (CC BY 4.0)
