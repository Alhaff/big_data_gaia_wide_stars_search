# Інструкція: запуск і що куди записувати у звіт

Команди виконуються з кореня проєкту в такому порядку. Після кожного кроку вказано, який розділ `report.md` він заповнює.
Повні дані лежать у `/media/data/big_data/gaia` (`raw/`, `manifest.json`, `measurements.json`).

## 0. Перевірити, що все на місці

```bash
ls /media/data/big_data/gaia/raw | wc -l          # 22
(cd /media/data/big_data/gaia/raw && md5sum -c "$OLDPWD/dataset/MD5SUM.txt")   # 22 × OK
podman --version && podman compose version
```

Контрольні суми й список файлів повного датасету є в репозиторії: `dataset/MD5SUM.txt`, `dataset/manifest.json` (файли, MD5, діапазони HEALPix), `dataset/measurements.json` (рядки й розмір без стиснення).

## 1. Частка зірок після фільтра (без Spark, ≈1 хв)

```bash
python3 scripts/filter_fraction.py --data /media/data/big_data/gaia
```

Читає по 20 000 перших рядків з кожного файлу й застосовує фільтр етапу 1.
Наприкінці виводить зважену частку і N.

**У звіт:**
- розділ 2, «Скільки зірок лишиться після фільтра»: частка з новим фільтром замість старих 14,6%;
- розділ 3, «Вихідні числа для оцінок»: нове N, і з нього перерахувати всю таблицю (N/C, shuffle 5N × 128 Б, порівняння 4,5·N²/C, етап 5);
- розділ 7, «Гіпотеза»: абсолютні числа shuffle для варіантів A і B.

## 2. Коміт звіту до коду

Курс вимагає, щоб таблиця з оцінками була закомічена раніше за реалізацію. Тому два окремі коміти:

```bash
git add README.md report.md docs/ dataset/ evidence/README.md requirements.txt Dockerfile \
        docker-compose.yml conf/ .gitignore spark-events/.gitkeep \
        scripts/download_gaia.py scripts/filter_fraction.py
git commit -m "Звіт: дані, етапи й оцінки, гіпотеза експерименту"

git add pipeline/ tests/ scripts/make_sample.py scripts/collect_metrics.py
git commit -m "Конвеєр етапів 1-6 і скрипти замірів"
```

Перед першим комітом переглянути `git status`, щоб не потрапили `output/`, `2101.05282v3.pdf` чи інші зайві файли.

## 3. Зібрати образ (один раз, потрібен інтернет)

```bash
podman compose build
```

Після кожної зміни `Dockerfile` чи `requirements.txt` збирати заново.

## 4. Перевірити пошук пар без Spark (≈10-30 с)

```bash
podman compose run --rm spark python3 tests/check_geometry.py
```

Наприкінці має бути `OK`. Тест порівнює пошук через комірки з перебором усіх пар для обох варіантів експерименту.

**У звіт:** розділ 3 або 8, одне речення про те, як перевірено правильність етапу 2 (скільки пар, скільки підкладених знайдено).

## 5. Вибірка (≈1-2 хв на створення)

```bash
python3 scripts/make_sample.py --raw /media/data/big_data/gaia/raw
ls -lh sample/
```

## 6. History Server

```bash
podman compose up -d history          # http://localhost:18080
```

Працює у фоні, поки не зупинити (`podman compose down`). Журнали всіх запусків лежать у `spark-events/`.

## 7. Конвеєр на вибірці (має вкластися в 1-2 хв)

```bash
time podman compose run --rm --service-ports spark \
    spark-submit pipeline/run_all.py --input /work/sample
```

Поки працює: Spark UI на http://localhost:4040.
Якщо падає, виправляємо тут, а не на повних даних.

**У звіт / README:** реальний час вибірки (вимога курсу «запуск за 1-2 хв»).

## 8. Конвеєр на повному датасеті

```bash
time podman compose run --rm --service-ports spark \
    spark-submit pipeline/run_all.py --input /data/raw
```

Після запуску в `output/`:
- `metrics.json` - час етапів, кількість записів, відкинуті рядки, розподіл ключів;
- `plans/stageN.txt` - плани `explain("formatted")`;
- Parquet кожного етапу.

**У звіт:**

| Розділ | Звідки брати |
|---|---|
| 2, «Скільки зірок лишиться» | `metrics.json` → `stage1.stars_out` |
| 3, колонка «Факт» | History Server → застосунок → вкладка Stages: Input, Shuffle Write, Shuffle Read, кількість задач; записи з `metrics.json` (`pairs_out`, `rows_into_shuffle`, `comparisons`) |
| 4, «Карта фаз» | `output/plans/*.txt` (рядки `Exchange hashpartitioning`) і вкладка SQL у History Server, де видно фінальний план після AQE (чи став join broadcast) |
| 5, «Брудні дані» | `metrics.json` → `stage1`: `total` і кількість відкинутих кожним правилом |
| 6, «Перекіс» | `metrics.json` → `stage2_skew` (keys, max, median, top10_share), `stage3.companions_histogram`; у History Server на вкладці Stages, у Summary Metrics, порівняти Max і Median тривалості задач етапу 2 |
| 9, «Результат» | `output/stage6_stats.csv`, `stage4.system_sizes` |

### Зберегти докази повного запуску

`output/` і `spark-events/` у git не входять, тож копіюємо в `evidence/`.
`app_id` запуску є в `output/metrics.json` (`run.app_id`) і в History Server.

```bash
APP=$(python3 -c "import json; print(json.load(open('output/metrics.json'))['run']['app_id'])")
mkdir -p evidence/spark-events
# Spark 4 пише журнал папкою eventlog_v2_<app_id>, а її власник - користувач spark з контейнера,
# тож звичайний cp не має доступу. podman unshare працює в тому самому просторі користувачів.
podman unshare cp -r spark-events/eventlog_v2_${APP} evidence/spark-events/
podman unshare chown -R 0:0 evidence/spark-events      # власником на хості стане ваш користувач
rm -rf evidence/plans && cp -r output/plans output/metrics.json evidence/
ls -lhR evidence/spark-events/
```

Журнал стиснений (`spark.eventLog.compress true`): для повного запуску 1,2 МБ.

Після перегляду `companions_histogram` підібрати `MAX_COMPANIONS` у `config.py` і перезапустити етапи 3-6:

```bash
podman compose run --rm spark spark-submit pipeline/run_all.py --input /data/raw --stages 3-6
```

## 9. Експеримент (розділ 7)

Спочатку записати в звіт гіпотезу з абсолютними числами (крок 1), потім міряти.
Перед замірами закрити все важке (браузер з багатьма вкладками, IDE-індексацію тощо).

8 запусків етапу 2, чергуючи варіанти: перша пара - прогрів, далі по 3 заміри кожного варіанту.

```bash
for i in 1 2 3 4; do
    podman compose run --rm spark spark-submit pipeline/run_all.py \
        --input /data/raw --stages 2 --no-measure
    podman compose run --rm spark spark-submit pipeline/run_all.py \
        --input /data/raw --stages 2 --no-measure --all-neighbours
done
```

Таблиця з усіма замірами й медіаною (з хоста, History Server має працювати):

```bash
python3 scripts/collect_metrics.py --stages 2 --skip-first
```

Вивід - готова markdown-таблиця для розділу 7, «Результати».
Якщо History Server ще не побачив останні запуски, зачекати ≈10 с і повторити.

Для кожного варіанту також зберегти `output/plans/stage2.txt`: у варіанті B має з'явитися другий `Exchange hashpartitioning` для `dropDuplicates`. Запускати по одному варіанту з `--stages 2` і копіювати план одразу після запуску:

```bash
mkdir -p evidence/experiment
cp output/plans/stage2.txt evidence/experiment/stage2_lower_neighbours.txt   # після запуску A
cp output/plans/stage2.txt evidence/experiment/stage2_all_neighbours.txt     # після запуску B
python3 scripts/collect_metrics.py --stages 2 --skip-first > evidence/experiment/results.md
cp output/runs/*.json evidence/experiment/
```

Журнали самих замірів у здачу можна не класти: таблиці й `runs/*.json` достатньо, а 8 журналів займуть зайве місце.

## 10. Етап 7: валідація за каталогом El-Badry

Завантажити каталог (1,42 ГБ, один раз, MD5 перевіряється):

```bash
python3 scripts/download_elbadry.py --path /media/data/big_data/elbadry
```

Уже завантажено (17.11.2026), MD5 збігся. `manifest.json` каталогу скопійовано в `dataset/elbadry_manifest.json`.

Запустити лише етап 7 (пари етапів 2 і 3 беруться з `output/`):

```bash
podman compose run --rm --service-ports spark \
    spark-submit pipeline/run_all.py --input /data/raw --stages 7
```

Перший запуск спершу перетворює потрібні колонки каталогу в `output/elbadry_catalog.parquet`: розпаковує `.fits.gz` у тимчасовий файл на SSD (кілька ГБ, видаляється після), тож займе кілька хвилин. Наступні запуски беруть готовий Parquet.

**У звіт (розділ про валідацію):**
- `metrics.json` -> `stage7`: скільки їхніх пар у нашій ділянці й межах, повнота етапів 2 і 3, повнота для надійних пар (R < 0,1), чистота етапу 3;
- `output/stage7_validation.csv`: те саме за бінами s;
- `output/stage7_missed_pairs.csv` (якщо є): приклади їхніх пар, яких немає на нашому етапі 2. Повнота етапу 2 має бути ≈100%, кожна пропущена пара - привід шукати помилку.

Після запуску скопіювати в `evidence/` разом з планом `plans/stage7.txt`.

## 11. Брудні дані: приклади рядків (без Spark, кілька секунд)

```bash
python3 scripts/dirty_examples.py                          # з вибірки
python3 scripts/dirty_examples.py > docs/dirty_examples.md  # одразу у файл для звіту
```

Друкує markdown: скільки рядків не проходить кожне правило фільтра і по 3 приклади на правило.
Правила перевіряються незалежно, тож один рядок може потрапити в кілька (наприклад, без паралаксу завжди й без власного руху).
Колонка `astrometric_params_solved` пояснює, чому немає паралаксу: 3 - розв'язок лише з положенням (2 параметри), 31 або 95 - повний (5 або 6 параметрів).

**У звіт:** розділ 5. Кількості брати з повного запуску (`evidence/metrics.json` -> `stage1`), а приклади рядків звідси.
За бажанням приклади з повного файлу: `--input /media/data/big_data/gaia/raw/GaiaSource_093881-097402.csv.gz`.

## 12. Рисунки (≈10 с)

Після етапу 6 (і етапу 7 для другого рисунка):

```bash
podman compose run --rm spark python3 scripts/plot_results.py
mkdir -p docs/figures && cp output/figures/*.png docs/figures/
```

- `separation.png`: ліворуч кількість знайдених пар і випадкових збігів за бінами s (логарифмічна шкала), праворуч частка випадкових збігів у %;
- `validation.png`: повнота й чистота етапу 3 порівняно з El-Badry за бінами s (з'явиться лише після етапу 7).

У контейнері користувач `spark` не може писати в `docs/`, тому рисунки спершу з'являються в `output/figures/`, а копіюємо їх з хоста.

**У звіт:** розділ «Результат», рисунки вставляти як `![Рисунок 1 - ...](docs/figures/separation.png)`.
