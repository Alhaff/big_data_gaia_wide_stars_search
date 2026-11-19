# Широкі подвійні зірки в Gaia DR3

Фінальний проєкт з курсу «Багатоетапна обробка великого датасету».
Автор: Олександр Приходченко.

Шукаю широкі подвійні зірки: пари, які близькі на небі й мають однаковий паралакс і власний рух.
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
- `dataset/MD5SUM.txt` - перевірити завантажені файли: `cd <папка>/raw && md5sum -c <проєкт>/dataset/MD5SUM.txt`;
- `dataset/manifest.json` - файли, MD5, розміри й діапазони пікселів HEALPix;
- `dataset/measurements.json` - кількість записів і розмір без стиснення для кожного файлу.

Посилання на кожен файл: `https://cdn.gea.esac.esa.int/Gaia/gdr3/gaia_source/<назва файлу>`.

## Структура

```
docker-compose.yml         Spark 4.2.0 (local[*]) + History Server
Dockerfile                 образ курсу apache/spark:4.2.0 + пакети з requirements.txt
requirements.txt           Python-пакети контейнера з фіксованими версіями
conf/                      spark-defaults.conf, log4j2.properties
pipeline/                  конвеєр: config, geometry, metrics, stage1..stage7, run_all
scripts/download_gaia.py   вибір файлів, завантаження, вимірювання розміру
scripts/make_sample.py     вибірка для запуску за 1-2 хв
scripts/filter_fraction.py частка зірок після фільтра (для оцінок, без Spark)
scripts/collect_metrics.py таблиця замірів експерименту з History Server
tests/check_geometry.py    перевірка пошуку пар без Spark
sample/                    вибірка (входить у здачу)
output/                    проміжні Parquet, плани, metrics.json (не в git)
spark-events/              журнали подій для History Server
dataset/                   MD5, список файлів і заміри повного датасету
evidence/                  докази повного запуску: журнал Spark, плани, метрики, експеримент
docs/manual.md             теорія
docs/run_guide.md          покрокова інструкція запуску і що куди писати у звіт
report.md                  звіт
```

## Завантаження даних

Потрібен лише Python 3.9+, сторонні бібліотеки не потрібні.

```bash
# лише показати, які файли буде вибрано (≈10 с)
python3 scripts/download_gaia.py --plan --path /media/data/big_data/gaia

# завантажити й виміряти
python3 scripts/download_gaia.py --path /media/data/big_data/gaia
```

Без `--path` дані лягають у `data/` в корені проєкту.
У папці з'являться:
- `raw/*.csv.gz` - файли Gaia;
- `manifest.json` - вибрані файли, їхні MD5 і діапазони HEALPix-пікселів;
- `measurements.json` - точна кількість записів і розмір без стиснення.

Завантаження 5,16 ГБ у мене тривало близько 2 годин (сервер віддає приблизно 0,6 МБ/с).
Якщо перервати (Ctrl+C), повторний запуск продовжить з місця зупинки, а вже завантажені файли пропустить.
Кожен файл перевіряється за MD5.

Очікуваний результат у кінці:

```
Measured: 11,682,760 rows, 14.88 GB uncompressed
```

## Запуск конвеєра

Один раз зібрати образ (потрібен інтернет, далі все працює офлайн):

```bash
podman compose build
```

Вибірка (створюється з повних даних, у здачу входить готовою):

```bash
python3 scripts/make_sample.py --raw /media/data/big_data/gaia/raw
```

Запуск:

```bash
podman compose up -d history                                   # History Server -> http://localhost:18080
podman compose run --rm --service-ports spark spark-submit pipeline/run_all.py --input /work/sample
podman compose run --rm --service-ports spark spark-submit pipeline/run_all.py --input /data/raw
```

Поки застосунок працює, Spark UI доступний на http://localhost:4040.
`--stages 2-3` запускає лише вибрані етапи, решта береться з Parquet в `output/`.
`--no-measure` вимикає додаткові job-и вимірювання (для чистих замірів часу в експерименті).
Шлях до повних даних на хості задає змінна `GAIA_RAW` (за замовчуванням `/media/data/big_data/gaia/raw`).

## Джерела

- Gaia Collaboration, 2022, Gaia DR3, https://doi.org/10.5270/esa-qa4lep3
- El-Badry, Rix & Heintz (2021), MNRAS 506, 2269, https://arxiv.org/abs/2101.05282
