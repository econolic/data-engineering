# Homework 4: Optimization & Spark UI

## Частина 1 (part1.py)

Читання даних з csv файлу з використанням `inferSchema` виконується в два проходи:
* Jobs 0 - перший прохід, що сканує дані для визначення типу колонок;
* Job 1 - другий прохід: реальне читання вже з відомою схемою

`collect()` - це одна дія, Spark розбиває виконання на окремі Jobs на кожній межі shuffle:
* Jobs 2 - читання + фільтр `final_priority < 3` + `repartition(2)` (shuffle boundary);
* Jobs 3 - `groupBy("unit_id").count()` (shuffle boundary);
* Jobs 4 - фінальний етап: фільтр `count > 2` + `collect()` повернення результату.

## Частина 2 (part2.py)

При додаванні однієї проміжної дії `nuek_processed.collect()` отримано 3 додаткові Jobs через відсутність кешування. Spark не зберігає проміжні результати між actions і знову будує весь execution plan від початку.

* Jobs 0–1 — inferSchema (як раніше)

Перший `nuek_processed.collect()` (3 Jobs):
* Job 2 — repartition shuffle;
* Job 3 — groupBy shuffle;
* Job 4 — collect результату

Другий `nuek_processed.collect()` (ще 3 Jobs):
* Job 5 — знову repartition shuffle;
* Job 6 — знову groupBy shuffle;
* Job 7 — where("count>2") + collect

## Частина 3 (part3.py)

Після першого `collect()` Spark зберігає результат `nuek_processed_cached` (вже після groupBy) в пам'яті.

Другий `nuek_processed.collect()` виконує лише останню частину плану, оскільки дані вже в кеші:
* Job 5 - читає з кешу (repartition + groupBy не виконуються);
* Job 6 - виконує лише where("count>2") + collect