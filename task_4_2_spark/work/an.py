import os
import sys
import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, desc, sum, when, dense_rank, rank, lower, lit
from pyspark.sql.window import Window

# === Инициализация ===
def init_spark():
    spark = SparkSession.builder \
        .appName("Pagila Analysis") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
        .getOrCreate()
    return spark

# === Извлекаем данные из sql-скрипта ===
def parse_and_create_df(spark, sql_filepath, table_name, schema):
    print(f"Загрузка таблицы '{table_name}'...")
    try:
        with open(sql_filepath, 'r', encoding='utf-8') as f:
            content = f.read()
    except FileNotFoundError:
        print(f"ОШИБКА: Файл не найден по пути '{sql_filepath}'.")
        return None

    pattern = re.compile(rf"COPY public\.{re.escape(table_name)}\s*\(.*?\)\s*FROM stdin;\n(.*?)\n\\.", re.DOTALL)
    match = pattern.search(content)

    if not match:
        print(f"Таблица '{table_name}' не найдена в файле.")
        return None

    raw_data = match.group(1).strip().split('\n')
    #пробелы удаляем на строки делим
    # опред кол-во столбцов
    expected_columns = len(schema)
    
    cleaned_data = []
    # строку делим уже на отдел знач
    for i, line in enumerate(raw_data):
        columns = line.split('\t')
        #кол-во столб== кол-во знач
        if len(columns) == expected_columns:
            cleaned_data.append(tuple(columns))
        else:
            print(f" Пропущена строка #{i + 1}: ожидалось {expected_columns}, найдено {len(columns)}.")

    if not cleaned_data:
        print(f"Нет данных для таблицы '{table_name}'.")
        return None

    return spark.createDataFrame(cleaned_data, schema)


def load_all_payments(spark, sql_filepath):
    """
    Загружает все партиции payment_pYYYY_MM и объединяет в один DataFrame.
    """
    print("Загрузка всех платежей из партиций payment_p* ...")
    # открываем и читаем файл
    try:
        with open(sql_filepath, 'r', encoding='utf-8') as f:
            content = f.read()
    except FileNotFoundError:
        print(f"Файл не найден: {sql_filepath}")
        return None
    # ищем блок 
    pattern = re.compile(
        r"COPY public\.(payment_p\d{4}_\d{2})\s*\(.*?\)\s*FROM stdin;\n(.*?)\n\\.",
        re.DOTALL 
        # включаем переносы строк
    )
    matches = pattern.finditer(content)
    # поиск неперес совпадений= наход все совпадения finditer в сод файла
    
    dfs = []
    total_rows = 0

    for match in matches:
        # извлекаем название таб и блок данных
        table_name = match.group(1)
        data_block = match.group(2).strip()
        # пустые блоки пропускаем
        if not data_block:
            continue

        lines = [line for line in data_block.split('\n') if line.strip()]
        rows = []
        #обрабатываем строки 
        for line in lines:
            cols = line.split('\t')
            # В партициях 6 колон 
            if len(cols) == 6:
                rows.append(tuple(cols))
            else:
                print(f" Пропущена строка в {table_name}: {len(cols)} колонок")

        if rows:
            schema = ["payment_id", "customer_id", "staff_id", "rental_id", "amount", "payment_date"]
            #cоздает отдельный DataFrame для каждой партиции
            df = spark.createDataFrame(rows, schema)
            dfs.append(df)
            total_rows += len(rows)
            print(f"  Загружено {len(rows)} строк из {table_name}")

    if not dfs:
        print("Не найдено ни одной партиции payment_p_")
        return None

    # Объединяем все партиции
    print(f"Объединение {len(dfs)} партиций... Всего строк: {total_rows}")
    payment_df = dfs[0]
    for df in dfs[1:]:
        payment_df = payment_df.union(df)

    print(f"Загружено {payment_df.count()} платежей.")
    return payment_df

def run_analysis(spark):
    sql_filepath = "/home/jovyan/work/data/raw/pagila-data.sql"

    category_df = parse_and_create_df(spark, sql_filepath, "category", ["category_id", "name", "last_update"])
    film_category_df = parse_and_create_df(spark, sql_filepath, "film_category", ["film_id", "category_id", "last_update"])
    actor_df = parse_and_create_df(spark, sql_filepath, "actor", ["actor_id", "first_name", "last_name", "last_update"])
    film_actor_df = parse_and_create_df(spark, sql_filepath, "film_actor", ["actor_id", "film_id", "last_update"])
    inventory_df = parse_and_create_df(spark, sql_filepath, "inventory", ["inventory_id", "film_id", "store_id", "last_update"])
    rental_df = parse_and_create_df(spark, sql_filepath, "rental", ["rental_id", "rental_date", "inventory_id", "customer_id", "return_date", "staff_id", "last_update"])
    film_df = parse_and_create_df(spark, sql_filepath, "film", ["film_id", "title", "description", "release_year", "language_id", "original_language_id", "rental_duration", "rental_rate", "length", "replacement_cost", "rating", "last_update", "special_features", "fulltext"])
    customer_df = parse_and_create_df(spark, sql_filepath, "customer", ["customer_id", "store_id", "first_name", "last_name", "email", "address_id", "activebool", "create_date", "last_update", "active"])
    address_df = parse_and_create_df(spark, sql_filepath, "address", ["address_id", "address", "address2", "district", "city_id", "postal_code", "phone", "last_update"])
    city_df = parse_and_create_df(spark, sql_filepath, "city", ["city_id", "city", "country_id", "last_update"])

    payment_df = load_all_payments(spark, sql_filepath)
    if payment_df is None:
        print("Не удалось загрузить платежи. Анализ остановлен.")
        return
    
    required_dfs = {
        "category": category_df, "film_category": film_category_df, "actor": actor_df,
        "film_actor": film_actor_df, "inventory": inventory_df, "rental": rental_df,
        "payment": payment_df, "film": film_df, "customer": customer_df,
        "address": address_df, "city": city_df
    }
    missing = [name for name, df in required_dfs.items() if df is None]
    if missing:
        print(f"Не удалось загрузить таблицы: {missing}")
        return


    print("\n--- 1. Количество фильмов в каждой категории ---")
    (category_df.join(film_category_df, category_df.category_id == film_category_df.category_id)
     .groupBy("name")
     .agg(count("film_id").alias("film_count"))
     .orderBy(desc("film_count"))
     .show()
    )
    
    print("\n--- 2. Топ-10 актёров по количеству аренд фильмов ---")

    (actor_df
        .join(film_actor_df, actor_df.actor_id == film_actor_df.actor_id)
        .join(inventory_df, film_actor_df.film_id == inventory_df.film_id)
        .join(rental_df, inventory_df.inventory_id == rental_df.inventory_id)
        .groupBy(actor_df.actor_id, actor_df.first_name, actor_df.last_name)
        .agg(count(rental_df.rental_id).alias("rental_count"))
        .orderBy(col("rental_count").desc())
        .limit(10)
        .show()
    )

    print("\n--- 3. Самая прибыльная категория фильмов ---")

    (payment_df
        .join(rental_df, "rental_id")
        .join(inventory_df, "inventory_id")
        .join(film_category_df, "film_id")
        .join(category_df, "category_id")
        .groupBy(category_df.name)
        .agg(sum(col("amount")).alias("total_revenue"))
        .orderBy(col("total_revenue").desc())
        .limit(1)
        .show()
    )


    print("\n--- 4. Фильмы, которых нет в inventory ---")

    missing_films_df = film_df.join(inventory_df, "film_id", "left_anti").select("title")
    
    total_missing = missing_films_df.count()
    print(f"Найдено фильмов без инвентаря: {total_missing}")
    
    missing_films_df.show(total_missing)
    

    print("\n--- 5. Топ-3 актёров в категории 'Children' ---")
    
    children_films = film_category_df.join(category_df, "category_id") \
        .filter(col("name") == "Children") \
        .select(col("film_id"))
    
    actor_film_counts = film_actor_df.join(children_films, "film_id") \
        .join(actor_df, "actor_id") \
        .groupBy("actor_id", "first_name", "last_name") \
        .agg(count("film_id").alias("film_count"))
    
    # Применяем оконную функцию RANK()== WITH ranked_actors
    window_spec = Window.orderBy(desc("film_count"))
    
    ranked_actors_df = actor_film_counts.withColumn(
        "rnk",
        rank().over(window_spec)
    )

    top_3_actors = ranked_actors_df.filter(col("rnk") <= 3) \
        .select("first_name", "last_name", "film_count") \
        .orderBy(desc("film_count"))
    
    top_3_actors.show()

    
    print("\n--- 6. Города с количеством активных и неактивных клиентов ---")
    city_customer_df = city_df.alias("c").join(address_df.alias("a"), col("c.city_id") == col("a.city_id")) \
                              .join(customer_df.alias("cu"), col("a.address_id") == col("cu.address_id")) \
                              .select("c.city", "cu.active")
    
    customer_counts_by_city = city_customer_df.groupBy("city").agg(
        sum(when(col("active") == '1', 1).otherwise(0)).alias("active_customers"),
        
        sum(when(col("active") == '0', 1).otherwise(0)).alias("inactive_customers")
    )
    
    result_df = customer_counts_by_city.orderBy(desc("inactive_customers"))
    
    result_df.show()

    print("\n--- 7. Категория с самой длительной общей продолжительностью аренды в каждой группе ---")
    city_groups_df = city_df.alias("c").join(address_df.alias("a"), col("c.city_id") == col("a.city_id")) \
                              .join(customer_df.alias("cu"), col("a.address_id") == col("cu.address_id")) \
                              .select(
                                  col("c.city"),
                                  col("cu.customer_id")
                              ) \
                              .withColumn(
                                  "city_group",
                                  # CASE WHEN / ILIKE 'a%' / LIKE '%-%'
                                  when(lower(col("city")).startswith("a"), lit("starts_with_a"))
                                  .when(col("city").contains("-"), lit("has_dash"))
                              ) \
                              .filter(col("city_group").isNotNull()) # WHERE city.city ILIKE 'a%' OR city.city LIKE '%-%'
    
    category_rentals_df = city_groups_df.alias("cg") \
        .join(rental_df.alias("r"), col("cg.customer_id") == col("r.customer_id")) \
        .join(inventory_df.alias("i"), col("r.inventory_id") == col("i.inventory_id")) \
        .join(film_df.alias("f"), col("i.film_id") == col("f.film_id")) \
        .join(film_category_df.alias("fc"), col("f.film_id") == col("fc.film_id")) \
        .join(category_df.alias("cat"), col("fc.category_id") == col("cat.category_id")) \
        .groupBy("cg.city_group", "cat.name").agg(
            # SUM(film.rental_duration)
            sum(col("f.rental_duration")).alias("total_duration_days")
        ).withColumnRenamed("name", "category_name") # Переименовываем столбец для чистоты
    
    # --- 3.CTE: ranked_categories ---
    #  PARTITION BY city_group
    window_spec = Window.partitionBy("city_group").orderBy(desc("total_duration_days"))
    
    ranked_categories_df = category_rentals_df.withColumn(
        "rnk",
        rank().over(window_spec) # Применяем RANK() OVER (...)
    )
    
    # ---  SELECT и WHERE rnk = 1 ---
    final_result = ranked_categories_df.filter(col("rnk") == 1) \
                                       .select("city_group", "category_name", "total_duration_days") \
                                       .orderBy("city_group")
    
    final_result.show(truncate=False)

def main():
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

    spark = init_spark()
    print("\nSpark Session created\n")
    spark.sparkContext.setLogLevel("ERROR")
    
    try:

        run_analysis(spark)
    finally:
        spark.stop()
        print("\nSpark Session stopped.")


if __name__ == "__main__":
    main()