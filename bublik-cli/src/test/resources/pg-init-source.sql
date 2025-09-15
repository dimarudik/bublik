-- 1. Создание таблицы
CREATE TABLE IF NOT EXISTS public.test_table (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);

-- 2. Создание функции для заполнения таблицы тестовыми данными
CREATE OR REPLACE FUNCTION public.fill_test_table(num_rows INT)
RETURNS VOID AS $$
DECLARE
    i INT := 1;
BEGIN
    WHILE i <= num_rows LOOP
        INSERT INTO test_table (name)
        VALUES ('Test Name ' || i);
        i := i + 1;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- 3. Вызов функции для вставки, например, 100 строк
SELECT fill_test_table(10000);