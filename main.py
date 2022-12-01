import psycopg2


def drop_table(cur):
    cur.execute("""
        DROP TABLE Phone;
        DROP TABLE Personal_data;
    """)

    return 'Таблицы очищены'


def create_db(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS Personal_data (
        id SERIAL PRIMARY KEY,
        user_name VARCHAR(40) NOT NULL,
        user_surname VARCHAR(40) NOT NULL,
        user_email VARCHAR(40) NOT NULL UNIQUE	
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS Phone (
        phone_id SERIAL PRIMARY KEY,
        user_id INTEGER NOT NULL REFERENCES Personal_data(id),
        phone_number INTEGER
        );
    """)
    return 'Структура БД создана'


def del_phone(cur, user_id, phone_number):
    if phone_number:
        cur.execute("""
        DELETE FROM Phone
        WHERE user_id = %s AND phone_number = %s
        """, (user_id, phone_number))
    else:
        cur.execute("""
        DELETE FROM Phone
        WHERE user_id = %s
        """, (user_id,))

    cur.execute("""
    SELECT * FROM Phone
    """)
    return cur.fetchall()


def add_phone(cur, user_id, phone):
    cur.execute("""
    INSERT INTO Phone(user_id, phone_number)
    VALUES (%s, %s) RETURNING phone_number;
    """, (user_id, phone))
    return cur.fetchone()[0]


def add_client(cur, name, surname, email, phone=None):
    cur.execute("""
    INSERT INTO Personal_data(user_name, user_surname, user_email)
    VALUES (%s, %s, %s) RETURNING id, user_surname;
    """, (name, surname, email))
    result = list(cur.fetchone())
    if phone:
        phone_result = add_phone(cur, result[0], phone)
        return f"id={result[0]} фамилия '{result[1]}' добавлен номер телефона {phone_result}"
    return f"id={result[0]} фамилия '{result[1]}'"


def search_client(cur, name=None, surname=None, email=None, phone=None):
    if not name:
        name = '%'
    if not surname:
        surname = '%'
    if not email:
        email = '%'
    if not phone:
        cur.execute("""
        SELECT id, user_name, user_surname, user_email, phone_number FROM Personal_data
        LEFT JOIN Phone ON Personal_data.id = Phone.user_id
        WHERE user_name LIKE %s AND user_surname LIKE %s AND
        user_email LIKE %s;
        """, (name, surname, email))
    else:
        cur.execute("""
        SELECT id, user_name, user_surname, user_email, phone_number FROM Personal_data
        LEFT JOIN Phone ON Personal_data.id = Phone.user_id
        WHERE phone_number = %s;
        """, (phone,))
    return cur.fetchall()


def del_client(cur, user_id):
    del_phone(cur, user_id, None)
    cur.execute("""
    DELETE FROM Personal_data
    WHERE id = %s 
    """, (user_id,))
    cur.execute("""
    SELECT * FROM Personal_data;
    """)
    return cur.fetchall()


def change_client(cur, user_id, name=None, surname=None, email=None, phone=None):
    if not name:
        name = '%'
    if not surname:
        surname = '%'
    if not email:
        email = '%'
    if phone:
        cur.execute("""
        SELECT user_id FROM Phone
        WHERE user_id = %s;
        """, (user_id,))
        if cur.fetchall():
            cur.execute("""
            UPDATE Phone
            SET phone_number=%s
            WHERE user_id=%s;
            """, (phone, user_id))
        else:
            add_phone(cur, user_id, phone)
    cur.execute("""
    UPDATE Personal_data
    SET user_name=%s, user_surname=%s, user_email=%s
    WHERE id=%s;
    """, (name, surname, email, user_id))

    cur.execute("""
    SELECT * FROM Personal_data
    LEFT JOIN Phone ON Personal_data.id = Phone.user_id;
    """)

    return cur.fetchall()



def main():
    conn = psycopg2.connect(database="netology_db", user="postgres", password="netologyAL")

    with conn.cursor() as cur:

        print(drop_table(cur))

        print(create_db(cur))
        conn.commit()

        print(f'Добавлен пользователь {add_client(cur, "Иван", "Петров", "petrov@mail.ru", 3255367)}')
        print(f'Добавлен пользователь {add_client(cur, "Сергей", "Наумов", "naum@mail.ru", 88003256)}')
        print(f'Добавлен пользователь {add_client(cur, "Михаил", "Лебедев", "lebed@mail.ru", 100500800)}')
        print(f'Добавлен пользователь {add_client(cur, "Иван", "Непряев", "iav@mail.ru")}')
        print(f'Добавлен пользователь {add_client(cur, "Сергей", "Николаев", "cav@mail.ru", 30303030)}')

        print(f'Добавить номер телефона клиенту id = 2, номер = {add_phone(cur, 2, 22222222)}')

        print(f'Изменим пользователя Непряев на Сидоров:\n'
              f'{change_client(cur, 4, "Владимир", "Сидоров", "sidor@mail.ru", 135645751)}')

        print(f'Удалим телефон 22222222у пользователя с id=2:\n'
              f'{del_phone(cur, 2, 22222222)}')

        print(f'Удалим пользователя с id=5:\n{del_client(cur, 5)}')

        print(f'Ищем пользователя по имени Иван:\n'
              f'{search_client(cur, "Иван", None, None, None)}')
        print(f'Ищем пользователя по Фамилии Лебедев:\n'
              f'{search_client(cur, None, "Лебедев", None, None)}')
        print(f'Ищем пользователя по email sidor@mail.ru:\n'
              f'{search_client(cur, None, None, "sidor@mail.ru", None)}')
        print(f'Ищем пользователя по телефону 88003256:\n'
              f'{search_client(cur, None, None, None, 88003256)}')

        conn.commit()

    conn.close()


if __name__ == '__main__':
    main()
