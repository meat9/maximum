import psycopg2
import pandas


def create_connection(db_name, db_user, db_password, db_host, db_port):
    connection = None
    try:
        connection = psycopg2.connect(
            database=db_name,
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port,
        )
        print("Connection to PostgreSQL DB successful")
    except psycopg2.OperationalError as e:
        print(f"The error '{e}' occurred")
    return connection


def sql_query(sql, connection):
    with connection:
        cur = connection.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        data = pandas.DataFrame(rows)
        data.columns = [column[0] for column in cur.description]
        print("Query successful")
        return data


# Настройка подлючения к БД
connection = create_connection(
    "data",
    "zhihar_aleksandr",
    "e0wImX0jg7Dq",
    "analytics.maximum-auto.ru",
    "15432"
)

# SQL-запрос
sql = 'SELECT DISTINCT ON (communication_id, communications.date_time) communication_id, communications.site_id, communications.visitor_id,' +\
    'communications.date_time AS date_time_communications,visitor_session_id, sessions.date_time AS date_time_sessions, campaign_id,' +\
    'coalesce(count(sessions.visitor_session_id) over (partition by sessions.visitor_session_id ' +\
    'order by communications.date_time rows between unbounded preceding and current row),0) as row_n ' +\
    'FROM  communications LEFT JOIN sessions  ON communications.visitor_id = sessions.visitor_id and ' +\
    'communications.site_id = sessions.site_id WHERE sessions.date_time <= communications.date_time ORDER BY communications.date_time'

# Запуск функции, возвращающей DataFrame объект итоговой таблицы
result = sql_query(sql, connection)

# Сохранение результата в файл *.csv
with open('result_zhikhar.csv', 'w') as file:
    result.to_csv(file, header=True, index=False)
    print("Save file is successful")
