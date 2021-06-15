import psycopg2
import pandas

#Настройка подлючения к БД
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

connection = create_connection(
    "data", 
    "zhihar_aleksandr", 
    "e0wImX0jg7Dq", 
    "analytics.maximum-auto.ru", 
    "15432"
)

#SQL-запросы, т.к. в задании необходимо получить все поля - запрос сделан с параметром'*'
sql_sessions = "SELECT * FROM sessions"
sql_communications = "SELECT * FROM communications"

#Сохранение запросов как pd.DataFrame объекты
sessions_df = pandas.read_sql_query(sql_sessions,connection)
communications_df = pandas.read_sql_query(sql_communications,connection)

# Добавление столбца с кол-вом сессий пользователя нарастающим итогом
sessions_df['row_n'] = 1 + sessions_df.groupby(['visitor_session_id','site_id']).cumcount().astype(int)

# Объединение двух таблиц
merged_left = (
    pandas.merge(
        left=communications_df, 
        right=sessions_df, 
        how='left', 
        suffixes = ('_communications','_sessions'),
        left_on=['visitor_id', 'site_id'],
        right_on=['visitor_id', 'site_id'],
        )
        .query('date_time_sessions <= date_time_communications')
    ).drop_duplicates(subset=['communication_id'], keep='last')
connection.close()

# Сохранение результата в файл *.csv
with open('result_zhikhar.csv', 'w') as file:
    merged_left.to_csv(file, header=True, index=False)
