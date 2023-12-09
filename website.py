from flask import Flask, render_template, request
import psycopg2
import psycopg2.extras
import decode_and_replicate_messages, ddl_replication

app = Flask(__name__)


def get_db_connection():
    conn_params = {
        'host': 'localhost',
        'port': 5555,
        'dbname': 'project-data',
        'user': 'postgres',
        'password': 'postgres'
    }
    conn = psycopg2.connect(**conn_params)
    return conn

def execute_query_dml(source_db, sql_query):
    conn_params = {
        'host': 'localhost',
        'port': 5555,
        'dbname': source_db,
        'user': 'postgres',
        'password': 'postgres'
    }
    conn = psycopg2.connect(**conn_params)
    try:
        with conn.cursor() as cur:
            cur.execute(sql_query)
            conn.commit()
    except Exception as e:
        print(f"Erreur lors de l'exécution de la requête: {e}")
    finally:
        conn.close()


@app.route('/', methods=['GET'])
def index():
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

    cur.execute("SELECT datname FROM pg_database;")
    db_list = cur.fetchall()

    cur.close()
    conn.close()
    return render_template('index.html', db_list=db_list)

@app.route('/execute_query', methods=['POST'])
def decode():
    source_db = request.form['source-db']
    target_db = request.form['target-db']
    sql_query = request.form['sql-query']

    source_conn = ddl_replication.source_db_connection(source_db)
    target_conn = ddl_replication.source_db_connection(target_db)

    table_name = sql_query.split(' ')[2]

    if sql_query.split(' ')[0].upper() == 'ALTER':
        ddl_replication.execute_query_ddl(source_db, sql_query)
        if 'ADD' in sql_query.upper():
            ddl_replication.replicate_alter_table_add(source_conn, target_conn, table_name)
        elif 'DROP' in sql_query.upper():
            ddl_replication.replicate_alter_table_drop(source_conn, target_conn, table_name)
        elif 'ALTER COLUMN' in sql_query.upper():
            ddl_replication.replicate_alter_table_modify(source_conn, target_conn, table_name)
        else:
            print("Commande ALTER non prise en charge.")

    else :
        execute_query_dml(source_db,sql_query)
        decode_and_replicate_messages.target_db_connection(target_db)
        decode_and_replicate_messages.main(source_db,target_db)


    return render_template('result.html')

@app.route('/show_table', methods=['POST'])
def show_table():
    source_db = request.form['source-db-data']
    target_db = request.form['target-db-data']
    table_name = request.form['table-name']

    # Récupérer les noms des tables pour chaque base de données
    source_data = get_table_data(source_db,table_name)
    target_data = get_table_data(target_db,table_name)

    return render_template('table_view.html', source_data=source_data, target_data=target_data)

def get_table_data(db_name, table_name):
    conn_params = {
        'dbname': db_name,
        'user': 'postgres',
        'password': 'postgres',
        'host': 'localhost',
        'port': 5555
    }
    conn = psycopg2.connect(**conn_params)
    table_data = {'columns': [], 'rows': []}
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT * FROM {table_name}")
            table_data['columns'] = [desc[0] for desc in cur.description]
            table_data['rows'] = cur.fetchall()
    except Exception as e:
        print(f"Erreur lors de la récupération des données de la table: {e}")
    finally:
        conn.close()
    return table_data




if __name__ == '__main__':
    app.run(debug=True, port=5432)
