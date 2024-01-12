from flask import Flask, request, jsonify
import psycopg2
import psycopg2.extras
import dml_replication_postgresql, ddl_replication_postgresql, dml_replication_mysql, ddl_replication_mysql
from flask_cors import CORS
import threading
import time
import mysql.connector
from mysql.connector import Error
import logging

app = Flask(__name__)
cors = CORS(app)

app.config['CORS_HEADERS'] = 'Content-Type'

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def connection_postgresql():
    try:
        conn_params = {
            'host': 'localhost',
            'port': 5555,
            'dbname': 'project-data',
            'user': 'postgres',
            'password': 'postgres'
        }
        conn = psycopg2.connect(**conn_params)

        logging.info(f"Connecting to PostgreSQL")
        return conn
    except Exception as e:
        logging.error(f"Error connecting to PostgreSQL: {e}")

@app.route('/execute_query', methods=['POST'])
def decode():

    data = request.json
    source_db = data.get('sourceDatabase')
    target_db = data.get('targetDatabase')
    sql_query = data.get('sqlQuery')

    source_conn = ddl_replication_postgresql.source_db_connection(source_db)
    target_conn = ddl_replication_postgresql.source_db_connection(target_db)

    table_name = sql_query.split(' ')[2]

    if sql_query.split(' ')[0].upper() == 'ALTER':
        ddl_replication_postgresql.execute_query_ddl(source_db, sql_query)
        if 'ADD' in sql_query.upper():
            ddl_replication_postgresql.replicate_alter_table_add(source_conn, target_conn, table_name)
        elif 'DROP' in sql_query.upper():
            ddl_replication_postgresql.replicate_alter_table_drop(source_conn, target_conn, table_name)
        elif 'ALTER COLUMN' in sql_query.upper():
            ddl_replication_postgresql.replicate_alter_table_modify(source_conn, target_conn, table_name)
        else:
            print("Commande ALTER non prise en charge.")

    else :
        execute_query_dml(source_db,sql_query)
        dml_replication_postgresql.target_db_connection(target_db)
        dml_replication_postgresql.main(source_db,target_db)

    return jsonify({'status': 'success', 'message': 'Query executed successfully'})

@app.route('/show_table', methods=['POST'])
def show_table():
    data = request.json
    source_db = data.get('sourceDatabase')
    target_db = data.get('targetDatabase')
    table_name = data.get('tableDatabase')

    source_data = get_table_data(source_db, table_name)
    target_data = get_table_data(target_db, table_name)

    return jsonify({'sourceData': source_data, 'targetData': target_data})

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


def mysql_connection():

    try:
        connection = mysql.connector.connect(
            host="localhost",
            database="receive_replication",
            port = 3306,
            user="nass",
            password="mysql"
        )
        logging.info(f"Connecting to MySQL")
        return connection
    except Error as e:
        logging.error(f"Error connecting to MySQL: {e}")
        return None

def postgresql_connection(dbname, db_type='postgresql'):
    try :
        if db_type == 'postgresql':

            conn_params = {
                'host': 'localhost',
                'port': 5555,
                'dbname': dbname,
                'user': 'postgres',
                'password': 'postgres'
            }
            logging.info(f"Connecting to PostgreSQL")
            return psycopg2.connect(**conn_params)
        else:
            raise ValueError("Type de base de données non supporté")
    except Exception as e:
        logging.error(f"Error connecting to PostgreSQL: {e}")

@app.route('/get_databases', methods=['POST'])
def get_databases():
    data = request.json
    db_type = data.get('type', 'postgresql')

    if db_type == 'mysql':
        conn = mysql_connection()
        if not conn:
            logging.error(f"Error connecting to MySQL")
            return jsonify({"error": "Connection to MySQL failed"}), 500
        query = "SHOW DATABASES"
    else:
        conn = postgresql_connection('postgres')
        if not conn:
            logging.error(f"Error connecting to PostgreSQL")
            return jsonify({"error": "Connection to PostgreSQL failed"}), 500
        query = "SELECT datname FROM pg_database"

    try:
        with conn.cursor() as cur:
            cur.execute(query)
            db_list = cur.fetchall()
            logging.info("Retrieved list of databases.")
    except Exception as e:
        logging.error(f"Error retrieving databases: {e}")
        return jsonify({"error": "Error retrieving databases"}), 500
    finally:
        conn.close()

    if db_type == 'mysql':
        return jsonify([{"name": db[0]} for db in db_list])
    else:
        return jsonify([{"name": db[0]} for db in db_list])

@app.route('/get_tables', methods=['POST'])
def get_tables():
    data = request.json
    db_type = data.get('type')
    db_name = data.get('dbname')

    if db_type == 'mysql':
        conn = mysql_connection()
        query = "SELECT table_name FROM information_schema.tables WHERE table_schema = %s;"
    else:
        conn = postgresql_connection(db_name)
        query = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';"

    try:
        with conn.cursor() as cur:
            cur.execute(query, (db_name,))
            tables_list = cur.fetchall()
            logging.info("Retrieved list of tables.")
    except Exception as e :
        logging.error(f"Error retrieving tables: {e}")
        return jsonify({"error": "Error retrieving tables"}), 500
    finally:
        conn.close()

    if db_type == 'mysql':
        return jsonify([{"table": table[0]} for table in tables_list])
    else:
        return jsonify([{"table": table[0]} for table in tables_list])


@app.route('/listen_continue', methods=['POST'])
def listen_continue():
    data = request.json

    source_config = data.get('sourceConfig')
    destination_config = data.get('destinationConfig')

    syst_source = source_config.get('syst_source')
    syst_dest = destination_config.get('syst_dest')

    table_source = source_config.get('table')
    table_dest = destination_config.get('table')

    source_db = source_config.get('database')
    target_db = destination_config.get('database')

    try:
        logging.info("Starting replication process.")

        if syst_source == 'postgresql':
            logging.info("Starting replication from PostgreSQL.")
            # Démarrage de la réplication DML pour PostgreSQL
            dml_replication_postgresql_thread = threading.Thread(target=continuous_dml_replication_postgresql, args=(source_db, target_db, syst_dest))
            dml_replication_postgresql_thread.start()

            # Démarrage de la réplication DDL pour PostgreSQL
            ddl_replication_postgresql_thread = threading.Thread(target=continuous_ddl_replication_postgresql, args=(source_db, target_db, syst_dest, table_dest))
            ddl_replication_postgresql_thread.start()

        elif syst_source == 'mysql':
            logging.info("Starting replication from MySQL.")
            # Démarrage de la réplication DML pour MySQL
            dml_replication_mysql_thread = threading.Thread(target=continuous_dml_replication_mysql, args=(source_db, target_db, syst_dest, table_source, table_dest))
            dml_replication_mysql_thread.start()

            # Démarrage de la réplication DDL pour MySQL
            ddl_replication_mysql_thread = threading.Thread(target=continuous_ddl_replication_mysql, args=(source_db, target_db, syst_dest, table_dest))
            ddl_replication_mysql_thread.start()

        logging.info("Replication activated successfully.")
        return jsonify({'status': 'success', 'message': 'Replication activated'})
    except Exception as e:
        logging.error(f"Error during replication: {e}")
        return jsonify({'status': 'error', 'message': str(e)})
def continuous_dml_replication_postgresql(source_db, target_db, syst_dest):
    while True:
        try:
            dml_replication_postgresql.target_db_connection(target_db, syst_dest)
            dml_replication_postgresql.main(source_db, target_db, syst_dest)

        except Exception as e:
            logging.error(f"Waiting for DML modifications to replicate: {e}")
        time.sleep(1)

def continuous_ddl_replication_postgresql(source_db, target_db, syst_dest, table_dest):
    while True:
        try:
            source_conn = ddl_replication_postgresql.source_db_connection(source_db)
            target_conn = ddl_replication_postgresql.target_db_connection(target_db, syst_dest)

            ddl_replication_postgresql.replicate_alter_table_add(source_conn, target_conn, table_dest)
            ddl_replication_postgresql.replicate_alter_table_drop(source_conn, target_conn, table_dest)
            ddl_replication_postgresql.replicate_alter_table_modify(source_conn, target_conn, table_dest, syst_dest)

            source_conn.close()
            target_conn.close()
        except Exception as e:
            logging.error(f"Waiting for DDL modifications to replicate: {e}")
        time.sleep(1)

def continuous_dml_replication_mysql(source_db, target_db, syst_dest, table_source, table_dest):
    while True:
        try:
            dml_replication_mysql.target_db_connection(target_db, syst_dest)
            dml_replication_mysql.main(source_db, target_db, syst_dest, table_source, table_dest)
        except Exception as e:
            logging.error(f"Waiting for DML modifications to replicate: {e}")
        time.sleep(1)

def continuous_ddl_replication_mysql(source_db, target_db, syst_dest, table_dest):
    while True:
        try:
            source_conn = ddl_replication_mysql.source_db_connection(source_db)
            target_conn = ddl_replication_mysql.target_db_connection(target_db, syst_dest)

            ddl_replication_mysql.replicate_alter_table_add(source_conn, target_conn, table_dest, target_db, source_db, syst_dest)
            ddl_replication_mysql.replicate_alter_table_drop(source_conn, target_conn, table_dest, target_db, source_db, syst_dest)
            ddl_replication_mysql.replicate_alter_table_modify(source_conn, target_conn, table_dest, target_db, syst_dest, source_db)

            source_conn.close()
            target_conn.close()
        except Exception as e:
            logging.error(f"Waiting for DDL modifications to replicate: {e}")
        time.sleep(1)

if __name__ == '__main__':
    app.run(debug=True, port=5432)
