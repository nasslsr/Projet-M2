import psycopg2
import mysql.connector
import logging
import redshift_connector

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def execute_query_ddl(source_db, sql_query):
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

def source_db_connection(source_db):
    conn_params = {
        'host': 'localhost',
        'port': 5555,
        'dbname': source_db,
        'user': 'postgres',
        'password': 'postgres',
    }
    try:
        source_conn = psycopg2.connect(**conn_params)
        logging.info(f"Connected to source database: {source_db}")
        return source_conn
    except Exception as e:
        logging.error(f"Error connecting to source database {source_db}: {e}")
        return None

def target_db_connection(target_db, syst_dest):
    try:
        if syst_dest == 'postgresql':
            conn_params = {
                'dbname': target_db,
                'host': 'localhost',
                'port': 5555,
                'user': 'postgres',
                'password': 'postgres',
            }
            target_conn = psycopg2.connect(**conn_params)
        elif syst_dest == 'mysql':
            conn_params = {
                'database': target_db,
                'host': 'localhost',
                'port': 3306,
                'user': 'nass',
                'password': 'mysql',
            }
            target_conn = mysql.connector.connect(**conn_params)
        elif syst_dest == 'redshift':
            conn_params = {
                'database': target_db,
                'host': '',
                'port': 5439,
                'user': '',
                'password': '',
            }
            target_conn = redshift_connector.connect(**conn_params)
        else:
            raise ValueError("Unsupported DBMS type")

        logging.info(f"Connected to target database: {target_db} using {syst_dest}")
        return target_conn
    except Exception as e:
        logging.error(f"Error connecting to target database {target_db} with system {syst_dest}: {e}")
        return None

def get_table_structure(conn, table_name):
    try :
        with conn.cursor() as cur:
            cur.execute("""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_name = %s;
            """, (table_name,))
            logging.info(f"Fetched table structure for {table_name}")
            return cur.fetchall()
    except Exception as e:
        logging.error(f"Error getting table structure for {table_name}: {e}")
        return []

def replicate_alter_table_add(source_conn, target_conn, table_name):
    try:
        source_structure = get_table_structure(source_conn, table_name)
        target_structure = get_table_structure(target_conn, table_name)

        source_columns = {col[0]: col[1] for col in source_structure}
        target_columns = {col[0]: col[1] for col in target_structure}

        for column, data_type in source_columns.items():
            if column not in target_columns:
                with target_conn.cursor() as cur:
                    alter_query = f"ALTER TABLE {table_name} ADD COLUMN {column} {data_type};"
                    cur.execute(alter_query)
                    target_conn.commit()
                    logging.info(f"Column {column} added to {table_name} in target database.")
    except Exception as e:
        logging.error(f"Error in replicate_alter_table_add for {table_name}: {e}")

def replicate_alter_table_drop(source_conn, target_conn, table_name):
    try:
        source_structure = get_table_structure(source_conn, table_name)
        target_structure = get_table_structure(target_conn, table_name)

        source_columns = {col[0]: col[1] for col in source_structure}
        target_columns = {col[0]: col[1] for col in target_structure}

        for column in target_columns:
            if column not in source_columns:
                with target_conn.cursor() as cur:
                    alter_query = f"ALTER TABLE {table_name} DROP COLUMN {column};"
                    cur.execute(alter_query)
                    target_conn.commit()
                    logging.info(f"Column {column} dropped from {table_name} in target database.")
    except Exception as e:
        logging.error(f"Error in replicate_alter_table_drop for {table_name}: {e}")

def replicate_alter_table_modify(source_conn, target_conn, table_name,syst_dest):

    try:

        source_structure = get_table_structure(source_conn, table_name)
        target_structure = get_table_structure(target_conn, table_name)

        source_columns = {col[0]: map_data_types(col[1]) for col in source_structure}
        target_columns = {col[0]: map_data_types(col[1]) for col in target_structure}

        for column, data_type in source_columns.items():
            if column in target_columns:
                if data_type != target_columns[column]:
                    with target_conn.cursor() as cur:
                        if syst_dest == 'postgresql' :
                            alter_query = f"ALTER TABLE {table_name} ALTER COLUMN {column} TYPE {data_type};"
                        elif syst_dest =='mysql' :
                            alter_query = f"ALTER TABLE {table_name} MODIFY {column} {data_type};"
                        cur.execute(alter_query)
                        target_conn.commit()
                        logging.info(f"Type of column {column} altered to {data_type} in {table_name} in target database.")
    except Exception as e:
        logging.error(f"Error in replicate_alter_table_modify for {table_name}: {e}")


def map_data_types(postgres_type):
    mapping = {
        'integer': 'int',
        'character varying': 'varchar'
    }
    return mapping.get(postgres_type, postgres_type)
