import psycopg2



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

    source_conn = psycopg2.connect(**conn_params)

    return source_conn

def target_db_connection(target_db):
    conn_params = {
        'host': 'localhost',
        'port': 5555,
        'dbname': target_db,
        'user': 'postgres',
        'password': 'postgres',
    }

    target_conn = psycopg2.connect(**conn_params)

    return target_conn
def get_table_structure(conn, table_name):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = %s;
        """, (table_name,))
        return cur.fetchall()

def replicate_alter_table_add(source_conn, target_conn, table_name):
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
                print(f"Column {column} added to {table_name} in target database.")


def replicate_alter_table_drop(source_conn, target_conn, table_name):
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
                print(f"Column {column} dropped from {table_name} in target database.")


def replicate_alter_table_modify(source_conn, target_conn, table_name):
    source_structure = get_table_structure(source_conn, table_name)
    target_structure = get_table_structure(target_conn, table_name)

    source_columns = {col[0]: col[1] for col in source_structure}
    target_columns = {col[0]: col[1] for col in target_structure}

    for column, data_type in source_columns.items():
        if column in target_columns:
            if data_type != target_columns[column]:
                with target_conn.cursor() as cur:
                    alter_query = f"ALTER TABLE {table_name} ALTER COLUMN {column} TYPE {data_type};"
                    cur.execute(alter_query)
                    target_conn.commit()
                    print(f"Type of column {column} altered to {data_type} in {table_name} in target database.")
