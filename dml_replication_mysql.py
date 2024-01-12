from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import (
    DeleteRowsEvent,
    UpdateRowsEvent,
    WriteRowsEvent,
)
import mysql.connector
import psycopg2
import logging


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def replicate_insert(data, table_name, target_db, syst_dest, table_dest):
    try:
        columns = ', '.join(data.keys())
        values = ', '.join([f"'{value}'" if isinstance(value, str) else str(value) for value in data.values()])
        insert_template = f'INSERT INTO {table_dest} ({columns}) VALUES ({values});'
        replicate_queries(insert_template, target_db, syst_dest)
        logging.info(f"Insert query executed: {insert_template}")
        return insert_template
    except Exception as e:
        logging.error(f"Error in replicate_insert: {e}")

def replicate_delete(data, table_name, target_db, syst_dest, table_dest):
    try:
        conditions = [f"{column} = '{value}'" if isinstance(value, str) else f"{column} = {value}" for column, value in data.items()]
        where_clause = ' AND '.join(conditions)
        delete_template = f'DELETE FROM {table_dest} WHERE {where_clause};'
        replicate_queries(delete_template, target_db, syst_dest)
        logging.info(f"Delete query executed: {delete_template}")
        return delete_template
    except Exception as e:
        logging.error(f"Error in replicate_delete: {e}")

def replicate_update(before_values, after_values, table_name, target_db, syst_dest, table_dest):
    try:
        set_clauses = [f"{column} = '{after_values[column]}'" if isinstance(after_values[column], str) else f"{column} = {after_values[column]}" for column in after_values]
        conditions = [f"{column} = '{before_values[column]}'" if isinstance(before_values[column], str) else f"{column} = {before_values[column]}" for column in before_values]
        where_clause = ' AND '.join(conditions)
        update_template = f"UPDATE {table_dest} SET {', '.join(set_clauses)} WHERE {where_clause};"
        replicate_queries(update_template, target_db, syst_dest)
        logging.info(f"Update query executed: {update_template}")
        return update_template
    except Exception as e:
        logging.error(f"Error in replicate_update: {e}")

def replicate_queries(query, target_db, syst_dest):
    conn = None
    try:
        conn = target_db_connection(target_db, syst_dest)
        cursor = conn.cursor()
        cursor.execute(query)
        conn.commit()
        logging.info(f"Query executed successfully: {query}")
    except psycopg2.Error as e:
        logging.error(f"PostgreSQL error: {e}")
        if conn:
            conn.rollback()
    except mysql.connector.Error as e:
        logging.error(f"MySQL error: {e}")
        if conn and conn.is_connected():
            conn.rollback()
    finally:
        if conn:
            cursor.close()
            if syst_dest == 'mysql' and conn.is_connected():
                conn.close()
            elif syst_dest == 'postgresql':
                conn.close()

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
            conn = psycopg2.connect(**conn_params)
        elif syst_dest == 'mysql':
            conn_params = {
                'database': target_db,
                'host': 'localhost',
                'port': 3306,
                'user': 'nass',
                'password': 'mysql',
            }
            conn = mysql.connector.connect(**conn_params)
        else:
            raise ValueError("Unsupported DBMS type")
        logging.info(f"Connected to target database: {target_db} using {syst_dest}")
        return conn
    except Exception as e:
        logging.error(f"Error connecting to database: {e}")
        return None


def main(source_db, target_db, syst_dest, table_source, table_dest):
    mysql_settings = {
        "host": "localhost",
        "port": 3306,
        "user": "nass",
        "passwd": "mysql",
        "db": source_db
    }

    stream = BinLogStreamReader(
        connection_settings=mysql_settings,
        server_id=1,
        only_events=[DeleteRowsEvent, UpdateRowsEvent, WriteRowsEvent],
        blocking=True,
        only_tables=table_source,
        resume_stream=True
    )

    try:
        for binlogevent in stream:
            for row in binlogevent.rows:
                table_name = f"{binlogevent.schema}.{binlogevent.table}"
                logging.info(f"Processing table: {table_name} from {source_db}")

                event = {"schema": binlogevent.schema, "table": binlogevent.table}

                if isinstance(binlogevent, DeleteRowsEvent):
                    event["action"] = "delete"
                    event["data"] = row["values"]
                    replicate_delete(row["values"], table_name, target_db, syst_dest, table_dest)

                elif isinstance(binlogevent, UpdateRowsEvent):
                    event["action"] = "update"
                    event["before_values"] = row["before_values"]
                    event["after_values"] = row["after_values"]
                    replicate_update(row["before_values"], row["after_values"], table_name, target_db, syst_dest, table_dest)

                elif isinstance(binlogevent, WriteRowsEvent):
                    event["action"] = "insert"
                    event["data"] = row["values"]
                    replicate_insert(row["values"], table_name, target_db, syst_dest, table_dest)

                logging.info(f"Event processed: {event}")

    finally:
        stream.close()
        logging.info("BinLogStreamReader closed")







