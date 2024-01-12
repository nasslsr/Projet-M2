import datetime
import psycopg2
from psycopg2.extras import LogicalReplicationConnection
import mysql.connector
import redshift_connector
import logging


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')



def fetch_changes_from_slot(conn, slot_name):
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT lsn, xid, data FROM pg_logical_slot_get_binary_changes('{slot_name}', NULL , NULL, 'proto_version', '1', 'publication_names', 'test_pub')")
            changes = cur.fetchall()
            logging.info(f"Fetched changes from slot: {slot_name}")
            return changes
    except Exception as e:
        logging.error(f"Error fetching changes from slot {slot_name}: {e}")
        return []

def fetch_values_type(conn, table_name):
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    attname AS column_name,
                    pg_catalog.format_type(atttypid, atttypmod) AS data_type
                FROM
                    pg_catalog.pg_attribute
                WHERE
                    attrelid = (SELECT oid FROM pg_catalog.pg_class WHERE relname = %s) AND
                    attnum > 0 AND
                    NOT attisdropped;
                """, (table_name,))
            columns_info = cur.fetchall()
            table_info = {column_info[0]: column_info[1] for column_info in columns_info}
            logging.info(f"Fetched column types for table: {table_name}")
            return table_info
    except Exception as e:
        logging.error(f"Error fetching column types for table {table_name}: {e}")
        return {}


def get_table_name_from_changes(changes):
    try:
        for _, _, data in changes:
            message_type = data.tobytes()[0:1].decode('utf-8')
            if message_type == 'R':
                table_name = decode_relation(data.tobytes())
                logging.info(f"Extracted table name from changes: {table_name}")
                return table_name
        return None
    except (ValueError, TypeError) as e:
        logging.error(f"Error extracting table name from changes: {e}")
        return None


def decode_begin(byte_data):
    try:
        lsn = int.from_bytes(byte_data[1:9], 'big')
        begin_ts = int.from_bytes(byte_data[9:17], 'big')
        xid = int.from_bytes(byte_data[17:21], 'big')
        epoch = datetime.datetime(2000, 1, 1)
        begin_datetime = epoch + datetime.timedelta(microseconds=begin_ts)
        logging.info(f"Decoded BEGIN: LSN={lsn}, Timestamp={begin_datetime}, Transaction XID={xid}")
        return {
            "type": "BEGIN",
            "lsn": lsn,
            "begin_ts": begin_datetime,
            "xid": xid
        }
    except Exception as e:
        logging.error(f"Error decoding BEGIN message: {e}")
        return None

def decode_commit(byte_data):
    try:
        flags = byte_data[1]
        lsn_commit = int.from_bytes(byte_data[2:10], 'big')
        lsn = int.from_bytes(byte_data[10:18], 'big')
        commit_ts = int.from_bytes(byte_data[18:26], 'big')
        epoch = datetime.datetime(2000, 1, 1)
        commit_datetime = epoch + datetime.timedelta(microseconds=commit_ts)
        logging.info(f"Decoded COMMIT: Flags={flags}, LSN Commit={lsn_commit}, LSN Ended={lsn}, Timestamp={commit_datetime}")
        return {
            "type": "COMMIT",
            "flags": flags,
            "lsn_commit": lsn_commit,
            "lsn": lsn,
            "commit_ts": commit_datetime
        }
    except Exception as e:
        logging.error(f"Error decoding COMMIT message: {e}")
        return None

def decode_relation(byte_data):
    try:
        idx = 1

        relation_oid = int.from_bytes(byte_data[idx:idx + 4], 'big')
        idx += 4

        xid = int.from_bytes(byte_data[idx:idx + 4], 'big')
        idx += 4

        namespace_len = byte_data[idx:].find(b'\x00')
        namespace = byte_data[idx:idx + namespace_len].decode('utf-8')
        idx += namespace_len + 1

        relation_name_len = byte_data[idx:].find(b'\x00')
        relation_name = byte_data[idx:idx + relation_name_len].decode('utf-8')
        idx += relation_name_len + 1

        replica_identity = byte_data[idx]
        idx += 1

        num_columns = int.from_bytes(byte_data[idx:idx + 2], 'big')
        idx += 2

        columns = []
        for _ in range(num_columns):
            column_flags = byte_data[idx]
            idx += 1

            column_name_len = byte_data[idx:].find(b'\x00')
            column_name = byte_data[idx:idx + column_name_len].decode('utf-8')
            idx += column_name_len + 1

            column_oid = int.from_bytes(byte_data[idx:idx + 4], 'big')
            idx += 4

            column_type_modifier = int.from_bytes(byte_data[idx:idx + 4], 'big')
            idx += 4

            columns.append({
                "flags": column_flags,
                "name": column_name,
                "oid": column_oid,
                "type_modifier": column_type_modifier
            })

        logging.info(f"Decoded RELATION: Transaction XID={xid}, Relation OID={relation_oid}, Namespace={namespace}, Relation Name={relation_name}, Replica Identity={replica_identity}, Columns={columns}")
        return relation_name
    except Exception as e:
        logging.error(f"Error decoding RELATION message: {e}")
        return None


def decode_insert(byte_data, table_info, table_name, target_db, syst_dest):
    try:
        idx = 0

        # Vérifier le type de message
        if byte_data[idx:idx + 1] != b'I':
            raise ValueError(f"Expected 'I' for insert, but got {byte_data[idx:idx + 1].decode('utf-8')}")

        idx += 1
        relation_oid = int.from_bytes(byte_data[idx:idx + 4], 'big')
        idx += 4

        tuple_type = byte_data[idx:idx + 1].decode('utf-8')
        idx += 1

        if tuple_type != 'N':
            raise ValueError(f"Expected 'N' for new tuple, but got {tuple_type}")

        # Lire les données du tuple
        tuple_data, idx = read_insert_tuple_data(byte_data, idx)
        inserted_values = [column['col_data'] for column in tuple_data['column_data']]

        insert_query = replicate_insert(table_info, inserted_values, table_name, target_db, syst_dest)
        logging.info(f"Decoded INSERT: Relation OID={relation_oid}, SQL query to replicate: {insert_query}")
        return insert_query
    except Exception as e:
        logging.error(f"Error decoding INSERT message: {e}")
        return None

def read_insert_tuple_data(buffer, idx):
    try:
        column_data = []
        n_columns = int.from_bytes(buffer[idx:idx + 2], 'big')
        idx += 2

        for _ in range(n_columns):
            col_data_category = buffer[idx:idx + 1].decode('utf-8')
            idx += 1

            if col_data_category in ("n", "u"):
                column_data.append({"col_data_category": col_data_category})
            elif col_data_category == "t":
                col_data_length = int.from_bytes(buffer[idx:idx + 4], 'big')
                idx += 4
                col_data = buffer[idx:idx + col_data_length].decode('utf-8')
                idx += col_data_length
                column_data.append({
                    "col_data_category": col_data_category,
                    "col_data_length": col_data_length,
                    "col_data": col_data
                })

        return {
            "n_columns": n_columns,
            "column_data": column_data
        }, idx
    except Exception as e:
        logging.error(f"Error reading insert tuple data: {e}")
        return {}, idx


def decode_update(byte_data, table_info, table_name, target_db, syst_dest):
    try:
        idx = 0

        if byte_data[idx:idx + 1] != b'U':
            raise ValueError(f"Expected 'U' for update, but got {byte_data[idx:idx + 1].decode('utf-8')}")
        idx += 1

        relation_oid = int.from_bytes(byte_data[idx:idx + 4], 'big')
        idx += 4

        tuple_type = byte_data[idx:idx + 1].decode('utf-8')
        idx += 1

        tuple_data, idx = read_update_tuple_data(byte_data, idx)

        mid_idx = len(tuple_data) // 2
        old_values = tuple_data[:mid_idx]
        new_values = tuple_data[mid_idx:]

        update_query = replicate_update(table_info, old_values, new_values, table_name, target_db, syst_dest)
        logging.info(f"Decoded UPDATE: Relation OID={relation_oid}, Old Tuple Data={old_values}, New Tuple Data={new_values}, SQL query to replicate: {update_query}")
        return update_query
    except Exception as e:
        logging.error(f"Error decoding UPDATE message: {e}")
        return None

def read_update_tuple_data(buffer, idx):
    try:
        tuple_data = []
        while idx < len(buffer):
            col_data_category = buffer[idx:idx + 1].decode('utf-8')
            idx += 1

            if col_data_category == "t":
                col_data_length = int.from_bytes(buffer[idx:idx + 4], 'big')
                idx += 4
                col_data = buffer[idx:idx + col_data_length].decode('utf-8')
                idx += col_data_length
                tuple_data.append(col_data)

        return tuple_data, idx
    except Exception as e:
        logging.error(f"Error reading update tuple data: {e}")
        return [], idx


def decode_delete(byte_data, table_info, table_name, target_db, syst_dest):
    try:
        idx = 1

        # Extraire l'ID de la transaction
        relation_oid = int.from_bytes(byte_data[idx:idx + 4], 'big')
        idx += 4

        idx += 1  # Passer le byte suivant

        deleted_values, idx = read_delete_tuple_data(byte_data, idx)
        delete_query = replicate_delete(table_info, deleted_values, table_name, target_db, syst_dest)

        logging.info(f"Decoded DELETE: Relation OID={relation_oid}, Old Tuple Data={deleted_values}, SQL query to replicate: {delete_query}")
        return delete_query
    except Exception as e:
        logging.error(f"Error decoding DELETE message: {e}")
        return None

def read_delete_tuple_data(buffer, idx):
    try:
        old_tuple_data = []
        while idx < len(buffer):
            col_data_category = buffer[idx:idx + 1].decode('utf-8')
            idx += 1

            if col_data_category == "t":
                col_data_length = int.from_bytes(buffer[idx:idx + 4], 'big')
                idx += 4
                col_data = buffer[idx:idx + col_data_length].decode('utf-8')
                idx += col_data_length
                old_tuple_data.append(col_data)

        return old_tuple_data, idx
    except Exception as e:
        logging.error(f"Error reading delete tuple data: {e}")
        return [], idx



def decode_truncate(byte_data, table_name,target_db,syst_dest):
    print(byte_data)
    idx = 1

    transaction_id = int.from_bytes(byte_data[idx:idx + 4], 'big')
    idx += 4

    number_of_relations = int.from_bytes(byte_data[idx:idx + 4], 'big')
    idx += 4

    options = byte_data[idx]
    idx += 1

    relation_oids = []
    for _ in range(number_of_relations):
        oid = int.from_bytes(byte_data[idx:idx + 4], 'big')
        relation_oids.append(oid)
        idx += 4

    truncate_query = replicate_truncate(table_name,target_db,syst_dest)

    print(f"Message Type: TRUNCATE\n")
    # print(f"Transaction ID: {transaction_id}")
    # print(f"Number of Relations: {number_of_relations}")
    # print(f"Options: {options}")
    # print(f"Relation OIDs: {relation_oids}")
    print('Here is the SQL query to replicate : ', truncate_query, '\n')

    return truncate_query


def replicate_insert(table_info, inserted_values, table_name,target_db,syst_dest):
    try :
        result_insert = {}
        for (column_name, column_type), value in zip(table_info.items(), inserted_values):
            result_insert[column_name] = {'value': value, 'column_type': column_type}

        columns = ', '.join(result_insert.keys())
        values = ', '.join(
            [f"'{value['value']}'" if 'character' in value['column_type'] else str(value['value']) for value in
             result_insert.values()])

        insert_template = f'INSERT INTO {table_name} ({columns}) VALUES ({values});'
        replicate_queries(insert_template,target_db,syst_dest)
        logging.info(f"Replicate INSERT: {insert_template}")
        return insert_template
    except Exception as e:
        logging.error(f"Error in replicate_insert: {e}")
        return None


def replicate_delete(table_info, deleted_values, table_name,target_db,syst_dest):
    try :
        result = {}
        for (column_name, column_type), value in zip(table_info.items(), deleted_values):
            result[column_name] = {'value': value, 'column_type': column_type}

        conditions = []
        for column_name, value_info in result.items():
            column_value = value_info['value']
            if 'character' in value_info['column_type']:
                conditions.append(f"{column_name} = '{column_value}'")
            else:
                conditions.append(f"{column_name} = {column_value}")

        if conditions:
            where_clause = ' AND '.join(conditions)
            delete_template = f'DELETE FROM {table_name} WHERE {where_clause};'
            replicate_queries(delete_template,target_db,syst_dest)
            logging.info(f"Replicate DELETE: {delete_template}")
            return delete_template
        else:
            logging.info("No conditions specified for DELETE")
            return "No delete conditions specified."
    except Exception as e:
        logging.error(f"Error in replicate_delete: {e}")
        return None


def replicate_update(table_info, old_values, new_values, table_name,target_db,syst_dest):
    try:
        result_new = {}
        result_old = {}
        updated_values = []

        for (column_name, column_type), new_value, old_value in zip(table_info.items(), new_values, old_values):
            result_new[column_name] = {'value': new_value, 'column_type': column_type}
            result_old[column_name] = {'value': old_value, 'column_type': column_type}

        for key in result_new:
            if result_new[key] != result_old[key]:
                column_name = key
                new_value = result_new[key]['value']
                if 'character' in result_new[key]['column_type']:
                    updated_values.append(f"{column_name} = '{new_value}'")
                else:
                    updated_values.append(f"{column_name} = {new_value}")

        conditions = []
        for key, value in result_new.items():
            if value['value'] == result_old[key]['value']:
                column_name = key
                column_value = value['value']
                if 'character' in value['column_type']:
                    conditions.append(f"{column_name} = '{column_value}'")
                else:
                    conditions.append(f"{column_name} = {column_value}")

        if conditions and updated_values:
            where_clause = ' AND '.join(conditions)
            update_clause = ', '.join(updated_values)
            update_template = f"UPDATE {table_name} SET {update_clause} WHERE {where_clause};"
            replicate_queries(update_template,target_db,syst_dest)
            logging.info(f"Replicate UPDATE: {update_template}")
            return update_template
        else:
            logging.info("No update conditions specified.")
            return "No update conditions specified."
    except Exception as e:
        logging.error(f"Error in replicate_update: {e}")
        return None


def replicate_truncate(table_name,target_db,syst_dest):
    truncate_template = f'TRUNCATE {table_name}'
    replicate_queries(truncate_template,target_db,syst_dest)
    return truncate_template


def decode_message(data, table_info, table_name, target_db, syst_dest):
    try:
        byte_data = data.tobytes()
        message_type = byte_data[0:1].decode('utf-8')

        if message_type == 'B':
            return decode_begin(byte_data)
        elif message_type == 'C':
            return decode_commit(byte_data)
        elif message_type == 'R':
            return decode_relation(byte_data)
        elif message_type == 'I':
            return decode_insert(byte_data, table_info, table_name, target_db, syst_dest)
        elif message_type == 'T':
            return decode_truncate(byte_data, table_name, target_db, syst_dest)
        elif message_type == 'D':
            return decode_delete(byte_data, table_info, table_name, target_db, syst_dest)
        elif message_type == 'U':
            return decode_update(byte_data, table_info, table_name, target_db, syst_dest)
        else:
            logging.error(f"Unrecognized message type: {message_type}")
            raise ValueError(f"Unrecognized message type: {message_type}")
    except Exception as e:
        logging.error(f"Error in decode_message: {e}")
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
        elif syst_dest == 'redshift':
            conn_params = {
                'database': target_db,
                'host': '',
                'port': 5439,
                'user': '',
                'password': '',
            }
            conn = redshift_connector.connect(**conn_params)
        else:
            raise ValueError("Unsupported DBMS type")

        logging.info(f"Connected to target database: {target_db} using {syst_dest}")
        return conn
    except Exception as e:
        logging.error(f"Error connecting to database {target_db} with system {syst_dest}: {e}")
        return None


def replicate_queries(query, target_db, syst_dest):
    conn = None
    try:
        conn = target_db_connection(target_db, syst_dest)
        cursor = conn.cursor()
        cursor.execute(query)
        conn.commit()
        logging.info(f"Query executed successfully on {target_db} using {syst_dest}: {query}")
    except psycopg2.Error as e:
        logging.error(f"PostgreSQL error occurred: {e}")
        if conn:
            conn.rollback()
    except mysql.connector.Error as e:
        logging.error(f"MySQL error occurred: {e}")
        if conn and conn.is_connected():
            conn.rollback()
    finally:
        if conn:
            cursor.close()
            if syst_dest == 'mysql' and conn.is_connected():
                conn.close()
            elif syst_dest == 'postgresql':
                conn.close()

def main(source_db, target_db, syst_dest):
    try:
        # Paramètres de connexion
        conn_params = {
            'host': 'localhost',
            'port': 5555,
            'dbname': source_db,
            'user': 'postgres',
            'password': 'postgres',
            'connection_factory': LogicalReplicationConnection
        }
        with psycopg2.connect(**conn_params) as conn:
            slot_name = "user_slot"
            changes = fetch_changes_from_slot(conn, slot_name)

            table_name = get_table_name_from_changes(changes)
            if table_name:
                table_info = fetch_values_type(conn, table_name)
            else:
                logging.error("No RELATION message found; unable to determine table name.")
                raise ValueError("No RELATION message found; unable to determine table name.")

            for lsn, xid, data in changes:
                decode_message(data, table_info, table_name, target_db, syst_dest)
            logging.info("Finished processing changes.")
    except Exception as e:
        logging.error(f"Error in main function: {e}")


if __name__ == "__main__":
    main()
