import datetime
import psycopg2
from psycopg2.extras import LogicalReplicationConnection


def fetch_changes_from_slot(conn, slot_name):
    # récupération des messages dans le WAL via la requête SQL
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT lsn, xid, data FROM pg_logical_slot_get_binary_changes('{slot_name}', NULL, NULL, 'proto_version', '1', 'publication_names', 'test_pub')")
        return cur.fetchall()


def fetch_values_type(conn, table_name):
    # Récupération des colonnes de la table avec leur type via la requête SQL
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
        table_info = {}

        for column_info in columns_info:
            column_name, data_type = column_info
            table_info[column_name] = data_type

        return table_info


def get_table_name_from_changes(changes):
    # Permet de récupérer le nom de la table dans laquelle la requête DML a eu lieu
    for _, _, data in changes:
        # Décode le premier byte
        message_type = data.tobytes()[0:1].decode('utf-8')
        if message_type == 'R':
            return decode_relation(data.tobytes())


def decode_begin(byte_data):
    lsn = int.from_bytes(byte_data[1:9], 'big')
    begin_ts = int.from_bytes(byte_data[9:17], 'big')
    xid = int.from_bytes(byte_data[17:21], 'big')

    # Convertit le timestamp en datetime pour obtenir l'heure de l'éxécution des requêtes
    epoch = datetime.datetime(2000, 1, 1)
    begin_datetime = epoch + datetime.timedelta(microseconds=begin_ts)

    # Afficher les informations
    print(f"Message Type: BEGIN")
    print(f"LSN: {lsn}")
    print(f"Begin Timestamp: {begin_datetime}")
    print(f"Transaction XID: {xid}\n")

    return {
        "type": "BEGIN",
        "lsn": lsn,
        "begin_ts": begin_datetime,
        "xid": xid
    }


def decode_commit(byte_data):
    flags = byte_data[1]
    lsn_commit = int.from_bytes(byte_data[2:10], 'big')
    lsn = int.from_bytes(byte_data[10:18], 'big')
    commit_ts = int.from_bytes(byte_data[18:26], 'big')

    epoch = datetime.datetime(2000, 1, 1)
    commit_datetime = epoch + datetime.timedelta(microseconds=commit_ts)

    # Afficher les informations
    print(f"Message Type: COMMIT")
    print(f"Flags: {flags}")
    print(f"LSN Commit: {lsn_commit}")
    print(f"LSN ENDED: {lsn}")
    print(f"Commit Timestamp: {commit_datetime}\n")

    return {
        "type": "COMMIT",
        "flags": flags,
        "lsn_commit": lsn_commit,
        "lsn": lsn,
        "commit_ts": commit_datetime
    }


def decode_relation(byte_data):
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

    print(f"Message Type: RELATION")
    print(f"Transaction XID: {xid}")
    print(f"Relation OID: {relation_oid}")
    print(f"Namespace: {namespace}")
    print(f"Relation Name: {relation_name}")
    print(f"Replica Identity: {replica_identity}")
    print(f"Columns: {columns}\n")

    return relation_name


def decode_insert(byte_data, table_info, table_name):
    print(byte_data)
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

    insert_query = replicate_insert(table_info, inserted_values, table_name)

    print(f"Message Type: INSERT")
    print(f"Relation OID: {relation_oid}")
    # print(f"Tuple Type: {tuple_type}")
    # print(f"Tuple Data: {tuple_data}\n")
    print('Here is the SQL query to replicate : ', insert_query, '\n')

    return insert_query


def read_insert_tuple_data(buffer, idx):
    column_data = []
    n_columns = int.from_bytes(buffer[idx:idx + 2], 'big')
    idx += 2

    for _ in range(n_columns):
        col_data_category = buffer[idx:idx + 1].decode('utf-8')
        idx += 1

        if col_data_category in ("n", "u"):
            column_data.append({
                "col_data_category": col_data_category
            })
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


def decode_update(byte_data, table_info, table_name):
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

    update_query = replicate_update(table_info, old_values, new_values, table_name)

    print(f"Message Type: UPDATE")
    print(f"Relation Oid: {relation_oid}")
    # print(f"Tuple Type: {tuple_type}")
    print(f"Old Tuple Data: {old_values}")
    print(f"New Tuple Data: {new_values}\n")
    print('Here is the SQL query to replicate : ', update_query, '\n')

    return update_query


def read_update_tuple_data(buffer, idx):
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


def decode_delete(byte_data, table_info, table_name):
    print(byte_data)
    idx = 1

    # Extraire l'ID de la transaction
    relation_oid = int.from_bytes(byte_data[idx:idx + 4], 'big')
    idx += 4

    idx += 1

    deleted_values, idx = read_delete_tuple_data(byte_data, idx)

    delete_query = replicate_delete(table_info, deleted_values, table_name)

    print(f"Message Type: DELETE")
    print(f"Relation OID: {relation_oid}")
    print(f"Old Tuple Data: {deleted_values}\n")
    print('Here is the SQL query to replicate : ', delete_query, '\n')

    return delete_query


def read_delete_tuple_data(buffer, idx):
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


def decode_truncate(byte_data, table_name):
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

    truncate_query = replicate_truncate(table_name)

    print(f"Message Type: TRUNCATE\n")
    # print(f"Transaction ID: {transaction_id}")
    # print(f"Number of Relations: {number_of_relations}")
    # print(f"Options: {options}")
    # print(f"Relation OIDs: {relation_oids}")
    print('Here is the SQL query to replicate : ', truncate_query, '\n')

    return truncate_query


def replicate_insert(table_info, inserted_values, table_name):
    result_insert = {}
    for (column_name, column_type), value in zip(table_info.items(), inserted_values):
        result_insert[column_name] = {'value': value, 'column_type': column_type}

    columns = ', '.join(result_insert.keys())
    values = ', '.join(
        [f"'{value['value']}'" if 'character' in value['column_type'] else str(value['value']) for value in
         result_insert.values()])

    # Génére la requête SQL complète
    insert_template = f'INSERT INTO {table_name} ({columns}) VALUES ({values});'
    replicate_queries(insert_template)
    return insert_template


def replicate_delete(table_info, deleted_values, table_name):
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
        replicate_queries(delete_template)
        return delete_template
    else:
        return "Aucune condition de suppression n'a été spécifiée."


def replicate_update(table_info, old_values, new_values, table_name):
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
        replicate_queries(update_template)
        return update_template
    else:
        return "Aucune condition de mise à jour n'a été spécifiée."


def replicate_truncate(table_name):
    truncate_template = f'TRUNCATE {table_name}'
    replicate_queries(truncate_template)
    return truncate_template


def decode_message(data, table_info, table_name):
    byte_data = data.tobytes()
    message_type = byte_data[0:1].decode('utf-8')

    if message_type == 'B':
        return decode_begin(byte_data)
    elif message_type == 'C':
        return decode_commit(byte_data)
    elif message_type == 'R':
        return decode_relation(byte_data)
    elif message_type == 'I':
        return decode_insert(byte_data, table_info, table_name)
    elif message_type == 'T':
        return decode_truncate(byte_data, table_name)
    elif message_type == 'D':
        return decode_delete(byte_data, table_info, table_name)
    elif message_type == 'U':
        return decode_update(byte_data, table_info, table_name)
    else:
        raise ValueError(f"Type de message non reconnu : {message_type}")


def target_db_connection():
    conn_params = {
        'dbname': 'replication_target',
        'host': 'localhost',
        'port': 5555,
        'user': 'postgres',
        'password': 'postgres',
    }

    conn = psycopg2.connect(**conn_params)

    return conn


def replicate_queries(query):
    conn = target_db_connection()
    cur = conn.cursor()

    try:
        cur.execute(query)
        conn.commit()
    except psycopg2.Error as e:
        print(f"Une erreur est survenue : {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()


def main():
    # Paramètres de connexion
    conn_params = {
        'host': 'localhost',
        'port': 5555,
        'dbname': 'project-data',
        'user': 'postgres',
        'password': 'postgres',
        'connection_factory': LogicalReplicationConnection
    }

    # Établir la connexion
    with psycopg2.connect(**conn_params) as conn:
        slot_name = "user_slot"
        changes = fetch_changes_from_slot(conn, slot_name)

        # Récupère le nom de la table
        table_name = get_table_name_from_changes(changes)
        if table_name:
            table_info = fetch_values_type(conn, table_name)
        else:
            raise ValueError("Aucun message RELATION trouvé; impossible de déterminer le nom de la table.")

        # Récupère les messages à décoder
        for lsn, xid, data in changes:
            print(data)
            decode_message(data, table_info, table_name)


if __name__ == "__main__":
    main()
