import uuid
import happybase


def __get_h_table__(hbase, table_name, cf=None):
    try:
        if not cf:
            cf = {"cf": {}}
        hbase.create_table(table_name, cf)
    except Exception as e:
        if str(e.__class__) == "<class 'Hbase_thrift.AlreadyExists'>":
            pass
        else:
            print(e)
    return hbase.table(table_name)


def save_to_hbase(documents, h_table_name, hbase_connection, cf_mapping, row_fields=None, batch_size=1000):
    hbase = happybase.Connection(**hbase_connection)
    table = __get_h_table__(hbase, h_table_name, {cf: {} for cf, _ in cf_mapping})
    h_batch = table.batch(batch_size=batch_size)
    row_auto = 0
    uid = uuid.uuid4()
    for d in documents:
        if not row_fields:
            row = f"{uid}~{row_auto}"
            row_auto += 1
        else:
            row = "~".join([str(d.pop(f)) if f in d else "" for f in row_fields])
        values = {}
        for cf, fields in cf_mapping:
            if fields == "all":
                for c, v in d.items():
                    values["{cf}:{c}".format(cf=cf, c=c)] = str(v)
            else:
                for c in fields:
                    if c in d:
                        values["{cf}:{c}".format(cf=cf, c=c)] = str(d[c])
        h_batch.put(str(row), values)
    h_batch.send()


def get_hbase_data_batch(hbase_conf, hbase_table, row_start=None, row_stop=None, row_prefix=None, columns=None,
                         _filter=None, timestamp=None, include_timestamp=False, batch_size=100000,
                         scan_batching=None, limit=None, sorted_columns=False, reverse=False):

    if row_prefix:
        row_start = row_prefix
        row_stop = row_prefix[:-1]+chr(row_prefix[-1]+1).encode("utf-8")

    if limit:
        if limit > batch_size:
            current_limit = batch_size
        else:
            current_limit = limit
    else:
        current_limit = batch_size
    current_register = 0
    while True:
        hbase = happybase.Connection(**hbase_conf)
        table = hbase.table(hbase_table)
        data = list(table.scan(row_start=row_start, row_stop=row_stop, columns=columns, filter=_filter,
                               timestamp=timestamp, include_timestamp=include_timestamp, batch_size=batch_size,
                               scan_batching=scan_batching, limit=current_limit, sorted_columns=sorted_columns,
                               reverse=reverse))
        if not data:
            break
        last_record = data[-1][0]
        current_register += len(data)
        yield data

        if limit:
            if current_register >= limit:
                break
            else:
                current_limit = min(batch_size, limit - current_register)
        row_start = last_record[:-1] + chr(last_record[-1] + 1).encode("utf-8")
    yield []
