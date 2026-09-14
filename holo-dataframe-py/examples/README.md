# Hologres DataFrame Examples

All examples connect directly to Hologres. Set these variables first:

```bash
export HOLO_HOST=xxx.hologres.aliyuncs.com
export HOLO_PORT=80
export HOLO_DATABASE=postgres
export HOLO_USER='<access-id>'
export HOLO_PASSWORD='<access-key>'
```

The read-only examples expect these table shapes by default:

- `public.orders`: `order_id`, `user_id`, `region`, `amount`
- `public.users`: `user_id`, `region`

Override them with `HOLO_ORDERS_TABLE` and `HOLO_USERS_TABLE`.

```bash
python examples/basic_query.py
python examples/join_and_aggregate.py
```

`write_rows.py` requires an existing target with columns
`id bigint, region text, amount bigint`:

```bash
export HOLO_WRITE_TABLE=public.example_orders
python examples/write_rows.py

# The target must have a primary key on id for UPSERT.
HOLO_WRITE_UPSERT=1 python examples/write_rows.py
```

`oss_files.py` requires Hologres 4.1+ and OSS permissions. Reading is the
default; export is enabled explicitly:

```bash
export HOLO_OSS_SOURCE=oss://bucket/input/
export HOLO_OSS_ENDPOINT=oss-cn-hangzhou-internal.aliyuncs.com
python examples/oss_files.py

export HOLO_EXPORT_TABLE=public.orders
export HOLO_OSS_DESTINATION=oss://bucket/output/
HOLO_OSS_EXPORT=1 python examples/oss_files.py
```

`dynamic_table.py` creates a Dynamic Table and therefore requires a target
name and a Hologres version with Dynamic Table support:

```bash
export HOLO_DYNAMIC_TABLE=public.dt_orders_example
python examples/dynamic_table.py
```
