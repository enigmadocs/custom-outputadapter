# custom-outputadapter

Demonstrates a custom output adapter.

Custom step seems to be failing in `create_table()` at the following line (with `output_path` = `names/output`):

```
self.tables[table_name]['fh'] = open(output_path, 'wb')
```
