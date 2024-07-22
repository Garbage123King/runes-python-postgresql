# runes-python-postgresql
make sure your Postgres is running, then:
```
python insert.py
```
if you have done anything wrong, just delete all the tables to get back to original:
```
drop table transactions cascade;
drop table outputs cascade;
drop table op_return cascade;
drop table blocks cascade;
drop table inputs cascade;
```

