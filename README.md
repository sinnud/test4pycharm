# postgresql
This is a simple ackage to connect to postgresql and submit queries.

## create package
Run command:
```
python3 setup.py sdist bdist_wheel
```

## upload to testpypi.org
Run command:
```
python3 -m twine upload --repository testpypi dist/*
```
With user name `__token__` and password `your_token`

## install from your virtual environment

Run command:
```
pip install --index-url https://test.pypi.org/simple  -no-deps postgresql-sinnud -U
```
The `-U` option let you uninstall previous version and install the newest version.

## use in python
```
from postgresql.utils import PostgresqlUtils
psc = PostgresqlUtils(host='host_with_postgresql')
psc.conn.execute('select count(*) from schema.table')
```

## sphinx doc generation
Under folder docs, run 
```
make html
```
Need Python module sphinx (sphinx-rtd-theme is you use this theme)