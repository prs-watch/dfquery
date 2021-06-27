# dfquery

Operating `pandas.DataFrame` by SQL.

## Geting Started

```python
import pandas as pd
from dfquery import DFQuery

data = pd.DataFrame({
    "A": [0, 1, 2, 3, 4],
    "B": [10, 20, 30, 40, 50]
})

dq = DFQuery(globals())

print(dq.read(data, "select * from data where A > 2"))
print(dq.update(data, "update data set A = 100"))

dq.close()
```

## Basics

### `DFQuery`

Class to operate your `pandas.DataFrame`.  
It requires session or environment variables as a parameter. (`locals()` or `globals()`)

```python
dq = DFQuery(globals())
```

### `read`

Function for supporting `SELECT` statement.  
You need to pass following parameters.

- dataframe
    - `pandas.DataFrame` that you want to use it as driving table.
- query
    - `SELECT` statement SQL query.
- resources (optional)
    - If you want to query with inner table, set `pandas.DataFrame` as list.

This will return `pandas.DataFrame` as a result.

```python
df.read(data, "select * from data where A = 1")
df.read(data, "select * from data inner join it on data.A = it.C", resources=[it])
```

### `update`

Function for supporting `UPDATE` statement.  
You need to pass following parameters.

- dataframe
    - `pandas.DataFrame` that you want to use it as driving table.
- query
    - `SELECT` statement SQL query.
- resources (optional)
    - If you want to query with inner table, set `pandas.DataFrame` as list.

This will return `pandas.DataFrame` as a result.

```python
df.update(data, "update data set A = 100")
```

### `execute`

Wide use interface to execute query.  
You may use this function when you want to define your own function (UDF, UDAF).

### `close`

Delete `SQLite` database that is created temporarily.