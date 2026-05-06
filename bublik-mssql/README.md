
#### Clustering Key (A)

```sql
WHERE 
    (A >= ?) 
AND 
    (A < ?)
ORDER BY A
```

#### Clustering Key (A DESC)

```sql
WHERE 
    (A <= ?) 
AND 
    (A > ?)
ORDER BY A DESC
```

#### Clustering Key (A, B)

```sql
WHERE 
    (
    (A > ?) OR 
    (A = ? AND B >= ?)
    )
AND (
    (A < ?) OR 
    (A = ? AND B < ?)
    )
ORDER BY A, B
```

#### Clustering Key (A, B DESC)

```sql
WHERE 
    (
    (A > ?) OR 
    (A = ? AND B <= ?)
    )
AND (
    (A < ?) OR 
    (A = ? AND B > ?)
    )
ORDER BY A, B DESC
```

#### Clustering Key (A DESC, B)

```sql
WHERE 
    (
    (A < ?) OR 
    (A = ? AND B >= ?)
    )
AND (
    (A > ?) OR 
    (A = ? AND B < ?)
    )
ORDER BY A DESC, B
```

#### Clustering Key (A DESC, B DESC)

```sql
WHERE 
    (
    (A < ?) OR 
    (A = ? AND B <= ?)
    )
AND (
    (A > ?) OR 
    (A = ? AND B > ?)
    )
ORDER BY A DESC, B DESC
```

#### Clustering Key (A DESC, B DESC)

```sql
WHERE
    (
    (A < ?) OR 
    (A = ? AND B <= ?)
    )
AND (
    (A > ?) OR 
    (A = ? AND B > ?)
    )
ORDER BY A DESC, B DESC
```

#### Clustering Key (A, B, C)

```sql
WHERE
    (
    (A > ?) OR 
    (A = ? AND B > ?) OR 
    (A = ? AND B = ? AND C >= ?)
    )
AND (
    (A < ?) OR 
    (A = ? AND B < ?) OR 
    (A = ? AND B = ? AND C < ?)
    )
ORDER BY A, B, C
```

#### Clustering Key (A DESC, B, C)
- Блок Старта (ВКЛЮЧАЯ):
    - A < ? — Переходим к следующим (меньшим) группам по первой колонке.
    - A = ? AND B > ? — Если A совпало, идем «вверх» по возрастающей колонке B.
    - A = ? AND B = ? AND C >= ? — Если первые две совпали, берем C начиная со стартового значения.
- Блок Конца (ИСКЛЮЧАЯ):
    - A > ? — Ограничиваем выборку «верхними» (большими) группами по A.
    - A = ? AND B < ? — Если дошли до граничной группы A, ограничиваем по возрастающей B.
    - A = ? AND B = ? AND C < ? — В самой глубокой вложенности отсекаем конечную точку C.

```sql
WHERE
    (
    (A < ?) OR 
    (A = ? AND B > ?) OR 
    (A = ? AND B = ? AND C >= ?)
    )
AND (
    (A > ?) OR 
    (A = ? AND B < ?) OR 
    (A = ? AND B = ? AND C < ?)
    )
ORDER BY A DESC, B, C
```

#### Clustering Key (A, B DESC, C)
```sql
WHERE
    (
    (A > ?) OR 
    (A = ? AND B < ?) OR 
    (A = ? AND B = ? AND C >= ?)
    )
AND (
    (A < ?) OR 
    (A = ? AND B > ?) OR 
    (A = ? AND B = ? AND C < ?)
    )
ORDER BY A, B DESC, C
```

#### Clustering Key (A, B, C DESC)
```sql
WHERE
    (
    (A > ?) OR 
    (A = ? AND B > ?) OR 
    (A = ? AND B = ? AND C <= ?)
    )
AND (
    (A < ?) OR 
    (A = ? AND B < ?) OR 
    (A = ? AND B = ? AND C > ?)
    )
ORDER BY A, B, C DESC
```

#### Clustering Key (A DESC, B DESC , C DESC)

```sql
WHERE 
    (
    (A < ?) OR 
    (A = ? AND B < ?) OR 
    (A = ? AND B = ? AND C <= ?)
    )
AND (
    (A > ?) OR 
    (A = ? AND B > ?) OR 
    (A = ? AND B = ? AND C > ?)
    )
ORDER BY A DESC, B DESC, C DESC
```
