# Introduction #

Add your content here.


# Details #

### SELECT  Query ###

```
"SELECT * FROM SeaDefence[NOW];" 120 "etc/query-parameters.xml"
```

### Select attributes without aliases Query ###

```
"SELECT s.seaLevel FROM SeaDefence[NOW] s;" 120
"etc/query-parameters.xml"
```

### Select attributes with aliases Query ###

```
"SELECT s.seaLevel as sl FROM SeaDefence[NOW] s;" 120
"etc/query-parameters.xml"
```


### SELECT attribute without alias and expression with alias Query ###

```
"SELECT seaLevel, (s.seaLevel + 2) as sl FROM SeaDefence[NOW] s;" 120
"etc/query-parameters.xml"
```

### SELECT attribute without alias and expression without alias Query ###

```
"SELECT seaLevel, (s.seaLevel + 2) FROM SeaDefence[NOW] s;" 120
"etc/query-parameters.xml"
```

### SELECT query with power function ###

```
"SELECT s.seaLevel, s.voltage^ 2 FROM SeaDefence[NOW] s;" 120
"etc/query-parameters.xml"
```

### SELECT query with square root function ###

```
"SELECT s.seaLevel, sqrt(s.seaLevel) FROM SeaDefence[NOW] s;" 120
"etc/query-parameters.xml"
```

### SELECT with ABS function ###

```
"SELECT (s.seaLevel - s.voltage), abs(s.seaLevel - s.voltage)FROM SeaDefence[NOW] s;" 120
"etc/query-parameters.xml"
```

### Average query without alias ###

```
"SELECT avg(s.seaLevel) FROM SeaDefence[NOW] s;" 120
"etc/query-parameters.xml"
```

### Average query with alias ###

```
"SELECT avg(s.seaLevel) as savg FROM SeaDefence[NOW] s;" 120
"etc/query-parameters.xml"
```

### Several aggregation functions ###

```
"SELECT avg(s.seaLevel) as savg, min(seaLevel), max(seaLevel), count(seaLevel) FROM SeaDefence[NOW] s;" 120
"etc/query-parameters.xml"
```

### Standard deviation query ###

```
"SELECT stdev(s.seaLevel) FROM SeaDefence[NOW] s;" 120
"etc/query-parameters.xml"
```

### Aggregation containing simple mathematical expression ###

```
"SELECT avg(seaLevel * 3) as savg FROM SeaDefence[NOW] s;"
120 "etc/query-parameters.xml"
```

Doesn't work due to [issue 29](https://code.google.com/p/snee/issues/detail?id=29).

### Aggregation containing mathematical expression with more than one attribute ###

```
"SELECT avg(seaLevel + voltage) as savg FROM SeaDefence[NOW] s;"
120 "etc/query-parameters.xml"
```

Doesn't work due to [issue 29](https://code.google.com/p/snee/issues/detail?id=29).

```
"SELECT avg(seaLevel * seaLevel) as savg FROM SeaDefence[NOW] s;"
120 "etc/query-parameters.xml"
```

Doesn't work due to [issue 29](https://code.google.com/p/snee/issues/detail?id=29).

### Overtopping query over NOW ###

```
"SELECT stdev(s.seaLevel) as sstdev, avg(seaLevel) as savg, 
   max(seaLevel) as smax FROM SeaDefence[NOW] s;" 120
"etc/query-parameters.xml"
```

### TODO: some time window test queries ###

### Join Query with alias in SELECT clause ###

```
"SELECT e.sealevel as x, w.sealevel as y 
FROM SeaDefenceEast[NOW] e, SeaDefenceWest[NOW] w;" 
120 "etc/query-parameters.xml"
```

Doesn't work with alias on outer query due to [issue 28](https://code.google.com/p/snee/issues/detail?id=28).

### Join Query without alias in SELECT clause ###

```
"SELECT e.sealevel, w.sealevel FROM SeaDefenceEast[NOW] e, SeaDefenceWest[NOW] w;" 120 "etc/query-parameters.xml"
```

works!

### Linear Regression Query ###

```
"SELECT sd.sealevel, lr.a * sd.sealevel + lr.b
from (
	SELECT	( U.n*U.sum_xy - U.sum_y * U.sum_x ) / ( U.n * U.sum_xx - U.sum_x * U.sum_x ) as a,
		( U.sum_y * U.sum_xx - U.sum_x * U.sum_xy ) / ( U.n * U.sum_xx - U.sum_x * U.sum_x ) as b 
	FROM (	
		SELECT	COUNT( T.x ) as n, SUM( T.x ) as sum_x,	SUM( T.y ) as sum_y, SUM( T.xx ) as sum_xx, SUM( T.xy ) as sum_xy
	 	FROM (
			SELECT S.sealevel as x, S.voltage as y, S.sealevel * S.voltage as xy, S.sealevel * S.sealevel as xx 
			FROM ( 
				SELECT seaLevel, voltage 
				FROM SeaDefenceEast[FROM NOW-5 seconds TO NOW SLIDE 5 seconds] 
			) S
	 	) T
	) U
) lr, seadefence[now] sd;" 120 "etc/query-parameters.xml"
```

Doesn't work with alias on outer query due to [issue 28](https://code.google.com/p/snee/issues/detail?id=28).