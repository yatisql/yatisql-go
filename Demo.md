# yatisql-go Demo

## Processing on the fly

```bash
./bin/yatisql -i data/huge.csv.gz -t t1 -q "SELECT COUNT(*) FROM t1"
```

## Persistent DB and progress

```bash
./bin/yatisql -i data/huge2.csv.gz -t t1 -q "SELECT COUNT(*) FROM t1" -p -d data/test1.db
```

## Concurrent data ingestion

```bash
./bin/yatisql -i data/huge.csv.gz,data/huge2.csv.gz,data/huge3.csv.gz -d data/test.db -t t1,t2,t3 -p
```

## Using persistent DB for queries

```bash
./bin/yatisql -d data/test.db -q "SELECT t1.col1, t2.col1 as t2_col1, t3.col1 as t3_col1, t1.col2 FROM t1 JOIN t2 ON t2.col1=t1.col1 JOIN t3 ON t3.col1=t1.col1" -o data/joined.csv
```

We could also nest queries:

```bash
./bin/yatisql -d data/test.db -q "SELECT COUNT(*) FROM (SELECT t1.col1, t2.col1 as t2_col1, t3.col1 as t3_col1, t1.col2 FROM t1 JOIN t2 ON t2.col1=t1.col1 JOIN t3 ON t3.col1=t1.col1)"
```

## Running on large datasets

Kaggle, US Accidents (2016 - 2023) - https://www.kaggle.com/datasets/sobhanmoosavi/us-accidents.

7.7 million rows, 677 Mb gzip compressed file:

```csv
ID,Source,Severity,Start_Time,End_Time,Start_Lat,Start_Lng,End_Lat,End_Lng,Distance(mi),Description,Street,City,County,State,Zipcode,Country,Timezone,Airport_Code,Weather_Timestamp,Temperature(F),Wind_Chill(F),Humidity(%),Pressure(in),Visibility(mi),Wind_Direction,Wind_Speed(mph),Precipitation(in),Weather_Condition,Amenity,Bump,Crossing,Give_Way,Junction,No_Exit,Railway,Roundabout,Station,Stop,Traffic_Calming,Traffic_Signal,Turning_Loop,Sunrise_Sunset,Civil_Twilight,Nautical_Twilight,Astronomical_Twilight
A-1,Source2,3,2016-02-08 05:46:00,2016-02-08 11:00:00,39.865147,-84.058723,,,0.01,Right lane blocked due to accident on I-70 Eastbound at Exit 41 OH-235 State Route 4.,I-70 E,Dayton,Montgomery,OH,45424,US,US/Eastern,KFFO,2016-02-08 05:58:00,36.9,,91.0,29.68,10.0,Calm,,0.02,Light Rain,False,False,False,False,False,False,False,False,False,False,False,False,False,Night,Night,Night,Night
A-2,Source2,2,2016-02-08 06:07:59,2016-02-08 06:37:59,39.92805900000001,-82.831184,,,0.01,Accident on Brice Rd at Tussing Rd. Expect delays.,Brice Rd,Reynoldsburg,Franklin,OH,43068-3402,US,US/Eastern,KCMH,2016-02-08 05:51:00,37.9,,100.0,29.65,10.0,Calm,,0.0,Light Rain,False,False,False,False,False,False,False,False,False,False,False,False,False,Night,Night,Night,Day
...
```

Let's ingest the whole file and add indexes for `ID`, `Source` and `Severity` columns, it loads to default `data` table as we did not use `-t` argument:

```bash
./bin/yatisql -d data/us-car1.db -i data/us-car-accidents-2016-2023.csv.gz -x ID,Source,Severity -p
```

Now let's run a query:

```bash
./bin/yatisql -d data/us-car1.db -q "SELECT Severity, COUNT(*) FROM data GROUP BY Severity"
```

or with saving the output to a compressed CSV file:

```bash
./bin/yatisql -d data/us-car1.db -q "SELECT Severity as severity, COUNT(*) AS accident_count FROM data GROUP BY Severity" -o data/accidents_count_by_severity.csv.gz
```

```bash
$ zcat < data/accidents_count_by_severity.csv.gz
severity,accident_count
1,67366
2,6156981
3,1299337
4,204710
```