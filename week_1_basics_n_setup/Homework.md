
## Question 1
Now run the command to get help on the "docker build" command

Which tag has the following text? - *Write the image ID to the file* 

- `--imageid string`
- `--iidfile string`
- `--idimage string`
- `--idfile string`


Output:
![Jawaban No  1](https://github.com/fachrulghiffari/data-engineering-zoomcamp/assets/104657138/9de8ac1c-7fff-4f32-b1f2-7d55b2832c78)

The answer:
```--iidfile string``` _Write the image ID to the file_

##  Question 2

Run docker with the python:3.9 image in an interactive mode and the entrypoint of bash.
Now check the python modules that are installed ( use pip list). 
How many python packages/modules are installed?

- 1
- 6
- 3
- 7

Output:
```
Package    Version
---------- -------
pip        23.0.1
setuptools 58.1.0
wheel      0.41.2
```
Python:3.9 has 3 packages installed

## Question 3

How many taxi trips were totally made on January 15?

Query:
```
SELECT COUNT(*) AS Total_trips 
FROM green_taxi_trips
WHERE lpep_pickup_datetime >= '2019-01-15 00:00:00' 
	AND lpep_dropoff_datetime < '2019-01-15 24:00:00'
```
Output:
![Jawaban No  3](https://github.com/fachrulghiffari/data-engineering-zoomcamp/assets/104657138/07c6eb8f-9c8b-4c4b-ab1b-c19693ae83a6)

The answer is 20530

## Question 4

Which was the day with the largest trip distance?
- 2019-01-18
- 2019-01-28
- 2019-01-15
- 2019-01-10

Query:
```
SELECT DATE(lpep_pickup_datetime), MAX(trip_distance) AS max_trip_distance 
FROM green_taxi_trips
GROUP BY 1
ORDER BY 2 DESC
LIMIT 5;
```
Output:
![Jawaban No  4](https://github.com/fachrulghiffari/data-engineering-zoomcamp/assets/104657138/0d64b390-3303-415e-ad10-7a72eacbfe44)

The answer is 2019-01-15

## Question 5

In 2019-01-01 how many trips had 2 and 3 passengers?

Query:
```
SELECT DATE(lpep_pickup_datetime), passenger_count, COUNT(*) AS total_trips
FROM green_taxi_trips
WHERE DATE(lpep_pickup_datetime) = '2019-01-01'
	AND passenger_count IN (2,3)
GROUP BY 1, 2;
```
Output:
![Jawaban No  5](https://github.com/fachrulghiffari/data-engineering-zoomcamp/assets/104657138/0db97e13-4b0f-47d4-95c4-8c729e6a201a)

The answer:

## Question 6
For the passengers picked up in the Astoria Zone which was the drop off zone that had the largest tip?
The answer must be the name of the zone, not the id.

Query:
```
SELECT puz."Zone" AS pickup_loc, doz."Zone" AS dropoff_loc, MAX(gt.tip_amount) AS max
FROM green_taxi_trips AS gt
JOIN zones AS puz
	ON "PULocationID" = puz."LocationID"
JOIN zones AS doz
	ON "DOLocationID" = doz."LocationID"
WHERE puz."Zone" = 'Astoria'
GROUP BY 1,2
ORDER BY 3 DESC
LIMIT 5;
```

Output:
![Jawaban No  6](https://github.com/fachrulghiffari/data-engineering-zoomcamp/assets/104657138/94c775d3-693f-4cca-8adb-68e933bbb725)

The answer: Long Island City/Queens Plaza
