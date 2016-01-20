======= main figures =========
agg_avg_correctness_trends.pdf:

x-axis: day of month

y-axis: aggregated correctness for queries, averaged in each day.

data series: showing the correctness updates for 3 different queries over Aarhus traffic sensors in August, 2014.
———————————————————————————
constraint_violations.pdf:

x-axis: three different queries

y-axis: percentage of quality updates that are critical

data series: showing the percentages of quality updates that will trigger technical adaptation for the 3 queries under 3 different constraints.
———————————————————————————
aceis_recover_time.pdf:

x-axis: number of sensors in different sensor repositories, size = (450 traffic + 450 pollution)* (1+n), n = number of fake duplicated sensors with random quality descriptions, n={3,4,…,9}

y-axis: adaptation time in milliseconds, including ACEIS re-composition and re-deployment time, excluding query engine delays.

data series: Q2 involves 5 sensors, Q3 involves 10 sensors, Q2 and Q3 adaptation use Genetic Algorithms (GA), Q1 involves 2 sensors, too small search space for GA so Brute-force enumeration is used (BF). (BF is efficient when query complexity and repo size is small but cannot scale)
======= backup figures ========
q1_2_sensors.png, q2_5_sensors.png, q3_10_sensors.png: show the queries on the map.
