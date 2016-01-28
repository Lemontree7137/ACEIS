#! /bin/bash	
java -jar ACEIS.jar cqelsCnt=0 csparqlCnt=4 smode=elastic qCnt=80 step=1000 query=queries3.txt duration=60
java -jar ACEIS.jar cqelsCnt=0 csparqlCnt=5 smode=elastic qCnt=90 step=1000 query=queries3.txt duration=60
java -jar ACEIS.jar cqelsCnt=0 csparqlCnt=5 smode=elastic qCnt=100 step=1000 query=queries3.txt duration=60


select ?addr where {?a http://www.insight-centre.org/ces#hasServerAddress ?addr} LIMIT 100

FeatureOfInterest-3f1458bb-dfe3-59ca-83d2-699d055b2b7e

SensorID-3f1458bb-dfe3-59ca-83d2-699d055b2b7e

PrimitiveEventService-3f1458bb-dfe3-59ca-83d2-699d055b2b7e

http://ict-citypulse.eu/PrimitiveEventService-3f1458bb-dfe3-59ca-83d2-699d055b2b7e