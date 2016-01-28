#! /bin/bash	

java -jar ACEIS.jar cqelsCnt=2 csparqlCnt=0 smode=rotation qCnt=30 step=1000 query=queries.txt duration=30

java -jar ACEIS.jar cqelsCnt=3 csparqlCnt=0 smode=rotation qCnt=30 step=1000 query=queries.txt duration=30

java -jar ACEIS.jar cqelsCnt=4 csparqlCnt=0 smode=rotation qCnt=30 step=1000 query=queries.txt duration=30

java -jar ACEIS.jar cqelsCnt=5 csparqlCnt=0 smode=rotation qCnt=30 step=1000 query=queries.txt duration=30

java -jar ACEIS.jar cqelsCnt=0 csparqlCnt=2 smode=rotation qCnt=30 step=1000 query=queries.txt duration=30

java -jar ACEIS.jar cqelsCnt=0 csparqlCnt=3 smode=rotation qCnt=30 step=1000 query=queries.txt duration=30

java -jar ACEIS.jar cqelsCnt=0 csparqlCnt=4 smode=rotation qCnt=30 step=1000 query=queries.txt duration=30

java -jar ACEIS.jar cqelsCnt=0 csparqlCnt=5 smode=rotation qCnt=30 step=1000 query=queries.txt duration=30

java -jar ACEIS.jar cqelsCnt=0 csparqlCnt=10 smode=rotation qCnt=100 step=5000 query=queries2.txt duration=30

java -jar ACEIS.jar cqelsCnt=0 csparqlCnt=10 smode=balancedLatency qCnt=100 step=5000 query=queries2.txt duration=30

java -jar ACEIS.jar cqelsCnt=10 csparqlCnt=0 smode=rotation qCnt=100 step=5000 query=queries2.txt duration=30

java -jar ACEIS.jar cqelsCnt=10 csparqlCnt=0 smode=balancedLatency qCnt=100 step=5000 query=queries2.txt duration=30

java -jar ACEIS.jar cqelsCnt=0 csparqlCnt=5 smode=rotation qCnt=50 step=5000 query=queries2.txt duration=30

java -jar ACEIS.jar cqelsCnt=0 csparqlCnt=5 smode=balancedLatency qCnt=50 step=5000 query=queries2.txt duration=30

java -jar ACEIS.jar cqelsCnt=5 csparqlCnt=0 smode=rotation qCnt=50 step=5000 query=queries2.txt duration=30

java -jar ACEIS.jar cqelsCnt=5 csparqlCnt=0 smode=balancedLatency qCnt=50 step=5000 query=queries2.txt duration=30
