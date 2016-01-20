#! /bin/bash	




java -jar ACEIS.jar cqelsCnt=0 csparqlCnt=3 smode=rotation qCnt=50 step=5000 query=queries.txt duration=18

java -jar ACEIS.jar cqelsCnt=0 csparqlCnt=3 smode=balancedLatency qCnt=50 step=5000 query=queries.txt duration=18

java -jar ACEIS.jar cqelsCnt=0 csparqlCnt=5 smode=rotation qCnt=80 step=5000 query=queries.txt duration=18

java -jar ACEIS.jar cqelsCnt=0 csparqlCnt=5 smode=balancedLatency qCnt=80 step=5000 query=queries.txt duration=18

java -jar ACEIS.jar cqelsCnt=0 csparqlCnt=3 smode=rotation qCnt=50 step=5000 query=queries2.txt duration=18

java -jar ACEIS.jar cqelsCnt=0 csparqlCnt=3 smode=balancedLatency qCnt=50 step=5000 query=queries2.txt duration=18

java -jar ACEIS.jar cqelsCnt=0 csparqlCnt=5 smode=rotation qCnt=80 step=5000 query=queries2.txt duration=18

java -jar ACEIS.jar cqelsCnt=0 csparqlCnt=5 smode=balancedLatency qCnt=80 step=5000 query=queries2.txt duration=18


java -jar ACEIS.jar cqelsCnt=5 csparqlCnt=0 smode=elastic qCnt=30 step=3000 query=queries.txt duration=30

