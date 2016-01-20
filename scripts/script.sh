#! /bin/bash	


java -jar ACEIS.jar cqelsCnt=1 csparqlCnt=0 smode=rotation qCnt=1 step=1000

java -jar ACEIS.jar cqelsCnt=1 csparqlCnt=0 smode=rotation qCnt=10 step=1000

java -jar ACEIS.jar cqelsCnt=1 csparqlCnt=0 smode=rotation qCnt=20 step=1000

java -jar ACEIS.jar cqelsCnt=1 csparqlCnt=0 smode=rotation qCnt=30 step=1000

java -jar ACEIS.jar cqelsCnt=1 csparqlCnt=0 smode=rotation qCnt=40 step=1000

java -jar ACEIS.jar cqelsCnt=1 csparqlCnt=0 smode=rotation qCnt=50 step=1000

java -jar ACEIS.jar cqelsCnt=0 csparqlCnt=1 smode=rotation qCnt=1 step=1000

java -jar ACEIS.jar cqelsCnt=0 csparqlCnt=1 smode=rotation qCnt=10 step=1000

java -jar ACEIS.jar cqelsCnt=0 csparqlCnt=1 smode=rotation qCnt=20 step=1000

java -jar ACEIS.jar cqelsCnt=0 csparqlCnt=1 smode=rotation qCnt=30 step=1000

java -jar ACEIS.jar cqelsCnt=0 csparqlCnt=1 smode=rotation qCnt=40 step=1000

java -jar ACEIS.jar cqelsCnt=0 csparqlCnt=1 smode=rotation qCnt=50 step=1000

java -jar ACEIS.jar cqelsCnt=3 csparqlCnt=0 smode=rotation qCnt=50 step=1000

java -jar ACEIS.jar cqelsCnt=5 csparqlCnt=0 smode=rotation qCnt=50 step=1000

java -jar ACEIS.jar cqelsCnt=7 csparqlCnt=0 smode=rotation qCnt=50 step=1000

java -jar ACEIS.jar cqelsCnt=9 csparqlCnt=0 smode=rotation qCnt=50 step=1000

java -jar ACEIS.jar cqelsCnt=0 csparqlCnt=3 smode=rotation qCnt=50 step=1000

java -jar ACEIS.jar cqelsCnt=0 csparqlCnt=5 smode=rotation qCnt=50 step=1000

java -jar ACEIS.jar cqelsCnt=0 csparqlCnt=7 smode=rotation qCnt=50 step=1000

java -jar ACEIS.jar cqelsCnt=0 csparqlCnt=9 smode=rotation qCnt=50 step=1000

java -jar ACEIS.jar cqelsCnt=0 csparqlCnt=10 smode=rotation qCnt=100 step=3000

java -jar ACEIS.jar cqelsCnt=0 csparqlCnt=10 smode=balancedLatency qCnt=100 step=3000

java -jar ACEIS.jar cqelsCnt=10 csparqlCnt=0 smode=rotation qCnt=100 step=3000

java -jar ACEIS.jar cqelsCnt=10 csparqlCnt=0 smode=balancedLatency qCnt=100 step=3000
