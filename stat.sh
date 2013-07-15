#!/bin/bash

TEST_NAME="xiong"
ROOT_PATH="/media22"
PIECE_NUM=10000
LOG_OUTPUT="./stat.log"
LOG_CSV="./stat.csv"

for((i=0;i<=88000;i=i+1000))
do

INDEX=$i
LOG_READ="./${TEST_NAME}_r_${INDEX}.log"
TEMP_STR=`grep stat_threads ${LOG_READ}`
echo "${INDEX} ${TEMP_STR}" >> ${LOG_OUTPUT}

done

awk -F"[ =]" '{print $1","$4","$6","$8}' ${LOG_OUTPUT} > ${LOG_CSV}

