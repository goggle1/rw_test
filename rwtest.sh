#!/bin/bash

TEST_NAME="xiong"
ROOT_PATH="/media13"
PIECE_NUM=10000

for((i=0;1;i=i+1000))
do

INDEX=$i

LOG_WRITE="./${TEST_NAME}_w_${INDEX}.log"
echo `date` > ${LOG_WRITE}
./rw_test -r ${ROOT_PATH} -m w -s $INDEX >> ${LOG_WRITE} 2>&1
RESULT=$?
echo "RESULT=${RESULT}" >> ${LOG_WRITE}
echo `date` >> ${LOG_WRITE}
if [ $RESULT -ne 0 ]; 
then
	break
fi

LOG_READ="./${TEST_NAME}_r_${INDEX}.log"
echo `date` > ${LOG_READ}
./rw_test -r ${ROOT_PATH} -m r -n ${PIECE_NUM} >> ${LOG_READ} 2>&1
RESULT=$?
echo "RESULT=${RESULT}" >> ${LOG_READ}
echo `date` >> ${LOG_READ}
if [ $RESULT -ne 0 ]; 
then
	break
fi

done

