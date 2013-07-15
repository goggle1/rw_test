#!/bin/bash

TEST_NAME="case2"
ROOT_PATH="/media13"
PIECE_NUM=5000
FILE_NUM=15

function test()
{
	MIN_SIZE=$1
	MAX_SIZE=$MIN_SIZE

	echo "MIN_SIZE=${MIN_SIZE}, MAX_SIZE=${MAX_SIZE}"	
	rm -rf ${ROOT_PATH}/dir0/

	LOG_WRITE="./${TEST_NAME}_w_${MIN_SIZE}.log"
	echo `date` > ${LOG_WRITE}
	./rw_test -r ${ROOT_PATH} -m w -i ${MIN_SIZE} -a ${MAX_SIZE} -f ${FILE_NUM} >> ${LOG_WRITE} 2>&1
	RESULT=$?
	echo "RESULT=${RESULT}" >> ${LOG_WRITE}
	echo `date` >> ${LOG_WRITE}

	LOG_READ="./${TEST_NAME}_r_${MIN_SIZE}.log"
	echo `date` > ${LOG_READ}
	./rw_test -r ${ROOT_PATH} -m r -n ${PIECE_NUM} >> ${LOG_READ} 2>&1
	RESULT=$?
	echo "RESULT=${RESULT}" >> ${LOG_READ}
	echo `date` >> ${LOG_READ}
	
}

# 1M, 10M, 100M, 1G(!=1000M)
for base in 1048576 10485760 104857600 1073741824
do

	echo "base=${base}"
	
	for((i=1;i<=9;i=i+1))
	do	
		SIZE=`expr ${base} \* ${i}`
		echo "i=${i}"
		echo "size=${SIZE}"	
		test ${SIZE}
	done
	
done

# 10G
SIZE=10737418240
echo "\n"
echo "size=${SIZE}"
test ${SIZE}

