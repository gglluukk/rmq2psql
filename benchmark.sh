#!/bin/bash

. functions.sh

consume() {
    echo
    sum=""
    for i in {1..5} ; do
        (
            purge_queue
            sql_flush
            rabby publish $MAX_READS
        ) > /dev/null

        echo -ne "${*}: "
        value=`( time ${*} > /dev/null ) 2>&1 | \
                grep real | sed -e 's|.*m||' -e 's|,|.|' -e 's|s||'`
        echo "#${i}: ${value}"
        
        sum="$sum + $value"

        sql > /dev/null
    done
    echo Average: `eval "python3 -c 'print(\"{:.3f}\".format((0 $sum) / 5));'"`
}

if [ ! $1 ] ; then
    test="all"
else
    test="$1"
fi

if [ "$test" == "all" ] || [ $test == "json" ] ; then
    for json in msgspec msgspec_struct orjson ujson ; do
        consume rmq2psql.py --json $json --max-bulks $MAX_BULKS
    done
fi

if [ $test == "all" ] || [ $test == "loop" ] ; then
    for loop_type in message_processing queue_iteration_without_timeout \
                 queue_iteration_with_timeout ; do
        consume rmq2psql.py --loop-type $loop_type --max-bulks $MAX_BULKS
    done
fi

if [ $test == "all" ] || [ $test == "go" ] ; then
    consume rmq2psql --max-bulks $MAX_BULKS --max-reads $MAX_READS
fi

