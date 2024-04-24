#!/bin/bash

export PATH=".:${PATH}"

VHOST="test_vhost"
QUEUE="test_queue"
DB=$VHOST
TABLE=$QUEUE
MAX_READS=10000
MAX_BULKS=100

list_queue() {
    rabbitmqctl list_queues --vhost $VHOST | grep ^${QUEUE}
}


purge_queue() {
    rabbitmqctl purge_queue -p $VHOST $QUEUE
}


sql_list() {
    psql -d $DB -t  -c "
        SELECT COUNT(*), MIN(message_number), MAX(message_number)
        FROM $TABLE;"
}


sql_flush() {
    psql -d $DB -c "DELETE FROM $TABLE;"
}


sql() {
    if psql -d $DB -t -AF $',' -c "
                SELECT COUNT(*), MIN(message_number), MAX(message_number)
                FROM $TABLE;" | \
            grep "$MAX_READS,1,$MAX_READS" ; then
        psql -d $DB -c "DELETE FROM $TABLE;"
    else
        echo "ERROR IN SQL OUTPUT" >&2
        exit
    fi
}


run_perf() {
    (
        purge_queue 
        sql_flush
        rabby publish $MAX_READS
    ) &> /dev/null

    export PYTHONPERFSUPPORT=1

    time perf record -g -F 999 \
        ./rmq2psql.py --max-bulks $MAX_BULKS --max-reads $MAX_READS

    perf report --stdio > perf.txt

    perf report --hierarchy --sort comm,dso,sample -U
}


run_profile() {
    (
        purge_queue 
        sql_flush
        rabby publish $MAX_READS
    ) > /dev/null

    #running rmq2psql.py via profile_wrapper each time recall itself -- slooow
    cat rmq2psql.py | sed -e 's|@profile_wrapper|@profile|g' > rmq2psql_p.py
    chmod +x rmq2psql_p.py

    time kernprof -lv \
        ./rmq2psql_p.py --max-bulks $MAX_BULKS --max-reads $MAX_READS \
                        --json all --profile $*

    rm rmq2psql_p.py rmq2psql_p.py.lprof
}

