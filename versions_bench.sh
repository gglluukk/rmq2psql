#!/bin/bash

. functions.sh

consume() {
    sum=""
    for i in {1..5} ; do
        (
            purge_queue
            sql_flush
            rabby publish $MAX_READS
        ) > /dev/null

        value=`( time ${*} > /dev/null ) 2>&1 | \
                grep real | sed -e 's|.*m||' -e 's|,|.|' -e 's|s||'`
        
        sum="$sum + $value"
    done
    echo `eval "python3 -c 'print(\"{:.3f}\".format((0 $sum) / 5));'"`
}

AIO_PIKA_VERSIONS="9.4.1 9.4.0 9.3.1 9.3.0 9.2.3 9.2.2 9.2.1 9.2.0 9.1.5 9.1.4 9.1.3 9.1.2 9.1.0 9.0.7 9.0.6 9.0.5 9.0.4 9.0.3 9.0.2 9.0.1 9.0.0 8.3.0 8.2.5 8.2.4 8.2.3 8.2.2 8.2.1 8.2.0 8.1.1 8.1.0 8.0.3 8.0.2 8.0.0 7.2.0 7.1.2 7.1.1 7.1.0 7.0.1 7.0.0 6.8.2 6.8.1 6.8.0 6.7.1 6.7.0 6.6.1 6.6.0 6.5.3 6.5.2 6.5.1 6.5.0 6.4.3 6.4.2 6.4.1 6.4.0 6.3.0 6.1.3 6.1.2 6.1.1 6.1.0 6.0.1"


for version in $AIO_PIKA_VERSIONS ; do
    echo -ne "aio-pika ${version}: "
    (
        pip uninstall -y aio-pika --break-system-packages
        pip install aio-pika==${version} --break-system-packages
    ) &> /dev/null
    consume rmq2psql.py --max-bulks $MAX_BULKS --max-reads $MAX_READS
done

