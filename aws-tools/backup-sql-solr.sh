#! /bin/bash

# Rsync all solr files (except statistics, which change too quickly) to an amazon server

LOCKFILE="/var/lock/.amazon.solr.rsync.exclusivelock"
LOGFILE="/opt/dryad/log/rsync-solr-last.log"


# ensure there is only one copy of this script running
lock() {
    exec 200>$LOCKFILE

    flock -n 200 \
        && return 0 \
        || return 1
}
lock || { exit 1; }

echo "perform the rsync"
rm -f $LOGFILE
rsync -rltgoWvO --delete --exclude 'responselogging' --exclude 'statistics' --exclude 'dataoneMNlog' /opt/dryad/solr $HOME/solrBackups >$LOGFILE

echo "aws sync"
aws s3 cp $HOME/databaseBackups/dryadDBlatest.sql s3://dryad-backup/databaseBackups/dryadDBlatest.sql >$LOGFILE
aws s3 sync /opt/dryad/solr s3://dryad-backup/solrBackups/ --delete >$LOGFILE
