#!/bin/bash
# Script to upload incremental course and enrollment updates to data loch (Nessie) S3.

# Make sure the normal shell environment is in place, since it may not be
# when running as a cron job.
source "$HOME/.bash_profile"

cd $( dirname "${BASH_SOURCE[0]}" )/..

LOG=`date +"$PWD/log/data_loch_recent_refresh_%Y-%m-%d.log"`
LOGIT="tee -a $LOG"

# Set Python environment
pyenv activate venv_jonesy

echo | $LOGIT
echo "------------------------------------------" | $LOGIT
echo "`date`: About to run the data loch recent refresh script..." | $LOGIT

JOB=upload_recent_refresh python jonesy.py |& $LOGIT
