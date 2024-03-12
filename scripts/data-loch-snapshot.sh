#!/bin/bash
# Script to upload course, enrollment, and user attribute snapshots to data loch (Nessie) S3.

# Make sure the normal shell environment is in place, since it may not be
# when running as a cron job.
source "$HOME/.bash_profile"

cd $( dirname "${BASH_SOURCE[0]}" )/..

LOG=`date +"$PWD/log/data_loch_snapshot_%Y-%m-%d.log"`
LOGIT="tee -a $LOG"

# Set Python environment
pyenv activate venv_jonesy

echo | $LOGIT
echo "------------------------------------------" | $LOGIT
echo "`date`: About to run the data loch snapshot script..." | $LOGIT

JOB=upload_snapshot python jonesy.py |& $LOGIT
