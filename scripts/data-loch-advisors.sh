#!/bin/bash
# Script to upload instructor-advisor mapping and advisor note permissions to data loch (Nessie) S3.

# Make sure the normal shell environment is in place, since it may not be
# when running as a cron job.
source "$HOME/.bash_profile"

cd $( dirname "${BASH_SOURCE[0]}" )/..

LOG=`date +"$PWD/log/data_loch_advisors_%Y-%m-%d.log"`
LOGIT="tee -a $LOG"

# Set Python environment
pyenv activate venv_jonesy

echo | $LOGIT
echo "------------------------------------------" | $LOGIT
echo "`date`: About to run the data loch advisors script..." | $LOGIT

JOB=upload_advisors python jonesy.py |& $LOGIT
