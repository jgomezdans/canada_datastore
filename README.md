# Canada datastore

An example crontab call for the script is:

```bash
SHELL=/bin/bash
BASH_ENV=~/.bashrc_conda
LOC=/home/jose/data/Canada_datastore_2024
EUMETSAT_KEY=wZbhgx5sf1qMYKsCpJ9Bf8lE7Aga
EUMETSAT_SECRET=DrBYHdM3AQwhxFiMaR0MIRbbX48a
QT_QPA_PLATFORM=offscreen
PATH=/bin:/usr/bin:/home/jose/mamba/bin/
MAILTO=jose.gomez-dans@kcl.ac.uk

0       3       *       *       *       conda activate py312 ; get_arable -f /home/jose/data/KCL_Arable/raw_data/ >> /home/jose/data/KCL_Arable/logs/logfile.$(date +\%Y-\%m-\%d).log 2>&1
0 6 * * * conda activate py312 ; get_sentinel3 -f1 ${LOC}/Sentinel3 -ff ${LOC}/FIRMS/ >> ${LOC}/logs/logfile.$(date +\%Y-\%m-\%d-\%H-\%M).log 2>&1
0 12 * * * conda activate py312 ; get_sentinel3 -f1 ${LOC}/Sentinel3 -ff ${LOC}/FIRMS/ >> ${LOC}/logs/logfile.$(date +\%Y-\%m-\%d-\%H-\%M).log 2>&1
45 17 * * * conda activate py312 ; get_sentinel3 -f1 ${LOC}/Sentinel3 -ff ${LOC}/FIRMS/ >> ${LOC}/logs/logfile.$(date +\%Y-\%m-\%d-\%H-\%M).log 2>&1

```