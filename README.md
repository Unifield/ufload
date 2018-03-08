# ufload
The Unifield Loader

[![Build Status](https://travis-ci.org/Unifield/ufload.svg?branch=master)](https://travis-ci.org/Unifield/ufload)

ufload is targeted at people who load dumps from live Unifield
instances into non-production testing/staging/training
instances. ufload will never leave a non-production database in a
production configuration (it will stomp production passwords and
disconnect from the live sync server).

ufload is 100% Python, and has been tested on Linux and Windows.

ufload can iterate over all the backups in a directory on ownCloud,
and restore the latest backup for each instance. You can limit the
list of instances to be restored.

ufload can make coffee for you in the morning, and then do your dishes.

## Installing on Windows

1. Install Python 2.7 from here: https://www.python.org/downloads/windows/
2. Add this directory to your PATH, separated from previous ones
by a semi-colon:
  * ;C:\Python27;C:\Python27\Scripts
  or with cmd line in windows:
   ```SETX /M PATH "%PATH%;C:\Python27;C:\Python27\Scripts"```
3. In a new CMD.EXE window, type: ```pip install --upgrade ufload```
4. Run ```ufload -h``` to get help.
5. Use Notepad to create a config file. Put the file in the same place CMD.EXE starts from, for example ```d:\Users\jae```. The file should be named ufload.txt. Be careful: notepad.exe will create a file called ufload.txt.txt by default. To avoid this, use "Save as..." and do not include .txt. Put the following into it:
```
[owncloud]
user=username
pw=password for ownCloud
oc=which OC's backups you are using: OCG, OCA, or OCB

[postgres]
db_user=openpg
db_pw=your database password
```
6. Use "dir" to confirm that the file is where you expect it to be, and is named "ufload.txt" and not "ufload.txt.txt".

For each line, put the right thing. If you do not want to put your
password in the file, you can add the ```-pw``` flag to any command,
after ```ufload.exe```.

## Installing on Linux

```sudo pip install --upgrade ufload```

The config file is in $HOME/.ufload

## Upgrading

Use the same command as you used to install it: ```pip install --upgrade ufload```

## Example Commands

To see a list of all backup files for an instance: ```ufload ls -i OCG_HQ```

For all instances, remove the ```-i``` flag.

To load all of the instances from OneDrive: ```ufload restore```

To load the OCG_HQ instance and all the OCG_NE1 instances from OneDrive: ```ufload restore -i OCG_HQ -i OCG_NE1```

To load the OCG_HQ and OCG_NE1 instances from OneDrive and load a sync server: ```ufload restore -i OCG_HQ -i OCG_NE1 -load-sync-server```

## Scheduling ufload in Windows

You can use the Windows Task Scheduler to run ufload in order to update a
sandbox environment every night.

Use the remote option in the [logs] section to arrange for remote logging.

Use a command like this to schedule it once a day: ```schtasks /create /F /TN Ufload /SC DAILY /st 20:00 /tr "cmd.exe /C start /min cmd.exe /C C:\python27\Scripts\ufload restore -load-sync-server"```

Or this to make it run every hour: ```schtasks /create /F /TN Ufload /SC DAILY /RI 60 /st 00:00 /du 24:00 /tr "cmd.exe /C start /min cmd.exe /C C:\python27\Scripts\ufload restore -load-sync-server"```

## Integrating other tools into ufload

Ufload's ```restore``` command has a ```-notify``` flag which will
call a program each time a database is sucessfully loaded. The program
receives the name of the newly loaded database as it's first argument.

For instance, the following script sends e-mail when a backup file is
older than expected:

```
#!/bin/sh

db=$1

# Convert OCG_NE1_COO_20161210_2102 into 20161210
d=`echo $db | perl -F_ -lane 'print $F[-2]'`
limit=`date --date='5 days ago' +%Y%m%d`

if [ "$d" -lt "$limit" ]; then
   echo "Database $db is too old." | mail user@example.org
fi
```

If it was loaded in ```/bin/notify-old-db```, then
```ufload restore -notify /bin/notify-old-db``` will run the script.
