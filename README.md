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
2. Add these directories to your PATH, separated by semi-colons:
  * c:\Python27
  * c:\Python27\Scripts
3. In a new CMD.EXE window, type: ```pip install --upgrade ufload```
4. Run ```ufload -h``` to get help.
5. Use Notepad to create a config file. Put the file in the same place CMD.EXE starts from, for example ```d:\Users\jae```. The file should be named ufload.txt. Put the following into it:
```
[owncloud]
user=username
pw=password for ownCloud
oc=which OC's backups you are using: OCG, OCA, or OCB

[postgres]
db_user=openpg
db_pw=your database password
```

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

To load all of the instances from ownCloud: ```ufload restore```

To load the OCG_HQ instance and all the OCG_NE1 instances from ownCloud: ```ufload restore -i OCG_HQ -i OCG_NE1```

## Scheduling ufload in Windows

You can use the Windows Task Scheduler to run ufload in order to update a
sandbox environment every night.

Use a command like this to schedule it: ```schtasks /create /F /TN Ufload /SC DAILY /st 20:00 /tr "cmd /C C:\python27\Scripts\ufload restore -load-sync-server >> d:\ufload.log 2>&1"```