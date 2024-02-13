# version 0.280

* restore option "-no-login" to no trigger upgrade

# version 0.278

* option -jobs for pg\_restore -j
* disable UniData pull cron
* store orginal value of sync\_client\_sync\_server\_connection.automatic\_patching in ufload\_automatic\_patching\_prod\_value column

# version 0.276

* replace deletegroups by hidegroups
* manage OneDrive timeout
* fix "list of instance to be restored"
* sync server configuration: set sync password

# version 0.275

* bug fixing on traceback

# version 0.274

* *adminpw* default value removed: must be defined explicitly on the command line or in the config file
* *createusers* new pattern to set the password
* *exclude* accepts a comma separated list of dbs
* new options: *deletegroups*, *logo*, *banner*

