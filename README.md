# MariaDB-Import
Import data from Oracle to MariaDB using Python 

# mariadb-import-tool.py 
-> it contains a python script which takes the exported data from oracle and import into mariadb

-> it prompts for the username, password, port, dbname etc before importing the data 

# import_template 
-> it conatins mysqlimport command using which data is imported

-> it fetches the username, password, port, dbname etc from the configuration file in the hierarchy, and this is done with python script created

# to run the script 
-> python2 mariadb-import-tool.py

-> tool uses python v2 and dependencies

# hierarchy :
-> bin, conf, logs, lib etc directories are created for executing the tool
