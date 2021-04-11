#!/usr/bin/python2

import time
import logging
import os
import ConfigParser
import csv
from subprocess import Popen, PIPE


try:
    cf = ConfigParser.ConfigParser()
    cf.read('../conf/config.ini')
except Exception as e:
    print 'Error Reading Configuration File....Exiting the tool'
    print e
    exit()



username =  cf.get('import', 'imp_db_usrname')
passwd = cf.get('import', 'imp_db_password')
ip = cf.get('import', 'imp_dbip')
port = cf.get('import', 'imp_dbport')
dbname = cf.get('import', 'imp_dbname')
csvfile = []
lines_in_db1 = []
lines_in_db2 = []
where_condition = []
cf_list = cf.get('sequence', 'import_seq').split(',')
cf_seq_list = cf.get('sequence', 'sequence_key').split(',')
host1 = cf.get('import', 'HOST_1')
host2 = cf.get('import', 'HOST_2')
cf_template = cf.get('post_import', 'template').split(',')



millis = int(round(time.time() * 1000))
logDirname = '../log_import/logs_'+str(millis)
print '***************************************************************'
print 'Log Execution Directory is at path :',logDirname
print '***************************************************************'
os.mkdir(logDirname)



def check_csv_files(cf, csvfile,i):
    filename = i+'.csv'
    filedir = cf.get('import', 'imp_input_dir')
    filepath = os.path.join(filedir,filename)
    csvfile.append(filepath)
    isfile = os.path.isfile(filepath)
    with open(logDirname+'/execution.log', 'a') as log_f:
        if isfile == True:
            print >> log_f,i,'Table Processing Started'
            print i,'Table Processing Started'
        else:
            print >> log_f,filename,'File not in Sequence'
            print filename,'File not in Sequence'




def count_lines(csvfile, i):
    input_file = open(csvfile[i], 'r+')
    reader_file = csv.reader(input_file)
    rowcount = len(list(reader_file))
    with open(logDirname+'/execution.log', 'a') as log_f:
        print >> log_f,'Number of Records in CSV File to Import:',rowcount
        print 'Number of Records in CSV File to Import:',rowcount



def login_db(sqlcmd, connstr, lines_in_db):
    y = []
    try:
        session = Popen([connstr], shell=True, stdin=PIPE, stdout=PIPE, stderr=PIPE)
        session.stdin.write(sqlcmd)
        result,error = session.communicate()
       #print 'SQL Query Result is:',result
        print error
        with open(logDirname+'/execution.log', 'a') as log_f:
            print >> log_f,error
        y = result.split("\n")
        line = y[1]
        lines_in_db.append(line)
    except Exception as e:
        with open(logDirname+'/execution.log', 'a') as log_f:
            print >> log_f,'Error Establishing Connection..\n(OR) Executing SQL command:',sqlcmd,'\nExiting the tool'
            print 'Error Establishing Connection..\n(OR) Executing SQL command:',sqlcmd,'\nExiting the tool'
            exit()


def import_data(username, passwd, ip, port, dbname, csvfile, i):
    with open('../log_import/import_command','w+') as file1:
        file1.truncate(0)
        with open('../conf/import_template', 'r+') as file2:
            filedata = file2.read()
            filedata = filedata.replace('<DB username>', username)
            filedata = filedata.replace('<DB password>', passwd)
            filedata = filedata.replace('<DB Port>', port)
            filedata = filedata.replace('<DB IP>', ip)
            filedata = filedata.replace('<DB Name>', dbname)
            filedata = filedata.replace('<CSV File>', csvfile[i])
            file1.write(filedata)
    with open(logDirname+'/execution.log', 'a') as log_f:
        print >> log_f,'Importing data...displaying connection and debug-info'
        print 'Importing data...displaying connection and debug-info'
    x = "bash ../log_import/import_command >> {0}/execution.log".format(logDirname)
    x = os.system(x)
    y = "{0} {1}".format('rm', '../log_import/import_command')
    y = os.system(y)


for i in cf_list:
    value = cf.get('Where_Clause', i)
    where_condition.append(value)

p=0
for i,j in zip(cf_list,where_condition):
    check_csv_files(cf, csvfile,i)
    count_lines(csvfile, p)
    sqlcmd = 'select count(*) from ' + i + ' ' + j + ' ;'
    connstr = 'mysql ' + '-h ' + ip + ' -u ' + username + ' -p' + passwd + ' ' + dbname
    login_db(sqlcmd, connstr, lines_in_db1)
    with open(logDirname+'/execution.log', 'a') as log_f:
        print >> log_f,'Number of Records before Import:',lines_in_db1[p]
        print 'Number of Records before Import:',lines_in_db1[p]
    import_data(username, passwd, ip, port, dbname, csvfile, p)
    login_db(sqlcmd, connstr, lines_in_db2)
    with open(logDirname+'/execution.log', 'a') as log_f:
        print >> log_f,'Records has been Successfully Imported'
        print 'Records has been Successfully Imported'
    with open(logDirname+'/execution.log', 'a') as log_f:
        print >> log_f,'Number of Records after Import:',lines_in_db2[p]
        print 'Number of Records after Import:',lines_in_db2[p]
        if (int(lines_in_db2[p]) - int(lines_in_db1[p]) == len(list(csv.reader(open(csvfile[p]))))):
            pass
        else:
            print >> log_f, 'Number of Records and Lines Mismatched'
            print 'Number of Records and Lines Mismatched'
        
        print >> log_f, '***************************************************************'
        print '***************************************************************'
        p=p+1

lines_in_db = []
def login_db(max_cmd, connstr, lines_in_db):
    y = []
    try:
        session = Popen([connstr], shell=True, stdin=PIPE, stdout=PIPE, stderr=PIPE)
        session.stdin.write(max_cmd)
        result,error = session.communicate()
        #print 'SQL Query Result is:',result
        print error
        with open(logDirname+'/execution.log', 'a') as log_f:
            print >> log_f,error
        y = result.split("\n")
        line = y[1]
        lines_in_db.append(line)
    except Exception as e:
        with open(logDirname+'/execution.log', 'a') as log_f:
            print >> log_f,'Error Establishing Connection..\n(OR) Executing SQL command:',max_cmd,'\nExiting the tool'
            print 'Error Establishing Connection..\n(OR) Executing SQL command:',max_cmd,'\nExiting the tool'
            exit()

temp_list = []
for i,j in zip(cf_list, cf_seq_list):
    if (str(j) == ""):
        continue
    else:
        max_cmd = 'select max(' + j + ')' + ' from ' + i + ' ;'
        temp_list.append(i)
        connstr = 'mysql ' + '-h ' + ip + ' -u ' + username + ' -p' + passwd + ' ' + dbname
        login_db(max_cmd, connstr, lines_in_db)

temp=100
new_list = []
for val in lines_in_db:
    cnt = int(val)+temp
    new_list.append(cnt)
output_list = []

def login_db(seq_cmd, connstr, output_list):
    y = []
    try:
        session = Popen([connstr], shell=True, stdin=PIPE, stdout=PIPE, stderr=PIPE)
        session.stdin.write(seq_cmd)
        result,error = session.communicate()
        #print 'SQL Query Result is:',result
        print error
        with open(logDirname+'/execution.log', 'a') as log_f:
            print >> log_f,error
        y = result.split("\n")
        line = y[1]
        output_list.append(line)
    except Exception as e:
        with open(logDirname+'/execution.log', 'a') as log_f:
            print >> log_f,'Error Establishing Connection..\n(OR) Executing SQL command:',seq_cmd,'\nExiting the tool'
            print 'Error Establishing Connection..\n(OR) Executing SQL command:',seq_cmd,'\nExiting the tool'
            exit()


for i,j in zip(temp_list,new_list):
    connstr = 'mysql ' + '-h ' + ip + ' -u ' + username + ' -p' + passwd + ' ' + dbname
    seq_cmd = 'select setval(' + i +'_SEQUENCE, ' + str(j) + ', true) ;'
    login_db(seq_cmd, connstr, output_list)
    with open(logDirname+'/execution.log', 'a') as log_f:
        print 'New Sequence Header for',i,'Table:',j
        print >> log_f,'New Sequence Header for',i,'Table:',j
        print >> log_f, '***************************************************************'
        print '***************************************************************'



conn_string = 'mysql ' + '-h ' + ip + ' -u ' + username + ' -p' + passwd + ' ' + dbname
def after_import(host1,host2,conn_string,i):
    with open('../log_import/final_command','w+') as file1:
        file1.truncate(0)
        with open('../lib/'+i, 'r+') as file2:
            filedata = file2.read()
            filedata = filedata.replace('<HOST_1>', host1)
            filedata = filedata.replace('<HOST_2>', host2)
            file1.write(filedata)
        cmd = filedata
        try:
            session1 = Popen([conn_string], shell=True, stdin=PIPE, stdout=PIPE, stderr=PIPE)
            session1.stdin.write(cmd)
            result,error = session1.communicate()
            #print 'SQL Query Result is:',result
            print error
            with open(logDirname+'/execution.log', 'a') as log_f:
                print >> log_f,error
        except Exception as e:
            with open(logDirname+'/execution.log', 'a') as log_f:
                print >> log_f,'Error Establishing Connection..\n(OR) Executing SQL command:',cmd,'\nExiting the tool'
                print 'Error Establishing Connection..\n(OR) Executing SQL command:',cmd,'\nExiting the tool'
                exit()
    run = "{0} {1}".format('rm', '../log_import/final_command')
    run = os.system(run)



for i in cf_template:
    after_import(host1,host2,conn_string,i)

