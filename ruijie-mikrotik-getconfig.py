import threading
import telnetlib
import time
import paramiko
import os, time
import os.path
import psycopg2
import psycopg2.extras
import datetime
import io
import sys
import mysql.connector

#MYSQL
mysqldb = mysql.connector.connect(
        host="zabbix-db-slave.apolloglobal.net",
        user="zabbixreadonly",
        password="ZabbixReadOnly",
        database="zabbix")
# print(mysqldb)

#POSTGRES
postgresdb = psycopg2.connect(
        user="netuser",
    password="admin123",
    host="127.0.0.1",
    port="5433",
    database="netmeister"
)
#print("Connecting to PostgreSQL")
postgres_cursor = postgresdb.cursor()
# print (postgresdb.get_dsn_parameters(), "\n")
postgres_cursor.execute("SELECT version();")
record = postgres_cursor.fetchone()

def hosts (host_id, host_name, host_ip, status, host_group, getconfig):
    postgres_cursor.execute("SELECT host_id from apps_allhosts WHERE apps_allhosts.host_id=%s", (host_id,))
    postgresdb.commit()
    postgres_result= postgres_cursor.fetchall()
    if len(postgres_result)==0:
        postgres_cursor.execute("INSERT into apps_allhosts (host_id, host_name, host_ip, status, host_group, getconfig) VALUES (%s, %s, %s, %s, %s,%s)", (host_id, host_name, host_ip, status, host_group, getconfig))
        postgresdb.commit()
        # print("INSERT ", host_id)
    else:
        postgres_cursor.execute("UPDATE apps_allhosts SET host_ip=%s, host_name=%s, status=%s, host_group=%s, getconfig=%s where apps_allhosts.host_id=%s", (host_ip, host_name, status, host_group, getconfig, host_id))
        postgresdb.commit()
        # print("UPDATE ", host_id)

def save_output (new_output, host_name, filename):
   #output into text file
   today = datetime.datetime.now().strftime('%Y%m%d')
   store_id = host_name.split(' ', 1)[0]
   path = os.environ.get("save_output", "/var/log/psc")
   new_dir = path + '/' + str(today)
   isdir = os.path.isdir(new_dir)
   if isdir is False:
      print("Path does not exist!")
      os.makedirs(path + '/' + str(today))
      with open (path + '/' + str(today)  + '/' +  str(filename) + '.txt', "w") as output:
         output.write(new_output)
   else:
      with open (path + '/' + str(today) + '/' + str(filename) + '.txt', "w") as output:
         output.write(new_output)


def ruijie(host_name, host_id, host_ip):
    today = datetime.datetime.now().strftime('%Y%m%d')
    path = os.environ.get("save_output", "/var/log/psc")
    new_dir = path + '/' + str(today)
    isdir = os.path.isdir(new_dir)

    if isdir is False:
        os.makedirs(path + '/' + str(today))

    # f = open(path + '/' + "store-status" + "-" + str(today) + '.log', "a")
    # store_status = str(datetime.datetime.now().strftime('%Y-%b-%d %H:%M:%S')) + " " + str(host_name) + " " + "START"
    # f.write(store_status + "\n")
    # print ("%s: %s" % (host_name, time.ctime(time.time())))
    success=1

    try:
        tnet_hndl = telnetlib.Telnet(host_ip,23,60)
        tnet_hndl.read_until(b"Username:")
        tnet_hndl.write(b"netadmin" + b"\n")
        tnet_hndl.read_until(b"Password:")
        tnet_hndl.write(b"psc-n3t4dm1n7E4!" + b"\n")
        tnet_hndl.write(b"en" + b"\n")
        tnet_hndl.read_until(b"Password:")
        tnet_hndl.write(b"711-@p01102k17!" + b"\n")
        tnet_hndl.write(b"terminal length 0" + b"\n")
        tnet_hndl.write(b"show run" + b"\n")
        time.sleep(20)
        output = tnet_hndl.read_very_eager()
        tnet_hndl.close()
    except:
        # print("Failed")
        print(host_name, host_ip, "FAILED", sys.exc_info()[0], "occurred.")
        success=0

    if success==1:
        filename=host_name.split(' ', 1)[0] + ".config"
        new_output = output.decode("ascii")
        save_output(new_output, host_name, filename)
        postgres_cursor.execute("UPDATE apps_allhosts SET getconfig=1 WHERE apps_allhosts.host_id=%s" %(host_id))
        postgresdb.commit()
        print (host_name, host_ip, "OK")
    else:
        # ts = str(datetime.datetime.now().strftime('%Y-%b-%d %H:%M:%S'))
        # f.write(ts,host_name,"FAIL")
        filename=host_name.split(' ', 1)[0] + ".fail"
        no_output=""
        save_output(no_output, host_name, filename)
        print (host_name, host_ip, "FAIL")

def mikrotik(host_name, host_id, host_ip):
    # print ("ssh to ",host_ip)
    today = datetime.datetime.now().strftime('%Y%m%d')
    ts = str(datetime.datetime.now().strftime('%Y-%b-%d %H:%M:%S'))
    path = os.environ.get("save_output", "/var/log/psc")
    new_dir = path + '/' + str(today)
    isdir = os.path.isdir(new_dir)
    if isdir is False:
        os.makedirs(path + '/' + str(today))

    # f = open(path + '/' + "store-status" + "-" + str(today) + '.log', "a")
    # store_status = ts + " " + str(host_name) + " " + "START"
    # f.write(store_status + "\n")
    # print ("%s: %s %s" % (host_name, host_ip, time.ctime(time.time())))
    success=1

    try:
        ssh = paramiko.SSHClient()
        ssh.load_system_host_keys()
        # ssh.load_host_keys(os.path.expanduser('/dev/null'))
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(host_ip,
        username="netadmin",
        password="psc-n3t4dm1n7E4!",
        look_for_keys=False, timeout=120 )
        ssh_stdin, ssh_stdout, ssh_stderr = ssh.exec_command("/export")
        output = ssh_stdout.readlines()[1:]
        new_output = ''.join(output)
        ssh.close()
    except:
        # print (host_name, host_ip, "FAILED")
        print(host_name, host_ip, "FAILED", sys.exc_info()[0], "occurred.")
        success=0

    if success == 1:
        filename=host_name.split(' ', 1)[0] + ".config"
        save_output(new_output, host_name, filename)
        postgres_cursor.execute("UPDATE apps_allhosts SET getconfig=1 WHERE apps_allhosts.host_id=%s" %(host_id))
        postgresdb.commit()
        print (host_name, host_ip, "OK")
    else:
        filename=host_name.split(' ', 1)[0] + ".fail"
        no_output=""
        save_output(no_output, host_name, filename)
        print (host_name, host_ip, "FAIL")


def getconfig_all():
   postgres_cursor.execute("SELECT host_id, host_ip, host_name, host_group, getconfig, status from apps_allhosts WHERE status='1' AND getconfig='0'") 
   postgresdb.commit()
   result= postgres_cursor.fetchall()
   rescnt = len(result)
   print ("HOST without CONFIG",rescnt)
   counter=0
   threads = list()

   for x in result:
       host_id=x[0]
       host_ip=x[1]
       host_name=x[2]
       host_group=x[3]
       counter += 1
       today = datetime.datetime.now().strftime('%b-%d-%Y %H:%M:%S')
       # print ("GETCONFIG ",host_name,host_group,counter)
       if host_group == "MIKROTIK":
           x = threading.Thread(target=mikrotik, args=(host_name, host_id, host_ip),daemon=True)
           threads.append(x)
           x.start()
       if host_group == "RUIJIE":
           x = threading.Thread(target=ruijie, args=(host_name, host_id, host_ip),daemon=True)
           threads.append(x)
           x.start()
       if counter == 10:
           # print("SLEEPING...")
           time.sleep(10)
           #for index, thread in enumerate(threads):
           #    thread.join()
           counter=0;
           threads = list()


def hosts_inactive():
    # set all hosts to inactive
    postgres_cursor.execute("UPDATE apps_allhosts SET status=0")
    postgresdb.commit()

def zabbix_update_hosts():
    #GET DATA from zabbix, hostid, hostname, store_status, hostip
    mysql_cursor= mysqldb.cursor()
    mysql_cursor.execute("SELECT h.hostid, h.name, hi.poc_2_phone_a, i.ip, hg.groupid FROM hosts h, host_inventory hi, hosts_groups hg, interface i WHERE h.hostid=hi.hostid AND h.hostid=i.hostid AND h.hostid=hg.hostid AND i.useip=1 AND i.type=2 AND hg.groupid IN (117,118)")
    mysql_result = mysql_cursor.fetchall()
    #Store data from local database:
    for x in mysql_result:
        host_id = x[0]
        host_name = x[1]
        store_status = x[2]
        host_ip = x[3]
        group_id = x[4]
        # print(host_id, host_name, store_status, host_ip, group_id)

        if group_id==117 and store_status=='active':
            host_group = "MIKROTIK"
            status='1'
            getconfig = '0'
            # print("Executed 117")
            hosts(host_id, host_name, host_ip, status, host_group, getconfig)
        if group_id==118  and store_status=='active':
            status='1'
            host_group="RUIJIE"
            getconfig='0'
            # print("Execute 118")
            hosts(host_id, host_name, host_ip, status, host_group, getconfig)

    print("UPDATE HOSTS - COMPLETE")

hosts_inactive()
zabbix_update_hosts()

loops = 0
while loops != 10:
    loops +=1;
    print("LOOP ",loops)
    getconfig_all()
    time.sleep(20)



