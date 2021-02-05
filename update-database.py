import mysql.connector
import psycopg2
import psycopg2.extras
import paramiko
import telnetlib
import os, time
import datetime
from pythonping import ping

#MYSQL
mysqldb = mysql.connector.connect(
        host="zabbix-db-slave.apolloglobal.net",
        user="zabbixreadonly",
        password="ZabbixReadOnly",
        database="zabbix")
print(mysqldb)

#POSTGRES
#Postgres to connect to local database
postgresdb = psycopg2.connect(
        user="netuser",
    password="admin123",
    host="127.0.0.1",
    port="5433",
    database="netmeister"
)
#print("Connecting to PostgreSQL")
postgres_cursor = postgresdb.cursor()
print (postgresdb.get_dsn_parameters(), "\n")
postgres_cursor.execute("SELECT version();")
record = postgres_cursor.fetchone()

def hosts (host_id, host_name, host_ip, status, host_group, getconfig):
    postgres_cursor.execute("SELECT host_id from apps_allhosts WHERE apps_allhosts.host_id=%s", (host_id,))
    postgresdb.commit()
    postgres_result= postgres_cursor.fetchall()
    if len(postgres_result)==0:
        postgres_cursor.execute("INSERT into apps_allhosts (host_id, host_name, host_ip, status, host_group, getconfig) VALUES (%s, %s, %s, %s, %s,%s)", (host_id, host_name, host_ip, status, host_group, getconfig))
        postgresdb.commit()
        print("INSERT ", host_id)
    else:
        postgres_cursor.execute("UPDATE apps_allhosts SET host_ip=%s, host_name=%s, status=%s, host_group=%s, getconfig=%s where apps_allhosts.host_id=%s", (host_ip, host_name, status, host_group, getconfig, host_id))
        postgresdb.commit()
        print("UPDATE ", host_id)

# set all hosts to inactive
postgres_cursor.execute("UPDATE apps_allhosts SET status=0")
postgresdb.commit()

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
        print(host_id, host_name, store_status, host_ip, group_id)

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

print("Done!")

