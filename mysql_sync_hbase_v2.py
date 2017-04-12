#!/usr/bin/python
# encoding=utf-8
# Filename: mysqltest
 
import MySQLdb as dbi
import threading
import types
import json
import collections
import datetime
import sys
from thrift.transport import TSocket
from thrift.protocol import TBinaryProtocol
sys.path.append("/usr/lib/python2.6/site-packages/hbase")
import Hbase
from ttypes import *
from apscheduler.scheduler import Scheduler
import logging


#noinsertid 数组用来计数,超过三次就停止更新
noinsertid = []

def insert_hbase_thread(row, column, value):
        try:
            transport = TSocket.TSocket('10.0.1.93', 9090)
            protocol = TBinaryProtocol.TBinaryProtocol(transport)
            client = Hbase.Client(protocol)
            transport.open()
            mutations = [Mutation(column="data:"+column, value=value)]
            client.mutateRow('for_moni_mysql', row, mutations, None)
            transport.close()
        except:
            print 'Error\n'

def insert_hbase(val_even):
    try:
            #print val_even
            idnum = str(val_even["id"])
            col = ['ip_addr', 'qps_all', 'tps_iud', 'tps_Com_rol', 'qps_s', 'qps_i', 'qps_u', 'qps_d', 'threads_conn', 'threads_run', 'cpu_used', 'mem_used', 'date_time']
            for collist in col:
                threads = []
                collistvar = str(val_even[collist])
                 # insert_tb( idnum,collist, str(vals[collist]))
                th = threading.Thread(target=insert_hbase_thread, args=(idnum, collist, collistvar))
                print "for_moni_mysql %s data:%s %s" % (idnum, collist, collistvar)
                th.start()
                threads.append(th)
            print "程序开始运行%s" % datetime.datetime.now()
            #等待线程运行完毕
            for th in threads:
                th.join()
            print "程序结束运行%s" % datetime.datetime.now()

    except:
        print 'Error\n'

def insert_tb(inid):

        try:
            conn = dbi.connect(host="10.0.1.169", user='tripb2bdba', passwd='Tripb2b.com', db='operation', port=3306, charset='utf8')
            cur = conn.cursor()
            count = cur.execute('select id,ip_addr,qps_all,tps_iud,tps_Com_rol,qps_s, qps_i,qps_u,qps_d,threads_conn,threads_run,cpu_used,mem_used,date_time from tb_mysql_moni where id = %s', inid)
            print count
            #result = cur.fetchmany()
            result = cur.fetchall()

            for row in result:
                print row
               # t = [row.id, row.ip_addr, row.qps_all, row.tps_iud, row.tps_Com_rol, row.qps_s, row.qps_i, row.qps_u,row.qps_d, row.threads_conn, row.threads_run, row.cpu_used, row.mem_used, row.date_time]
                rowarray_list = {'id': str(row[0]), 'ip_addr': str(row[1]), 'qps_all': str(row[2]), 'tps_iud': str(row[3]), 'tps_Com_rol': str(row[4]), 'qps_s': str(row[5]), 'qps_i': str(row[6]), 'qps_u': str(row[7]),
                                 'qps_d': str(row[8]), 'threads_conn': str(row[9]), 'threads_run': str(row[10]), 'cpu_used': str(row[11]), 'mem_used': str(row[12]), 'date_time': str(row[13])}
                print rowarray_list
                insert_hbase(rowarray_list)
                cur.close()
                conn.close()

        except dbi.Error, e:
            print 'Mysql error %d:%s' % (e.args[0], e.args[1])

def select_tb(tableName, rowKey):

    try:
        transport = TSocket.TSocket('10.0.1.93', 9090)
        protocol = TBinaryProtocol.TBinaryProtocol(transport)
        client = Hbase.Client(protocol)
        transport.open()
        result = client.getRow(tableName, rowKey, None)
        #print client.getTableNames()
        if len(result):
            print result
            for r in result:
                print 'the row is ', r.row
                #noinsertid 没次找到数据，即将对应ID填入数组。
            noinsertid.append(rowKey)

        else:
            print 'the row is empty'
            insert_tb(rowKey)
            #noinsertid 计数每次ID有数据即清空数组.
            del noinsertid[:]
        transport.close()
    except:
        print 'Error\n'

def main():

    try:
        conn = dbi.connect(host="10.0.1.169", user='******', passwd='*****', db='operation', port=3306, charset='utf8')
        cur = conn.cursor()
        count = cur.execute('select id from tb_mysql_moni order by id DESC limit 1440')
        print count
        result = cur.fetchall()
        for tr_num in result:
            #print tr_num[0]
            numid = str(tr_num[0])
            print numid
            select_tb('for_moni_mysql', numid)
            if len(noinsertid) > 3:
                del noinsertid[:]
                print 'hbase have last 3 record,do not repeat insert '
                cur.close()
                conn.close()
                break
        del noinsertid[:]
        cur.close()
        conn.close()
    except dbi.Error, e:
        print 'Mysql error'
        print e
        #print 'Mysql error %d:%s' % (e.args[0], e.args[1])

#main()
#select_tb('for_moni_mysql', '1246998', 'cf:a')
if __name__ == '__main__':
    sched = Scheduler()
    sched.daemonic = False
# sched.add_cron_job(job_function,day_of_week='mon-fri', hour='*', minute='0-59',second='*/5')
    sched.add_cron_job(main, day_of_week='*', hour='*', minute='*', second='59')
    logging.basicConfig()
    sched.start()
    #main()
