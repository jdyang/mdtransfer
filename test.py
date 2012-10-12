# -*- coding: utf-8 -*-

import PyQt4, PyQt4.QtGui,sys

from PyQt4.QtGui import QDialog
from PyQt4.QtCore import pyqtSignature

from util import Constant
import string
import pymysql
import datetime

class TestA():
    """
    Class documentation goes here.
    """
    def __init__(self, parent = None):
        """
        Constructor
        """
        #print u"in init"
    
    def say_hi(self):
        """
        Slot documentation goes here.
        """
        #print u"hiResGlobalPos"
        
class TestMysql():
    """just a test"""
    
    name = u"TestMysql"
    
    
    
    def test_date(self):
            for month in range(4,5):
                for day in range(1,31):
                    theday = datetime.datetime(2011,month,day)
                    print theday
            self.do_things("1",  2)
            
    def do_things(self, a,  b):
        print 'hello' ,  a,  b      

    def test_connection(self):
        conn = pymysql.connect(host=Constant.mysqlHost, port=3306, user='mdwriter', passwd='mdwriter', 
                               db=Constant.dbName)
        cur = conn.cursor()
 
        for contra in Constant.preContractID:
            tableName = "%" + contra + "%"
            sqlCmd = "show tables like " + "'" + tableName + "'"
#            print sqlCmd
            cur.execute(sqlCmd)
            tables = cur.fetchall()
            
            for t in tables:
                print t
      
        cur.close()
        conn.close()
    
    def test_toy(self):
        str1 = "2012-12-2"
        str2 = "2012-12-09"
        
        print str1 > str2 
        
        str0 = "cffe_if1203"
        
        myDate = str0.strip("cffe_if")
        myYear = myDate[0:2]
        myMon = myDate[2:]
        print myYear ,  myMon
        
        fullDateMon = "20" + myYear + "-" + myMon 
        
        for i in range(1, 32):
            if (i < 10):
                r = "0" + str(i)
            else:
                r = "" + str(i)
            print fullDateMon + "-" + r
        
    def test_insert(self):
        conn = pymysql.connect(host=Constant.mysqlHost, port=3306, user='mdwriter', passwd='mdwriter', 
                               db=Constant.dbName)
        cur = conn.cursor()
        
        cur.execute("SELECT * FROM cffe_if1203 WHERE Tm BETWEEN \
        '2011-12-26 00:00:00' AND '2011-12-27 00:00:00' ORDER BY Tm DESC LIMIT 0,1")
        
        myResult = cur.fetchone()
        print myResult

        if myResult[10] ==0:
            print "0"
        else:
            print myResult[10]
        date1 =  "'" + str(myResult[17]) + "'"
        date2 =  "'" + str(myResult[17]).split(' ')[0] + "'"
        value0 = myResult[2]
        value1 = str(myResult[3:17]).replace("None",  "0").replace("L", "")[1:-1]
        value2 = str(myResult[18:]).replace("None",  "0").replace("L", "")[1:-1]
        myValue = "'" + value0 + "' ," +value1 +  " ," + date1 + ", " + date2 + " ," + value2
#        print myValue
        
        insertSql = "INSERT INTO `mdata`.`contract_daily_view`" + str(Constant.tableStruct).replace("'", "`") + \
        " values (" + myValue + ")"
        
        print insertSql
        
#        cur.execute(insertSql)
            
        
        cur.close()
        conn.close()
        
if __name__ == "__main__":
    
    m = TestMysql()
    #m.test_connection()
    
    print m.name
    m.test_date()
    #m.test_toy()
    
    m.test_insert()
