# -*- coding: utf-8 -*-

import PyQt4, PyQt4.QtGui,sys

from PyQt4.QtGui import QDialog
from PyQt4.QtCore import pyqtSignature

from util import Constant
import string
import pymysql, datetime, time,  re


class PreProcess():
    """ preprocess the market data, calculate the nessessary information"""
    
    conn = None
    cur = None
    
    def __init__(self):
       self.conn = None
       self.cur = None
       
    def connectToMysql(self):
        self.conn = pymysql.connect(host=Constant.mysqlHost, port=3306, user='mdwriter', passwd='mdwriter', 
                               db=Constant.dbName)
        self.cur = self.conn.cursor()
    
    def close(self):
        self.cur.close()
        self.conn.close()
        
    
    def calculateHistoryMaxV(self):
        """process the history market data then calculate the maxV of every contract in everyday"""
        #step 1, get valid tables
        #!be careful to choose the real task table instead of the testing one
        for contra in Constant.preContractIDForTest:
            msg("start dealing with the contract : " + contra)
            tableNameLike = "%" + contra.lower() + "%"
            sqlCmd = "show tables like " + "'" + tableNameLike + "'"

            self.cur.execute(sqlCmd)
            tables = self.cur.fetchall()

            #step 2, for every table, to calculate the maxV of everyday
            for tmpT in tables:
                tableName = tmpT[0]
                msg('begin processing table : ' + tableName)
                myDate = str(tableName).strip(contra.lower())
                
                #to omit none-mdata tables
                regex=u"^\d+$"
                if re.match(regex, myDate) == None:
                    msg('invalid tableName : ' + tableName + " passed")
                    continue
                myYear = myDate[0:2]
                myMon = myDate[2:]
                
                #to omit the table like %efp
                if (len(myMon) < 2):
                    msg('invalid tableName : ' + tableName + " passed")
                    continue
        
                fullDateMon = "20" + myYear + "-" + myMon 
                

        
                #step 3, traverse every day about 1.6w record/day
                
                #step 3.1 get the traverse date interval
                self.cur.execute("SELECT Tm FROM %s WHERE id = (SELECT MAX(ID) FROM %s)" % (tableName,  tableName))
                try:
                    maxDate = str(self.cur.fetchone()[0]).split(' ')[0]
                except TypeError, e:
                    msg("no result in " + tableName)
                    continue
                self.cur.execute("SELECT Tm FROM %s WHERE id = (SELECT MIN(ID) FROM %s)" % (tableName,  tableName))
                try:
                    minDate = str(self.cur.fetchone()[0]).split(' ')[0]
                except TypeError, e:
                    msg("no result in " + tableName)
                    continue

                
                #step 3.2 traverse the very date
                minDateList = minDate.split("-")
                maxDateList = maxDate.split("-")

                digitMinYear = int(minDateList[0])
                digitMaxYear = int(maxDateList[0])
                
                for year in range(digitMinYear,  digitMaxYear + 1):
                    if (year == digitMinYear):
                        for month in range(int(minDateList[1]),  13):
                            for day in range(1,  32):
                                try:
                                    leftParenthesis = datetime.datetime(year,month,day, 0, 0, 0)
                                    rightParenthesis = datetime.datetime(year,month,day, 23, 59, 59)
                                except ValueError, e:
                                    continue
                                #step 3.2.1 deal with the very day
                                self.dealWithTheDay(leftParenthesis,  rightParenthesis, tableName,  contra.lower())
                    elif (year == digitMaxYear):
                        for month in range(1,  int(maxDateList[1]) + 1):
                            for day in range(1,  32):
                                try:
                                    leftParenthesis = datetime.datetime(year,month,day, 0, 0, 0)
                                    rightParenthesis = datetime.datetime(year,month,day, 23, 59, 59)
                                except ValueError, e:
                                    continue
                                #step 3.2.1 deal with the very day
                                self.dealWithTheDay(leftParenthesis,  rightParenthesis, tableName,  contra.lower())
                                
                    else:
                        for month in range(1,  13):
                            for day in range(1,  32):
                                try:
                                    leftParenthesis = datetime.datetime(year,month,day, 0, 0, 0)
                                    rightParenthesis = datetime.datetime(year,month,day, 23, 59, 59)
                                except ValueError, e:
                                    continue
                                #step 3.2.1 deal with the very day
                                self.dealWithTheDay(leftParenthesis,  rightParenthesis,  tableName, contra.lower())
                        
                        
                msg('finish table: ' + tableName)
                
            msg("finish contract : " + contra)
        
    def dealWithTheDay(self,  leftParenthesis,  rightParenthesis, tableName,  contra):
        self.cur.execute("SELECT * FROM %s WHERE Tm BETWEEN \
        %s AND %s ORDER BY Tm DESC LIMIT 0,1"%(tableName, "'" + str(leftParenthesis) + "'",  "'" +str(rightParenthesis) + "'"))
        myResult = self.cur.fetchone()
        if myResult == None:
            return
        #need check maxV != 0
        if myResult[10] == 0:
            return
            
        date1 =  "'" + str(myResult[17]) + "'"
        date2 =  "'" + str(myResult[17]).split(' ')[0] + "'"
        value0 = myResult[2]
        value1 = str(myResult[3:17]).replace("None",  "0").replace("L", "")[1:-1]
        value2 = str(myResult[18:]).replace("None",  "0").replace("L", "")[1:-1]
        myValue = "'" + value0 + "' ," +value1 +  " ," + date1 + ", " + date2 + " ," + value2

        #! dbName hardCode here be careful
        insertSql = "INSERT INTO `mdata`.`" + contra + "_daily_view`" + str(Constant.tableStruct).replace("'", "`") + \
        " values (" + myValue + ")" + " ON DUPLICATE KEY UPDATE Tm =" + date1
        
        #insert the daily statistics into daily_view
        self.cur.execute(insertSql)
    
    def findTheMainContractOfDay(self,  contra, date):
        
        tableName = contra + "_daily_view"
        targetTableName = contra + "_main_contracts"
        strDate = "'" + str(date) + "'"
        
        self.cur.execute("SELECT ConstrId, MaxV, LatestOI FROM  %s WHERE DATE=%s ORDER BY MaxV DESC LIMIT 0, 4" % (tableName,  strDate))
        
        recCount = 0
        recValue = [["",  0,  0], ["",  0,  0], ["",  0,  0], ["",  0,  0]]
        myValue = strDate 
        for r in self.cur.fetchall():
            recValue[recCount] = list(r)
#            print r, recCount,  recValue[recCount]
            recCount = recCount + 1
            
        if recCount == 0:
            return
        
        myValue = myValue + "," + "'" + recValue[0][0] + "', " + str(recValue[0][1:]).replace("None",  "0").replace("L",  "")[1:-1] \
                    + "," + "'" + recValue[1][0] + "', " + str(recValue[1][1:]).replace("None",  "0").replace("L",  "")[1:-1] \
                    + "," + "'" + recValue[2][0] + "', " + str(recValue[2][1:]).replace("None",  "0").replace("L",  "")[1:-1] \
                    + "," + "'" + recValue[3][0] + "', " + str(recValue[3][1:]).replace("None",  "0").replace("L",  "")[1:-1] 
       
        #! dbName hardCode here be careful
        insertSql = "INSERT INTO `mdata`.`" + targetTableName + "`" + str(Constant.mainContractTableStruct).replace("'", "`") + \
        " values (" + myValue + ")" + " ON DUPLICATE KEY UPDATE Date =" + strDate
        
        #insert the daily statistics into main contract table
        self.cur.execute(insertSql)

    def findTheMainContractOfVariety(self,  contra):
        tableName = contra + "_daily_view"
        targetTableName = contra + "_main_contracts"
        #! date region should be user's input when this script used for daily build
        self.cur.execute("SELECT Date FROM %s WHERE id = (SELECT MAX(ID) FROM %s)" % (tableName,  tableName))
        try:
            maxDate = str(self.cur.fetchone()[0])
        except TypeError, e:
            msg("no result in " + tableName)
            return
        
        self.cur.execute("SELECT Date FROM %s WHERE id = (SELECT MIN(ID) FROM %s)" % (tableName,  tableName))
        try:
            minDate = str(self.cur.fetchone()[0])
        except TypeError, e:
            msg("no result in " + tableName)
            return
#        print minDate,  maxDate
        
        minDateList = minDate.split("-")
        maxDateList = maxDate.split("-")

        digitMinYear = int(minDateList[0])
        digitMaxYear = int(maxDateList[0])
        
        for year in range(digitMinYear,  digitMaxYear + 1):
            if (year == digitMinYear):
                for month in range(int(minDateList[1]),  13):
                    for day in range(1,  32):
                        try:
                            leftParenthesis = datetime.date(year,month,day)
                        except ValueError, e:
                            continue
                        self.findTheMainContractOfDay(contra,  leftParenthesis)
            elif (year == digitMaxYear):
                for month in range(1,  int(maxDateList[1]) + 1):
                    for day in range(1,  32):
                        try:
                            leftParenthesis = datetime.date(year,month,day)
                        except ValueError, e:
                            continue
                        self.findTheMainContractOfDay(contra,  leftParenthesis)
                        
            else:
                for month in range(1,  13):
                    for day in range(1,  32):
                        try:
                            leftParenthesis = datetime.date(year,month,day)
                        except ValueError, e:
                            continue
                        self.findTheMainContractOfDay(contra,  leftParenthesis)
    
        

    def generateSpreadOfProduct(self,  contra):
        tableName = contra + "_main_contracts"
        #! date region should be user's input when this script used for daily build
        self.cur.execute("SELECT ID FROM %s WHERE id = (SELECT MAX(ID) FROM %s)" % (tableName,  tableName))
        try:
            maxID = self.cur.fetchone()[0]
        except TypeError, e:
            msg("no result in " + tableName)
            return
        
        self.cur.execute("SELECT ID FROM %s WHERE id = (SELECT MIN(ID) FROM %s)" % (tableName,  tableName))
        try:
            minID = self.cur.fetchone()[0]
        except TypeError, e:
            msg("no result in " + tableName)
            return
        
        # index 0 indicate the main contract
        contraList = ["", "", "", ""]
        ContraSet = set(("CannotBeOne",  ))
        mainContra = "CannotBeOne"
        for i in range(minID,  maxID + 1): 
            self.cur.execute("SELECT * FROM %s WHERE id = %s" % (tableName,  i))
            rec = self.cur.fetchone()
            contraList[0] = rec[2]
            contraList[1] = rec[5]
            contraList[2] = rec[8]
            contraList[3] = rec[11]
            if contraList[0] not in ContraSet:
                ContraSet.add(contraList[0])
                mainContra = contraList[0]
            self.doTheSpreadJobWithMainContra(contraList,  mainContra, contra, rec[1])
            
    #this function deal with oneday's market data
    def doTheSpreadJobWithMainContra(self,  contraList, mainContra, contra, rawDate):
        """this function deal with oneday's market data"""
        exchangeName = contra.split("_")[0].lower()
        date = str(rawDate)
        msg("begin process " + date + "'s data of " + mainContra + " minus " + str(contraList))
        #generate dif for main con and con
        for con in contraList:
            #do not in the main contract table
            if len(con) < 2:
                continue
            if con == mainContra:
                continue
            tableName = exchangeName + "_pxspread_" + mainContra + "_" + con
#            print tableName
            #to check if the spread table exists
            self.cur.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE table_name='%s'"%tableName)
            if None == self.cur.fetchone():
                #create the spread table
                self.cur.execute("CREATE TABLE `%s` (\
                      `ID` INT(11) NOT NULL AUTO_INCREMENT,\
                      `PxSpread` DOUBLE DEFAULT NULL,\
                      `Date` DATE DEFAULT NULL,\
                      `MainTm` DATETIME DEFAULT NULL, `MainEX` VARCHAR(10) DEFAULT NULL,\
                                    `MainInsID` VARCHAR(10) NOT NULL,\
                                    `MainPreStlPx` DOUBLE DEFAULT NULL,\
                                    `MainPreClsPx` DOUBLE DEFAULT NULL,\
                                    `MainPreOI` DOUBLE DEFAULT NULL,\
                                    `MainPx` DOUBLE DEFAULT NULL,\
                                  `MainOPx` DOUBLE DEFAULT NULL,\
                                  `MainHPx` DOUBLE DEFAULT NULL,\
                                  `MainLPx` DOUBLE DEFAULT NULL,\
                                  `MainV` INT(11) DEFAULT NULL,\
                                  `MainTov` DOUBLE DEFAULT NULL,\
                                  `MainOI` DOUBLE DEFAULT NULL,\
                                  `MainCPx` DOUBLE DEFAULT NULL,\
                                  `MainSPx` DOUBLE DEFAULT NULL,\
                                  `MainULPx` DOUBLE DEFAULT NULL,\
                                  `MainLLPx` DOUBLE DEFAULT NULL,\
                                  `MainBPx1` DOUBLE DEFAULT NULL,\
                                  `MainBV1` INT(11) DEFAULT NULL,\
                                  `MainAPx1` DOUBLE DEFAULT NULL,\
                                  `MainAV1` INT(11) DEFAULT NULL,\
                                  `MainSEQ` INT(11) DEFAULT NULL,\
                                  `Tm` DATETIME DEFAULT NULL, \
                                  `EX` VARCHAR(10) DEFAULT NULL,\
                                  `InsID` VARCHAR(10) NOT NULL,\
                                  `PreStlPx` DOUBLE DEFAULT NULL,\
                                  `PreClsPx` DOUBLE DEFAULT NULL,\
                                  `PreOI` DOUBLE DEFAULT NULL,\
                                  `Px` DOUBLE DEFAULT NULL,\
                                  `OPx` DOUBLE DEFAULT NULL,\
                                  `HPx` DOUBLE DEFAULT NULL,\
                                  `LPx` DOUBLE DEFAULT NULL,\
                                  `V` INT(11) DEFAULT NULL,\
                                  `Tov` DOUBLE DEFAULT NULL,\
                                  `OI` DOUBLE DEFAULT NULL,\
                                  `CPx` DOUBLE DEFAULT NULL,\
                                  `SPx` DOUBLE DEFAULT NULL,\
                                  `ULPx` DOUBLE DEFAULT NULL,\
                                  `LLPx` DOUBLE DEFAULT NULL,\
                                  `BPx1` DOUBLE DEFAULT NULL,\
                                  `BV1` INT(11) DEFAULT NULL,\
                                  `APx1` DOUBLE DEFAULT NULL,\
                                  `AV1` INT(11) DEFAULT NULL,\
                                  `SEQ` INT(11) DEFAULT NULL,\
                                  PRIMARY KEY (`ID`)\
                                ) ENGINE=MYISAM DEFAULT CHARSET=utf8\
                                "%tableName)
            
            #start process mainContra and con of the very date
            mainContraTableName = exchangeName+"_"+mainContra
            conTableName = exchangeName + "_" +con
            leftTm = date + " 00:00:00"
            rightTm = date + " 23:59:59"
            selectSql = "SELECT * FROM %s WHERE Tm BETWEEN '%s 00:00:00' AND '%s 23:59:59'"
            self.cur.execute(selectSql%(mainContraTableName, leftTm, rightTm))
            mainContraRec = self.cur.fetchall()
            self.cur.execute(selectSql%(conTableName, leftTm, rightTm))
            conRec = self.cur.fetchall()
            #data ready ,then do the compare job
    
def msg(mess):
    print time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(time.time())) + " " + mess
 
if __name__ == "__main__":
   
    msg("start work~")
    m = PreProcess()
    m.connectToMysql()
    
    #process the market data of contract defined in Constant contract table
#    m.calculateHistoryMaxV()
    
#    m.findTheMainContractOfVariety("cffe_if")

#    for s in Constant.preContractIDForTest:
#        msg("begin generate main contract " + s)
#        m.findTheMainContractOfVariety(s.lower())
        
    for s in Constant.preContractIDForTest:
        msg("begin to generate Px spread")
        m.generateSpreadOfProduct(s)
    
    m.close()
    msg("end of work")
