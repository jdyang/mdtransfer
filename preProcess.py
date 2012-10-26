# -*- coding: utf-8 -*-

import sys

from util import Constant,  Switch

import util

import string
import pymysql, datetime, time,  re


class PreProcess():
    """ preprocess the market data, calculate the nessessary information"""
    
    conn = None
    cur = None
    spreaddb = None
    spreadcur = None
    
    spread_s = None
    cur_s = None
    
    def __init__(self):
       self.conn = None
       self.cur = None
       self.spreaddb = None
       self.spreadcur = None
       
       self.spread_s = None
       self.cur_s = None
       
       
    def connectToMysql(self):
        self.conn = pymysql.connect(host=Constant.mysqlHost, port=3306, user='mdwriter', passwd='mdwriter', 
                               db=Constant.dbName)
        self.spreaddb = pymysql.connect(host=Constant.mysqlHost, port=3306, user='mdwriter', passwd='mdwriter', 
                               db=Constant.pxSpreadDBName)
        self.spreadcur = self.spreaddb.cursor()
        self.cur = self.conn.cursor()
        
        self.spread_s = pymysql.connect(host=Constant.mysqlHost, port=3306, user='mdwriter', passwd='mdwriter', 
                               db=Constant.pxSpreadSdbName)
        self.cur_s = self.spread_s.cursor()
    
    def close(self):
        self.cur.close()
        self.conn.close()
        
        self.spreadcur.close()
        self.spreaddb.close()
        
        self.cur_s.close()
        self.spread_s.close()
        
    
    def calculateHistoryMaxV(self):
        """process the history market data then calculate the maxV of every contract in everyday"""
        
        
        #step 1, get valid tables
        #!be careful to choose the real task table instead of the testing one
        for contra in Constant.preContractID:
            msg(" in calculateHistoryMaxVstart: dealing with the contract : " + contra.lower())
            
            #step 0, create the daily_view table
            self.cur.execute("CREATE TABLE IF NOT EXISTS `%s` (\
                      `ID` int(11) NOT NULL AUTO_INCREMENT,\
                      `ConstrID` varchar(10) NOT NULL,\
                      `PreStlPx` double DEFAULT NULL,\
                      `PreClsPx` double DEFAULT NULL,\
                      `PreOI` double DEFAULT NULL,\
                      `Px` double DEFAULT NULL,\
                      `OPx` double DEFAULT NULL,\
                      `HPx` double DEFAULT NULL,\
                      `LPx` double DEFAULT NULL,\
                      `MaxV` int(11) DEFAULT NULL,\
                      `Tov` double DEFAULT NULL,\
                      `LatestOI` double DEFAULT NULL,\
                      `CPx` double DEFAULT NULL,\
                      `SPx` double DEFAULT NULL,\
                      `ULPx` double DEFAULT NULL,\
                      `LLPx` double DEFAULT NULL,\
                      `Tm` datetime DEFAULT NULL,\
                      `Date` date DEFAULT NULL,\
                      `BPx1` double DEFAULT NULL,\
                      `BV1` int(11) DEFAULT NULL,\
                      `APx1` double DEFAULT NULL,\
                      `AV1` int(11) DEFAULT NULL,\
                      `SEQ` int(11) DEFAULT NULL,\
                      PRIMARY KEY (`ID`),\
                      UNIQUE KEY `UniqueKey` (`ConstrID`,`Date`)\
                    ) ENGINE=MyISAM DEFAULT CHARSET=utf8" % (contra.lower()+"_daily_view"))
            
            tableNameLike = "%" + contra + "%"
            sqlCmd = "show tables like " + "'" + tableNameLike + "'"

            retNum = self.cur.execute(sqlCmd)
            tables = self.cur.fetchall()
            
            if tables == None or retNum == 0:
                msg("no tables like %s "%tableNameLike)
                return 

            #step 2, for every table, to calculate the maxV of everyday
            for tmpT in tables:
                tableName = tmpT[0]
                msg('begin processing table : ' + tableName)
                myDate = str(tableName).strip(contra)
                
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
        '''get the maxV of the day(get the last record of the day)'''
        
        dbg("dealWithTheDay between %s, %s of table(%s) contra: %s"%(leftParenthesis,  rightParenthesis, tableName,  contra))
        
        retNum = self.cur.execute("SELECT * FROM %s WHERE Tm BETWEEN \
        %s AND %s ORDER BY Tm DESC LIMIT 0,1"%(tableName, "'" + str(leftParenthesis) + "'",  "'" +str(rightParenthesis) + "'"))
        myResult = self.cur.fetchone()
        if myResult == None or retNum == 0:
            return
        #need check maxV != 0
        if myResult[10] == 0:
            return
            
        dbg("find data~ begin to find the maxV of the date")
        
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
        
        retNum = self.cur.execute("SELECT ConstrId, MaxV, LatestOI FROM  %s WHERE DATE=%s ORDER BY MaxV DESC LIMIT 0, 4" % (tableName,  strDate))
        if retNum == 0:
            return 
        
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
        
        dbg("find the maxV into main_contracts table")
        
        #insert the daily statistics into main contract table
        self.cur.execute(insertSql)

    def findTheMainContractOfVariety(self,  contra):
        tableName = contra + "_daily_view"
        targetTableName = contra + "_main_contracts"
        
        #prepare target table ready
        self.cur.execute("CREATE TABLE IF NOT EXISTS `%s` (\
                  `ID` int(11) NOT NULL AUTO_INCREMENT,\
                  `Date` date DEFAULT NULL,\
                  `ConstrID1` varchar(10) NOT NULL,\
                  `MaxV1` int(11) DEFAULT NULL,\
                  `LatestOI1` double DEFAULT NULL,\
                  `ConstrID2` varchar(10) NOT NULL,\
                  `MaxV2` int(11) DEFAULT NULL,\
                  `LatestOI2` double DEFAULT NULL,\
                  `ConstrID3` varchar(10) NOT NULL,\
                  `MaxV3` int(11) DEFAULT NULL,\
                  `LatestOI3` double DEFAULT NULL,\
                  `ConstrID4` varchar(10) NOT NULL,\
                  `MaxV4` int(11) DEFAULT NULL,\
                  `LatestOI4` double DEFAULT NULL,\
                  PRIMARY KEY (`ID`),\
                  UNIQUE KEY `UniqueKey` (`Date`)\
                ) ENGINE=MyISAM DEFAULT CHARSET=utf8" %(targetTableName))
        
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
        tableName = contra.lower() + "_main_contracts"
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
        maxVList = [0, 0, 0, 0]
        ContraSet = set(("CannotBeOne",  ))
        mainContra = "CannotBeOne"
        for i in range(minID,  maxID + 1): 
            retNum = self.cur.execute("SELECT * FROM %s WHERE id = %s" % (tableName,  i))
            if retNum == 0:
                continue
            rec = self.cur.fetchone()
            contraList[0] = rec[2]
            maxVList[0] = rec[3]
            contraList[1] = rec[5]
            maxVList[1] = rec[6]
            contraList[2] = rec[8]
            maxVList[2] = rec[9]
            contraList[3] = rec[11]
            maxVList[3] = rec[12]
            if contraList[0] not in ContraSet:
                ContraSet.add(contraList[0])
                mainContra = contraList[0]
            if i + 1 > maxID:
                break
            retNum = self.cur.execute("SELECT Date FROM %s WHERE id = %s" % (tableName,  i + 1))
            if retNum == 0:
                continue
            realDate = self.cur.fetchone()
         
            self.doTheSpreadJobWithMainContraWithSeq(contraList,  maxVList,  mainContra, contra, str(realDate[0]))
     
    #this function deal with oneday's market data
    def doTheSpreadJobWithMainContraWithSeq(self,  contraList, maxVList,  mainContra, contra, rawDate):
        """this function deal with oneday's market data in the sequence way"""
        
        exchangeName = contra.split("_")[0]
        date = str(rawDate)
        msg("begin process " + date + "'s data of " + mainContra + " minus " + str(contraList)\
             + "in the seq way")
        #generate dif for main con and con
        conIndex = 0
        for con in contraList:
            #do not in the main contract table
            if len(con) < 2:
                conIndex = conIndex + 1
                continue
            if con == mainContra:
                conIndex = conIndex + 1
                continue
            tableName = exchangeName.lower() + "_pxspread_" + mainContra.lower() + "_" + con.lower()
#            print tableName
            #to check if the spread table exists
            
            self.spreadcur.execute("CREATE TABLE IF NOT EXISTS `%s` (\
                  `ID` INT(11) NOT NULL AUTO_INCREMENT,\
                  `PxSpread` DOUBLE DEFAULT NULL,\
                  `SpreadTm` DATETIME DEFAULT NULL,\
                  `Vper` double DEFAULT NULL,\
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
            #get one day's ticket data of main contract and the con's data
            selectSql = "SELECT * FROM %s WHERE Tm BETWEEN '%s 00:00:00' AND '%s 23:59:59'"
            self.cur.execute(selectSql%(mainContraTableName, leftTm, rightTm))
            mainContraRec = self.cur.fetchall()
            self.cur.execute(selectSql%(conTableName, leftTm, rightTm))
            conRec = self.cur.fetchall()
            lenMain = len(mainContraRec)
            lenCon = len(conRec)

            pMain = 0; pCon = 0
            
            #data ready ,then do the compare job
            
            #filter the v=0 record
            while  pMain < lenMain and mainContraRec[pMain][10] == 0:
                pMain = pMain + 1
            while  pCon < lenCon and conRec[pCon][10] == 0:
                pCon = pCon + 1
                
                
            if pMain >= lenMain or pCon >=lenCon:
                msg("no valid data pass ")
                continue
            
            fullList = mainContraRec[pMain:] + conRec[pCon:]
            
       
 
            #sort by seq
            tmPList = sorted(fullList,  key = lambda k : k[22])
            
            fullList = tmPList
            
            #find first con - main(time(con) < time(main) )
            startIndex = 0; mainRIndex = 0; conRIndex = 0
            spreadTm = "'"  + "'"
            for i in range(0,  len(fullList) - 1):

                if fullList[i][2].lower() == con.lower() and fullList[i + 1][2].lower() == mainContra.lower():
                    startIndex = i
                    mainRIndex = startIndex + 1
                    conRIndex = startIndex
                    spreadTm = "'" + str(fullList[mainRIndex][17]) + "'"
                    break;
                elif fullList[i + 1][2].lower() == con.lower() and fullList[i][2].lower() == mainContra.lower():
                    startIndex = i
                    mainRIndex = startIndex
                    conRIndex = startIndex + 1
                    spreadTm = "'" + str(fullList[conRIndex][17]) + "'"
                    break;
            
            #begin to traverse through
    
            vPer = maxVList[conIndex]*1.0 / maxVList[0]
            myValue = str(fullList[mainRIndex][6] - fullList[conRIndex][6]) + ", " + spreadTm + ", " + \
                str(vPer) + ", '" + date + "', '" + str(fullList[mainRIndex][17]) + "', " \
                + str(fullList[mainRIndex][1:3])[1:-1] + ", " \
                + str(fullList[mainRIndex][3:17])[1:-1].replace("L",  "") + ", "\
                + str(fullList[mainRIndex][18:])[1:-1].replace("None",  "0").replace("L",  "") + ", '"\
                + str(fullList[conRIndex][17]) + "', " \
                + str(fullList[conRIndex][1:3])[1:-1] + ", " \
                + str(fullList[conRIndex][3:17])[1:-1].replace("L",  "") + ", "\
                + str(fullList[conRIndex][18:])[1:-1].replace("None",  "0").replace("L",  "") 
                
            insertSql = "INSERT INTO `" + tableName + "`" + str(Constant.pxSpreadTableStruct).replace("'", "`")+ \
            " values (" + myValue + ")"
            self.spreadcur.execute(insertSql)

            for i in range(startIndex + 2,  len(fullList[startIndex + 2:])):
               
                if fullList[i][2].lower() == con.lower():
                    conRIndex = i
                    spreadTm = "'" + str(fullList[conRIndex][17]) + "'"
                elif fullList[i][2].lower() == mainContra.lower():
                    mainRIndex = i
                    spreadTm = "'" + str(fullList[mainRIndex][17]) + "'"
                
                vPer = maxVList[conIndex]*1.0 / maxVList[0]
                myValue = str(fullList[mainRIndex][6] - fullList[conRIndex][6]) + ", " + spreadTm + ", " + \
                str(vPer) + ", '" + date + "', '" + str(fullList[mainRIndex][17]) + "', " \
                + str(fullList[mainRIndex][1:3])[1:-1] + ", " \
                + str(fullList[mainRIndex][3:17])[1:-1].replace("L",  "") + ", "\
                + str(fullList[mainRIndex][18:])[1:-1].replace("None",  "0").replace("L",  "") + ", '"\
                + str(fullList[conRIndex][17]) + "', " \
                + str(fullList[conRIndex][1:3])[1:-1] + ", " \
                + str(fullList[conRIndex][3:17])[1:-1].replace("L",  "") + ", "\
                + str(fullList[conRIndex][18:])[1:-1].replace("None",  "0").replace("L",  "") 
                
                insertSql = "INSERT INTO `" + tableName + "`" + str(Constant.pxSpreadTableStruct).replace("'", "`")+ \
                " values (" + myValue + ")"
                self.spreadcur.execute(insertSql)
  
                    
            conIndex = conIndex + 1
            
        msg("finish processing " + date + "'s data of " + mainContra + " minus " + str(contraList))

    #this function deal with oneday's market data
    def doTheSpreadJobWithMainContra(self,  contraList, maxVList,  mainContra, contra, rawDate):
        """this function deal with oneday's market data in the original way"""
        
        exchangeName = contra.split("_")[0].lower()
        date = str(rawDate)
        msg("begin process " + date + "'s data of " + mainContra + " minus " + str(contraList))
        #generate dif for main con and con
        conIndex = 0
        for con in contraList:
            #do not in the main contract table
            if len(con) < 2:
                conIndex = conIndex + 1
                continue
            if con == mainContra:
                conIndex = conIndex + 1
                continue
            tableName = exchangeName + "_pxspread_" + mainContra.lower() + "_" + con.lower()
#            print tableName
            #to check if the spread table exists
            
#            self.spreadcur.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE table_name='%s'"%tableName)
#            if None == self.spreadcur.fetchone():
                #create the spread table
#                msg("the table %s do not exist will be created" % tableName)
            self.spreadcur.execute("CREATE TABLE IF NOT EXISTS `%s` (\
                  `ID` INT(11) NOT NULL AUTO_INCREMENT,\
                  `PxSpread` DOUBLE DEFAULT NULL,\
                  `SpreadTm` DATETIME DEFAULT NULL,\
                  `Vper` double DEFAULT NULL,\
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
            #get one day's ticket data of main contract and the con's data
            selectSql = "SELECT * FROM %s WHERE Tm BETWEEN '%s 00:00:00' AND '%s 23:59:59'"
            self.cur.execute(selectSql%(mainContraTableName, leftTm, rightTm))
            mainContraRec = self.cur.fetchall()
            self.cur.execute(selectSql%(conTableName, leftTm, rightTm))
            conRec = self.cur.fetchall()
            lenMain = len(mainContraRec)
            lenCon = len(conRec)
#            print lenMain,  lenCon
            pMain = 0; pCon = 0
            
            #data ready ,then do the compare job
            
            #filter the v=0 record
            while  pMain < lenMain and mainContraRec[pMain][10] == 0:
                pMain = pMain + 1
            while  pCon < lenCon and conRec[pCon][10] == 0:
                pCon = pCon + 1
            
            #find the first valid record (ignore the con's record if the time  < the first record of main's time)
            while pMain < lenMain and pCon < lenCon and mainContraRec[pMain][17] > conRec[pCon][17]:
                pCon = pCon + 1
  
            spreadTm = "'" + str(mainContraRec[pMain][17]) + "'"
            #generate Px spread of every record and insert into table
            while pMain < lenMain - 1 and pCon < lenCon - 1:
                
                pxSpread = mainContraRec[pMain][6] - conRec[pCon][6]
                vPer = maxVList[conIndex]*1.0 / maxVList[0]

                myValue = str(pxSpread) + ", " + spreadTm + ", " + str(vPer) + ", '" + date + "', '" + str(mainContraRec[pMain][17]) + "', " \
                + str(mainContraRec[pMain][1:3])[1:-1] + ", " \
                + str(mainContraRec[pMain][3:17])[1:-1].replace("L",  "") + ", "\
                + str(mainContraRec[pMain][18:])[1:-1].replace("None",  "0").replace("L",  "") + ", '"\
                + str(conRec[pCon][17]) + "', " \
                + str(conRec[pCon][1:3])[1:-1] + ", " \
                + str(conRec[pCon][3:17])[1:-1].replace("L",  "") + ", "\
                + str(conRec[pCon][18:])[1:-1].replace("None",  "0").replace("L",  "") 
                
                insertSql = "INSERT INTO `" + tableName + "`" + str(Constant.pxSpreadTableStruct).replace("'", "`")+ \
                " values (" + myValue + ")"
                self.spreadcur.execute(insertSql)
                
                pMain = pMain + 1; pCon = pCon + 1
                if mainContraRec[pMain][17] < conRec[pCon][17]:
                    pCon = pCon - 1 
                    spreadTm = "'" + str(mainContraRec[pMain][17]) + "'"
                elif mainContraRec[pMain][17] > conRec[pCon][17]:
                    pMain = pMain - 1
                    spreadTm = "'" + str(conRec[pCon][17]) + "'"
                elif mainContraRec[pMain][17] == conRec[pCon][17]:
                    spreadTm = "'" + str(mainContraRec[pMain][17]) + "'"
            
            #deal with the remain main record
            if pCon == lenCon - 1:
                while pMain < lenMain:
                    pxSpread = mainContraRec[pMain][6] - conRec[pCon][6]
                    vPer = maxVList[conIndex]*1.0 / maxVList[0]

                    myValue = str(pxSpread) + ", " + spreadTm + ", " + str(vPer) + ", '" + date + "', '" + str(mainContraRec[pMain][17]) + "', " \
                    + str(mainContraRec[pMain][1:3])[1:-1] + ", " \
                    + str(mainContraRec[pMain][3:17])[1:-1].replace("L",  "") + ", "\
                    + str(mainContraRec[pMain][18:])[1:-1].replace("None",  "0").replace("L",  "") + ", '"\
                    + str(conRec[pCon][17]) + "', " \
                    + str(conRec[pCon][1:3])[1:-1] + ", " \
                    + str(conRec[pCon][3:17])[1:-1].replace("L",  "") + ", "\
                    + str(conRec[pCon][18:])[1:-1].replace("None",  "0").replace("L",  "") 
                    
                    insertSql = "INSERT INTO `" + tableName + "`" + str(Constant.pxSpreadTableStruct).replace("'", "`")+ \
                    " values (" + myValue + ")"
                    self.spreadcur.execute(insertSql)
                    
                    pMain = pMain + 1
                    if pMain >= lenMain:
                        break
                    spreadTm = "'" + str(mainContraRec[pMain][17]) + "'"
            
            #deal with the remain con record
            elif pMain >= lenMain - 1:
                while pCon < lenCon:
                    pxSpread = mainContraRec[pMain][6] - conRec[pCon][6]
                    vPer = maxVList[conIndex]*1.0 / maxVList[0]

                    myValue = str(pxSpread) + ", " + spreadTm + ", " + str(vPer) + ", '" + date + "', '" + str(mainContraRec[pMain][17]) + "', " \
                    + str(mainContraRec[pMain][1:3])[1:-1] + ", " \
                    + str(mainContraRec[pMain][3:17])[1:-1].replace("L",  "") + ", "\
                    + str(mainContraRec[pMain][18:])[1:-1].replace("None",  "0").replace("L",  "") + ", '"\
                    + str(conRec[pCon][17]) + "', " \
                    + str(conRec[pCon][1:3])[1:-1] + ", " \
                    + str(conRec[pCon][3:17])[1:-1].replace("L",  "") + ", "\
                    + str(conRec[pCon][18:])[1:-1].replace("None",  "0").replace("L",  "") 
                    
                    insertSql = "INSERT INTO `" + tableName + "`" + str(Constant.pxSpreadTableStruct).replace("'", "`")+ \
                    " values (" + myValue + ")"
                    self.spreadcur.execute(insertSql)
                    
                    pCon = pCon + 1
                    if pCon >= lenCon:
                        break
                    spreadTm = "'" + str(conRec[pCon][17]) + "'"
                    
            conIndex = conIndex + 1
            
        msg("finish processing " + date + "'s data of " + mainContra + " minus " + str(contraList))
        
    def calculate_the_k_by_table(self,  tableName):
        '''
            to calculate the k-line from data of db(pxspread) to db(pxspreadstatistic)
        '''
        
        msg("calculate the k of table : " + tableName)
        
        self.spreadcur.execute("SELECT Date FROM %s WHERE id = (SELECT MAX(ID) FROM %s)" % (tableName,  tableName))
        
        try:
            maxDate = str(self.spreadcur.fetchone()[0])
        except TypeError, e:
            msg("no result in " + tableName)
            return
        
        self.spreadcur.execute("SELECT Date FROM %s WHERE id = (SELECT MIN(ID) FROM %s)" % (tableName,  tableName))
        try:
            minDate = str(self.spreadcur.fetchone()[0])
        except TypeError, e:
            msg("no result in " + tableName)
            return
#        print minDate,  maxDate
        
        minDateList = minDate.split("-")
        maxDateList = maxDate.split("-")

        digitMinYear = int(minDateList[0])
        digitMaxYear = int(maxDateList[0])
        
        #debug
#        print tableName,  digitMinYear,  digitMaxYear,  minDateList,  digitMaxYear
        
        for year in range(digitMinYear,  digitMaxYear + 1):
            if (year == digitMinYear):
                for month in range(int(minDateList[1]),  13):
                    for day in range(1,  32):
                        try:
                            leftParenthesis = datetime.date(year,month,day)
                        except ValueError, e:
                            continue
                        self.calculate_the_k_by_date(str(leftParenthesis),  tableName)
            elif (year == digitMaxYear):
                for month in range(1,  int(maxDateList[1]) + 1):
                    for day in range(1,  32):
                        try:
                            leftParenthesis = datetime.date(year,month,day)
                        except ValueError, e:
                            continue
                        self.calculate_the_k_by_date(str(leftParenthesis),  tableName)
                        
            else:
                for month in range(1,  13):
                    for day in range(1,  32):
                        try:
                            leftParenthesis = datetime.date(year,month,day)
                        except ValueError, e:
                            continue
                        self.calculate_the_k_by_date(str(leftParenthesis),  tableName)

    def calculate_10MA_by_table(self,  tableName):
        '''
            to calculate the 10ma from data of db(pxspread) to db(pxspreadstatistic)
        '''
        self.cur_s.execute("SELECT Date FROM %s WHERE id = (SELECT MAX(ID) FROM %s)" % (tableName,  tableName))
        
        try:
            maxDate = str(self.cur_s.fetchone()[0])
        except TypeError, e:
            msg("no result in " + tableName)
            return
        
        self.cur_s.execute("SELECT Date FROM %s WHERE id = (SELECT MIN(ID) FROM %s)" % (tableName,  tableName))
        try:
            minDate = str(self.cur_s.fetchone()[0])
        except TypeError, e:
            msg("no result in " + tableName)
            return
#        print minDate,  maxDate
        
        minDateList = minDate.split("-")
        maxDateList = maxDate.split("-")

        digitMinYear = int(minDateList[0])
        digitMaxYear = int(maxDateList[0])
        
        #debug
        print tableName,  digitMinYear,  digitMaxYear,  minDateList,  digitMaxYear
        
        for year in range(digitMinYear,  digitMaxYear + 1):
            if (year == digitMinYear):
                for month in range(int(minDateList[1]),  13):
                    for day in range(1,  32):
                        try:
                            leftParenthesis = datetime.date(year,month,day)
                        except ValueError, e:
                            continue
                        self.calculate_10MA_by_date(str(leftParenthesis),  tableName)
            elif (year == digitMaxYear):
                for month in range(1,  int(maxDateList[1]) + 1):
                    for day in range(1,  32):
                        try:
                            leftParenthesis = datetime.date(year,month,day)
                        except ValueError, e:
                            continue
                        self.calculate_10MA_by_date(str(leftParenthesis),  tableName)
                        
            else:
                for month in range(1,  13):
                    for day in range(1,  32):
                        try:
                            leftParenthesis = datetime.date(year,month,day)
                        except ValueError, e:
                            continue
                        self.calculate_MA_by_date(str(leftParenthesis),  tableName)
    
    
    def calculate_10MA_by_date(self,  date,  tableName):
        '''
        '''
        
        for typeStr in Constant.dictSpreadSType.keys():
        
            selectSql = "SELECT Tm, Px  FROM %s WHERE DATE = '%s' and Type = %s"\
            %( tableName,  date,  Constant.dictSpreadSType[typeStr])
            
            retNum = self.cur_s.execute(selectSql)
            
            retRec = self.cur_s.fetchall()
            
            if None == retRec or retNum ==0:
                msg("no data in table %s of date %s"%(tableName,  date))
                return 
                
            hisTime = date + " 00:00:00"    
            hisSql = "SELECT Tm, Px FROM %s WHERE tm < '%s' \
            AND TYPE = %s ORDER BY Tm DESC LIMIT 0, %s"%(tableName,  hisTime,  Constant.dictSpreadSType[typeStr],  str(Constant.MANum - 1))
            
            histNum = self.cur_s.execute(hisSql)
            hisRec = self.cur_s.fetchall()
            
            list(hisRec).reverse()
            globalList = hisRec + retRec
           
            if len(globalList) < Constant.MANum:
                msg("no valid ma10 data of date  %s typeStr:%s, table :%s: "%(date,  typeStr,  tableName))
                updateSql = "UPDATE %s SET MA10 = NULL, BollUp = NULL, BollBo = NULL WHERE \
             TYPE = %s"%(tableName, Constant.dictSpreadSType[typeStr])
                self.cur_s.execute(updateSql)
                continue
            
            pxList = [0] * Constant.MANum
            for i in range(0,  Constant.MANum):
                pxList[i] = globalList[i][1]
            
            pList = Constant.MANum - 1
            while pList < len(globalList):
                
                stdDif = util.stdDeviation(pxList)
                ma10 = util.MA_10(pxList)
                
                updateSql = "UPDATE %s SET MA10 = %s, BollUp = %s, BollBo = %s \
                WHERE Tm = '%s' AND TYPE=%s"\
                %(tableName, str(ma10), str(ma10 + Constant.stdDeCount * stdDif),  str(ma10 - Constant.stdDeCount * stdDif),  str(globalList[pList][0]),  Constant.dictSpreadSType[typeStr] )
                
                self.cur_s.execute(updateSql)
                
                if pList + 1 >= len(globalList):
                    break;
                tmpList = pxList[1:] + [globalList[pList + 1][1]]
                pxList = tmpList
                
                pList = pList + 1




    def calculate_the_k_by_date(self,  date,  tableName):
        '''
            calculate the openPx, closePx, averagePx, highestPx, lowestPx of the tableName(main-con) of date
        '''
        msg("start processing data in table %s of date %s"%(tableName,  date))
        
        #first should judge whether the table exist
        realTableName = tableName + "_s"
        
        self.cur_s.execute("CREATE TABLE IF NOT EXISTS `%s` (\
                        `ID` int(11) NOT NULL AUTO_INCREMENT,\
                        `Tm` datetime DEFAULT NULL,\
                        `Vper` double DEFAULT NULL,\
                        `Date` date DEFAULT NULL,\
                        `OPx` double DEFAULT NULL,\
                        `CPx` double DEFAULT NULL,\
                        `Px` double DEFAULT NULL,\
                        `HPx` double DEFAULT NULL,\
                        `LPx` double DEFAULT NULL,\
                        `Type` int(11) DEFAULT NULL,\
                        `Cnt` int(11) DEFAULT NULL,\
                        `MA10` double DEFAULT NULL,\
                        `BollUp` double DEFAULT NULL,\
                        `BollBo` double DEFAULT NULL,\
                        PRIMARY KEY (`ID`),\
                        UNIQUE KEY `UniqueKey` (`Tm`,`Type`)\
                        ) ENGINE=MyISAM DEFAULT CHARSET=utf8"%realTableName)
        
        
        selectSql = "SELECT Pxspread, SpreadTm, Vper  FROM `%s`.%s WHERE DATE = '%s' ORDER BY SpreadTm"\
        %(Constant.pxSpreadDBName,  tableName,  date)
        
        retNum = self.spreadcur.execute(selectSql)
        
        retRec = self.spreadcur.fetchall()
        
        if None == retRec or retNum ==0:
            msg("no data in table %s of date %s"%(tableName,  date))
            return 
        
        #define the list of each level process all of them in one traverse
        #hPx = 0; lPx = 0; oPx = 0; cPx = 0; aPx = 0; sumPx = 0; count = 0;       
        l1min = [-65535, 655350, -65535, 0, 0, 0, 0]
        l3min = [-65535, 655350, -65535, 0, 0, 0, 0]
        l5min = [-65535, 655350, -65535, 0, 0, 0, 0]
        l15min = [-65535, 655350, -65535, 0, 0, 0, 0]
        l1hour = [-65535, 655350, -65535, 0, 0, 0, 0]
        l1day = [-65535, 655350, 65535, 0, 0, 0, 0]
        #only concern with the minute level , the hour and daily level should traverse 15min records
        lists = [l1min,  l3min, l5min, l15min]
        pCurTm = "00:00"; pCurTm3m = "00:00"; pCurTm5m = "00:00"; pCurTm15m = "00:00";
        
        vPer = 1
        
        invalidFlag = True
        contractName = tableName.split("_")[0] + "_" + tableName.split("_")[2][:2]
        
        #invalid data filter threshold
        thresHold = Constant.normalThreshold
            
        if Constant.invalidDataRule.has_key(contractName):   
            thresHold = Constant.invalidDataRule[contractName]
        
        for rec in retRec:
            pxspread = rec[0]
          
            #deal with the time-relevant part(2) print spreadTm = 09:15
        
            spreadTm = str(rec[1]).split()[1][:-3]
            
            #invalid data filter
            if invalidFlag and str(rec[1]).split()[1] < thresHold:
                continue;
            else:
                invalidFlag = False
            
            #1min
            #when reach the first record of a minute
            if pCurTm != spreadTm:
                if l1min[0] != -65535 and l1min[6] > Constant.invalidNumCount:
                    myValue =  "'" + date + " " + pCurTm + ":00'"\
                    + ", " + str(rec[2]) + ", '" + date + "'"\
                    + ", " + str(l1min[2]) + ", " + str(l1min[3])\
                    + ", " + str(l1min[5]*1.0/l1min[6]) + ", " + str(l1min[0]) + ", " + str(l1min[1])\
                    + ", " + "1" + ", " + str(l1min[6])
                    insertSql = "INSERT INTO `%s` %s VALUES ( %s) on duplicate key update Cnt = %s;"\
                    %(tableName + "_s",  str(Constant.pxSpreadSTableStruct).replace("'", "`"),  myValue,  str(l1min[6]))  
#                    print insertSql
                    self.cur_s.execute(insertSql)
                    l1min[2] = pxspread
                    l1min[0] = pxspread; l1min[1] = pxspread; l1min[5] = 0; l1min[6] = 0;
                elif l1min[0] == -65535:
                    l1min[2] = pxspread
                    vPer = rec[2]
                
                pCurTm = spreadTm;
            
            minNum = int(spreadTm[3:])
            #3min
            #when reach the first record of 3 minute
            if pCurTm3m != spreadTm:
                if l3min[2] != -65535  and minNum % 3 == 0 and l3min[6] > Constant.invalidNumCount:
                    myValue =  "'" + date + " " + pCurTm3m + ":00'"\
                    + ", " + str(rec[2]) + ", '" + date + "'"\
                    + ", " + str(l3min[2]) + ", " + str(l3min[3])\
                    + ", " + str(l3min[5]*1.0/l3min[6]) + ", " + str(l3min[0]) + ", " + str(l3min[1])\
                    + ", " + "2" + ", " + str(l3min[6])
                    insertSql = "INSERT INTO `%s` %s VALUES ( %s) on duplicate key update Cnt = %s;"\
                    %(tableName + "_s",  str(Constant.pxSpreadSTableStruct).replace("'", "`"),  myValue,  str(l3min[6])  )
                
                    self.cur_s.execute(insertSql)
                    l3min[2] = pxspread
                    l3min[5] = 0; l3min[6] = 0; l3min[0] = pxspread; l3min[1] = pxspread; 
                    pCurTm3m = spreadTm
                elif l3min[2] == -65535 and minNum % 3 == 0:
                    l3min[2] = pxspread
                    l3min[5] = 0; l3min[6] = 0; l3min[0] = pxspread; l3min[1] = pxspread; 
                    pCurTm3m = spreadTm
            
            #5min
            #when reach the first record of 5 minute
            if pCurTm5m != spreadTm:
                if l5min[2] != -65535  and minNum % 5 == 0 and l5min[6] > Constant.invalidNumCount:
                    myValue =  "'" + date + " " + pCurTm5m + ":00'"\
                    + ", " + str(rec[2]) + ", '" + date + "'"\
                    + ", " + str(l5min[2]) + ", " + str(l5min[3])\
                    + ", " + str(l5min[5]*1.0/l5min[6]) + ", " + str(l5min[0]) + ", " + str(l5min[1])\
                    + ", " + "3" + ", " + str(l5min[6])
                    insertSql = "INSERT INTO `%s` %s VALUES ( %s) on duplicate key update Cnt = %s;"\
                    %(tableName + "_s",  str(Constant.pxSpreadSTableStruct).replace("'", "`"),  myValue,  str(l5min[6])  )
                
                    self.cur_s.execute(insertSql)
                    l5min[2] = pxspread
                    l5min[5] = 0; l5min[6] = 0; l5min[0] = pxspread; l5min[1] = pxspread;
                    pCurTm5m = spreadTm 
                elif l5min[2] == -65535 and minNum % 5 == 0:
                    l5min[2] = pxspread
                    l5min[5] = 0; l5min[6] = 0; l5min[0] = pxspread; l5min[1] = pxspread;
                    pCurTm5m = spreadTm 
                    
                
                
            #15min
            #when reach the first record of 15 minute
            if pCurTm15m != spreadTm:
                if l15min[2] != -65535  and minNum % 15 == 0 and l15min[6] > Constant.invalidNumCount:
                    myValue =  "'" + date + " " + pCurTm15m + ":00'"\
                    + ", " + str(rec[2]) + ", '" + date + "'"\
                    + ", " + str(l15min[2]) + ", " + str(l15min[3])\
                    + ", " + str(l15min[5]*1.0/l15min[6]) + ", " + str(l15min[0]) + ", " + str(l15min[1])\
                    + ", " + "4" + ", " + str(l15min[6])
                    insertSql = "INSERT INTO `%s` %s VALUES ( %s) on duplicate key update Cnt = %s;"\
                    %(tableName + "_s",  str(Constant.pxSpreadSTableStruct).replace("'", "`"),  myValue,  str(l15min[6])  )
                
                    self.cur_s.execute(insertSql)
                    l15min[2] = pxspread
                    l15min[5] = 0; l15min[6] = 0; l15min[0] = pxspread; l15min[1] = pxspread; 
                    pCurTm15m = spreadTm
                elif l15min[2] == -65535 and minNum % 15 == 0:
                    l15min[2] = pxspread
                    l15min[5] = 0; l15min[6] = 0; l15min[0] = pxspread; l15min[1] = pxspread; 
                    pCurTm15m = spreadTm
                 
                
            
            #update the list's data 0, 1 , 3, 5, 6
            for tmpList in lists:
                if pxspread > tmpList[0]:
                    tmpList[0] = pxspread
                if pxspread <  tmpList[1]:
                    tmpList[1] = pxspread
                tmpList[5] = tmpList[5] + pxspread
                tmpList[6] = tmpList[6] + 1
                tmpList[3] = pxspread
            
         
        #finish the last period data's statistics
        if l1min[6] > 0:
            myValue =  "'" + date + " " + pCurTm + ":00'"\
                        + ", " + str(vPer) + ", '" + date + "'"\
                        + ", " + str(l1min[2]) + ", " + str(l1min[3])\
                        + ", " + str(l1min[5]*1.0/l1min[6]) + ", " + str(l1min[0]) + ", " + str(l1min[1])\
                        + ", " + "1" + ", " + str(l1min[6])
            insertSql = "INSERT INTO `%s` %s VALUES ( %s) on duplicate key update Cnt = %s;"\
                        %(tableName + "_s",  str(Constant.pxSpreadSTableStruct).replace("'", "`"),  myValue,  str(l1min[6]))  
    #                    print insertSql
            self.cur_s.execute(insertSql)
        
        if l3min[6] > 0 : 
            myValue =  "'" + date + " " + pCurTm3m + ":00'"\
                + ", " + str(vPer) + ", '" + date + "'"\
                + ", " + str(l3min[2]) + ", " + str(l3min[3])\
                + ", " + str(l3min[5]*1.0/l3min[6]) + ", " + str(l3min[0]) + ", " + str(l3min[1])\
                + ", " + "2" + ", " + str(l3min[6])
            insertSql = "INSERT INTO `%s` %s VALUES ( %s) on duplicate key update Cnt = %s;"\
                    %(tableName + "_s",  str(Constant.pxSpreadSTableStruct).replace("'", "`"),  myValue,  str(l3min[6])  )
        
            self.cur_s.execute(insertSql)
        
        if l5min[6] > 0:
            myValue =  "'" + date + " " + pCurTm5m + ":00'"\
                + ", " + str(vPer) + ", '" + date + "'"\
                + ", " + str(l5min[2]) + ", " + str(l5min[3])\
                + ", " + str(l5min[5]*1.0/l5min[6]) + ", " + str(l5min[0]) + ", " + str(l5min[1])\
                + ", " + "3" + ", " + str(l5min[6])
            insertSql = "INSERT INTO `%s` %s VALUES ( %s) on duplicate key update Cnt = %s;"\
                %(tableName + "_s",  str(Constant.pxSpreadSTableStruct).replace("'", "`"),  myValue,  str(l5min[6])  )
            
            self.cur_s.execute(insertSql)
        
        if l15min[6] > 0:
            myValue =  "'" + date + " " + pCurTm15m + ":00'"\
                    + ", " + str(vPer) + ", '" + date + "'"\
                    + ", " + str(l15min[2]) + ", " + str(l15min[3])\
                    + ", " + str(l15min[5]*1.0/l15min[6]) + ", " + str(l15min[0]) + ", " + str(l15min[1])\
                    + ", " + "4" + ", " + str(l15min[6])
            insertSql = "INSERT INTO `%s` %s VALUES ( %s) on duplicate key update Cnt = %s;"\
                    %(tableName + "_s",  str(Constant.pxSpreadSTableStruct).replace("'", "`"),  myValue,  str(l15min[6])  )
                
            self.cur_s.execute(insertSql)
    
        
  
        #next will deal with hour and daily level (type 5 and 6)
        #! be careful while dealing with the 1day k-line, different contract has his own rules processing the data

        ruleList = Constant.normalList
        dayVper = 0
        
        if Constant.hourKLineRule.has_key(contractName):   
            ruleList = Constant.hourKLineRule[contractName]
        dictKeys = ruleList.keys()
        dictKeys.sort()
        for r in dictKeys:
            sTime = "'" + date + " " + r + "'"
            cTime = "'" + date + " " + ruleList[r] + "'"
            recNum = self.cur_s.execute("SELECT * FROM %s WHERE TYPE = %s and Tm >= %s and Tm < %s"\
                                          %(realTableName,  Constant.dictSpreadSType["15min"],  sTime,  cTime) )
            rec15min = self.cur_s.fetchall()
            if None == rec15min or recNum == 0 or len(rec15min) == 0:
                continue
           
            l1hour[5] = 0; l1hour[6] = 0; l1hour[0] = rec15min[0][7]; l1hour[1] = rec15min[0][8]; 
            l1hour[2] =  rec15min[0][4]
            for rec in rec15min:
                if rec[7] > l1hour[0]:
                    l1hour[0] = rec[7]
                if rec[8] <  l1hour[1]:
                    l1hour[1] = rec[8]
                l1hour[5] = l1hour[5]  + 1.0 * rec[10] * rec[6]
                l1hour[6] = l1hour[6] + rec[10]
                l1hour[3] = rec[5]
            
#            print l1hour[6],  date
            
            dayVper = rec15min[0][2]
            if (l1hour[6] == 0):
                msg("WARNING no pxspread data between %s and %s in table %s"%(sTime,  cTime,  realTableName))
                continue
            else:
                myValue =  sTime + ", " + str(rec15min[0][2]) + ", '" + date + "'"\
                    + ", " + str(l1hour[2]) + ", " + str(l1hour[3])\
                    + ", " + str(l1hour[5]*1.0/l1hour[6]) + ", " + str(l1hour[0]) + ", " + str(l1hour[1])\
                    + ", " + Constant.dictSpreadSType["1hour"] + ", " + str(l1hour[6])
            insertSql = "INSERT INTO `%s` %s VALUES ( %s) on duplicate key update Cnt = %s;"\
                    %(tableName + "_s",  str(Constant.pxSpreadSTableStruct).replace("'", "`"),  myValue,  str(l1hour[6])  )
        
            self.cur_s.execute(insertSql)
            
            #calculate the day k-line
            if l1hour[0] > l1day[0]:
                    l1day[0] = l1hour[0]
            if l1hour[1] <  l1day[1]:
                l1day[1] = l1hour[1]
            l1day[5] = l1day[5] + l1hour[5]*1.0
            l1day[6] = l1day[6] + l1hour[6]
            l1day[3] = l1hour[3]
            if l1day[2] == 65535:
                l1day[2] = l1hour[2]
         
#        print l1day[6] ,  l1day[5] 
      
        if (l1day[6] == 0):
            msg("WARNING no pxspread data of the date %s in table %s"%(date,  realTableName))
            return 
        else:
            myValue =  "'" + date  + " 18:00:00', " + str(dayVper) + ", '" + date + "'"\
                    + ", " + str(l1day[2]) + ", " + str(l1day[3])\
                    + ", " + str(l1day[5]*1.0/l1day[6]) + ", " + str(l1day[0]) + ", " + str(l1day[1])\
                    + ", " + Constant.dictSpreadSType["1day"] + ", " + str(l1day[6])
                    
        insertSql = "INSERT INTO `%s` %s VALUES ( %s) on duplicate key update Cnt = %s;"\
                %(tableName + "_s",  str(Constant.pxSpreadSTableStruct).replace("'", "`"),  myValue,  str(l1day[6])  )
    
        self.cur_s.execute(insertSql)
  
    def traverse_db_pxspread_for_k(self):
        sqlCmd = "SHOW TABLES LIKE	'%pxspread%'"

        retNum = self.spreadcur.execute(sqlCmd)
        tables = self.spreadcur.fetchall()
        
        if tables == None or retNum == 0:
            msg("no tables like ")
            return 
        
        for t in tables:
            self.calculate_the_k_by_table(t[0])
            
    def traverse_db_pxspread_for_10MA(self):
        sqlCmd = "SHOW TABLES LIKE	'%pxspread%'"

        retNum = self.cur_s.execute(sqlCmd)
        tables = self.cur_s.fetchall()
        
        if tables == None or retNum == 0:
            msg("no tables like")
            return 
        regex=u"^census_.*$"
        for t in tables:
            if re.match(regex, t[0]) != None:
                continue
            self.calculate_10MA_by_table(t[0])
    
    
    def sayHello(self):
        print 'hello'

def msg(mess):
    print time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(time.time())) + " " + mess
    sys.stdout.flush()
    
def dbg(mess):
    if Switch.debug_on :
        print time.strftime("%Y-%m-%d %H:%M:%S [debug]",time.localtime(time.time())) + " " + mess
        sys.stdout.flush()
 
if __name__ == "__main__":
   
    msg("start work~")
    m = PreProcess()
    m.connectToMysql()
    
    #process the market data of contract defined in Constant contract table
    #step 1
#    m.calculateHistoryMaxV()
    
    #step 2
#    m.findTheMainContractOfVariety("cffe_if")

#    for s in Constant.preContractIDForTest:
#        msg("begin generate main contract " + s)
#        m.findTheMainContractOfVariety(s.lower())
        
#    for s in Constant.preContractIDForTest:
#        msg("begin to generate Px spread")
#        m.generateSpreadOfProduct(s)

    #step 3
#    m.generateSpreadOfProduct('cffe_if')

#    m.doTheSpreadJobWithMainContra(['IF1202', 'IF1203', '', ''], [1, 1, 1, 1],  \
#    'IF1202', 'cffe_if', '2012-10-17')

#    m.doTheSpreadJobWithMainContraWithSeq(['IF1210', 'IF1211', '', ''], [1, 1, 1, 1],  'IF1210', 'cffe_if', '2012-10-17')
    
    #step 4
    #calculate the spreadPx statistics
#    m.calculate_the_k_by_date("2012-10-16",  "cffe_pxspread_if1210_if1211")

#    m.calculate_the_k_by_table("cffe_pxspread_if1201_if1203")
    
#    m.traverse_db_pxspread()

    #step 5 
    #calculate the 10ma data
#    m.calculate_10MA_by_date("2012-10-17", "cffe_pxspread_if1210_if1211" + "_s")
    m.traverse_db_pxspread_for_10MA()
    
    
    m.close()
    msg("end of work")
