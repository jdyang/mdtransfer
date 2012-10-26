import sys

from util import Constant

import util,  preProcess

from preProcess import msg,  PreProcess

import string
import pymysql, datetime, time,  re


def get_px_status(px,  pxList):
    if px > pxList[0]:
        return 1
    elif px >pxList[1]:
        return 2
    elif px > pxList[2]:
        return 3
    else:
        return 4
        
def get_conName_from_tableName(tableName): 
#        return tableName.split("_")[0] + "_" + tableName.split("_")[2][:2]
    return tableName.split("_")[0] + "_" + tableName.split("_")[2][:2]
        
def traverse_table_get_trend(m,  tableName):
    
    msg("traverse_table_get_trend of table: %s"%tableName)
    
    
    contractName = tableName.split("_")[0] + "_" + tableName.split("_")[2][:2]
    
    for typeStr in Constant.dictSpreadSType.keys():
        selectSql = "SELECT Px, BollUp, MA10, BollBo, Tm  FROM %s WHERE Type = %s AND MA10 IS NOT NULL"\
            %( tableName, Constant.dictSpreadSType[typeStr])
        retNum = m.cur_s.execute(selectSql)
        
        retRec = m.cur_s.fetchall()
        
        if None == retRec or retNum ==0:
            msg("no data in table %s of type %s"%(tableName,  typeStr))
            continue 
            
        countFlag = False; lineCount = 0
        
        #status 1 shang->zhong
        #init the status machine 
        lastStatus = 0; newStatus = 0; beginTm = 'cannotbe'; beginPx = 0
        lastStatus = get_px_status(retRec[0][0],  retRec[0][1:-1])
        if lastStatus == 1:
            countFlag = True
            beginTm = str(retRec[0][4])
            beginPx = retRec[0][0]
            beginMa = retRec[0][2]
            beginStd = retRec[0][1] - retRec[0][2]
            lineCount = 0
        
        for i in xrange(1,  len(retRec)):
            newStatus = get_px_status(retRec[i][0],  retRec[i][1:-1])
            if countFlag:
                if newStatus == 1 or newStatus == 2:
                    lineCount = lineCount + 1
                    continue
                elif newStatus == 3 or newStatus == 4:
                    lineCount = lineCount + 1
                    myvalue = "'" + tableName + "', '" + contractName + "', " + Constant.dictSpreadSType[typeStr]\
                        + ", 1, '" + beginTm + "', '" + str(retRec[i][4]) + "', " + str(beginPx) + ", " + \
                        str(retRec[i][0]) + ", " + str(lineCount) + ", " + str(beginMa) + ", " + str(beginStd)
                    insertSql = "INSERT INTO `census_pxspread_regression`(`TableName`,`InsID`,`Type`,\
                    `RegType`,`BeginTm`,`EndTm`,`BeginPx`,`EndPx`,`LineNum`, `BeginMA`, `BeginStd`) VALUES ( \
                    %s) on duplicate key update LineNum = %s ;"%(myvalue,  str(lineCount))
                    m.cur_s.execute(insertSql)
                    countFlag = False;  lineCount = 0
            else:
                if newStatus == 1:
                    countFlag = True
                    beginTm = str(retRec[i][4])
                    beginPx = retRec[i][0]
                    lineCount = 0
                    beginMa = retRec[i][2]
                    beginStd = retRec[i][1] - retRec[i][2]
        
        countFlag = False; lineCount = 0
        #status 2 shang->xia
        #init the status machine 
        lastStatus = 0; newStatus = 0; beginTm = 'cannotbe'; beginPx = 0
        lastStatus = get_px_status(retRec[0][0],  retRec[0][1:-1])
        if lastStatus == 1:
            countFlag = True
            beginTm = str(retRec[0][4])
            beginPx = retRec[0][0]
            lineCount = 0
            beginMa = retRec[0][2]
            beginStd = retRec[0][1] - retRec[0][2]
        
        for i in xrange(1,  len(retRec)):
            newStatus = get_px_status(retRec[i][0],  retRec[i][1:-1])
            if countFlag:
                if newStatus == 1 or newStatus == 2 or newStatus == 3:
                    lineCount = lineCount + 1
                    continue
                elif newStatus == 4:
                    lineCount = lineCount + 1
                    myvalue = "'" + tableName + "', '" + contractName + "', " + Constant.dictSpreadSType[typeStr]\
                        + ", 2, '" + beginTm + "', '" + str(retRec[i][4]) + "', " + str(beginPx) + ", " + \
                        str(retRec[i][0]) + ", " + str(lineCount) + ", " + str(beginMa) + ", " + str(beginStd)
                    insertSql = "INSERT INTO `census_pxspread_regression`(`TableName`,`InsID`,`Type`,\
                    `RegType`,`BeginTm`,`EndTm`,`BeginPx`,`EndPx`,`LineNum`, `BeginMA`, `BeginStd`) VALUES ( \
                    %s) on duplicate key update LineNum = %s ;"%(myvalue,  str(lineCount))
                    m.cur_s.execute(insertSql)
                    countFlag = False;  lineCount = 0
            else:
                if newStatus == 1:
                    countFlag = True
                    beginTm = str(retRec[i][4])
                    beginPx = retRec[i][0]
                    lineCount = 0      
                    beginMa = retRec[i][2]
                    beginStd = retRec[i][1] - retRec[i][2]
                    
        countFlag = False; lineCount = 0
        #status 3 xia->zhong
        #init the status machine 
        lastStatus = 0; newStatus = 0; beginTm = 'cannotbe'; beginPx = 0
        lastStatus = get_px_status(retRec[0][0],  retRec[0][1:-1])
        if lastStatus == 4:
            countFlag = True
            beginTm = str(retRec[0][4])
            beginPx = retRec[0][0]
            lineCount = 0
            beginMa = retRec[0][2]
            beginStd = retRec[0][1] - retRec[0][2]
        
        for i in xrange(1,  len(retRec)):
            newStatus = get_px_status(retRec[i][0],  retRec[i][1:-1])
            if countFlag:
                if newStatus == 3 or newStatus == 4:
                    lineCount = lineCount + 1
                    continue
                elif newStatus == 1 or newStatus == 2:
                    lineCount = lineCount + 1
                    myvalue = "'" + tableName + "', '" + contractName + "', " + Constant.dictSpreadSType[typeStr]\
                        + ", 3, '" + beginTm + "', '" + str(retRec[i][4]) + "', " + str(beginPx) + ", " + \
                        str(retRec[i][0]) + ", " + str(lineCount) + ", " + str(beginMa) + ", " + str(beginStd)
                    insertSql = "INSERT INTO `census_pxspread_regression`(`TableName`,`InsID`,`Type`,\
                    `RegType`,`BeginTm`,`EndTm`,`BeginPx`,`EndPx`,`LineNum`, `BeginMA`, `BeginStd`) VALUES ( \
                    %s) on duplicate key update LineNum = %s ;"%(myvalue,  str(lineCount))
                    m.cur_s.execute(insertSql)
                    countFlag = False;  lineCount = 0
            else:
                if newStatus == 4:
                    countFlag = True
                    beginTm = str(retRec[i][4])
                    beginPx = retRec[i][0]
                    lineCount = 0
                    beginMa = retRec[i][2]
                    beginStd = retRec[i][1] - retRec[i][2]
             
        countFlag = False; lineCount = 0
        #status 4 xia->shang
        #init the status machine 
        lastStatus = 0; newStatus = 0; beginTm = 'cannotbe'; beginPx = 0
        lastStatus = get_px_status(retRec[0][0],  retRec[0][1:-1])
        if lastStatus == 4:
            countFlag = True
            beginTm = str(retRec[0][4])
            beginPx = retRec[0][0]
            lineCount = 0
            beginMa = retRec[0][2]
            beginStd = retRec[0][1] - retRec[0][2]
        
        for i in xrange(1,  len(retRec)):
            newStatus = get_px_status(retRec[i][0],  retRec[i][1:-1])
            if countFlag:
                if newStatus == 4 or newStatus == 2 or newStatus == 3:
                    lineCount = lineCount + 1
                    continue
                elif newStatus == 1:
                    lineCount = lineCount + 1
                    myvalue = "'" + tableName + "', '" + contractName + "', " + Constant.dictSpreadSType[typeStr]\
                        + ", 4, '" + beginTm + "', '" + str(retRec[i][4]) + "', " + str(beginPx) + ", " + \
                        str(retRec[i][0]) + ", " + str(lineCount) + ", " + str(beginMa) + ", " + str(beginStd)
                    insertSql = "INSERT INTO `census_pxspread_regression`(`TableName`,`InsID`,`Type`,\
                    `RegType`,`BeginTm`,`EndTm`,`BeginPx`,`EndPx`,`LineNum`, `BeginMA`, `BeginStd`) VALUES ( \
                    %s) on duplicate key update LineNum = %s ;"%(myvalue,  str(lineCount))
                    m.cur_s.execute(insertSql)
                    countFlag = False;  lineCount = 0
            else:
                if newStatus == 4:
                    countFlag = True
                    beginTm = str(retRec[i][4])
                    beginPx = retRec[i][0]
                    lineCount = 0
                    beginMa = retRec[i][2]
                    beginStd = retRec[i][1] - retRec[i][2]

def census_the_trend(m):
    
    msg("start census_the_trend")
    sqlCmd = "SHOW TABLES LIKE	'%pxspread%'"

    retNum = m.cur_s.execute(sqlCmd)
    tables = m.cur_s.fetchall()
    
    if tables == None or retNum == 0:
        msg("no tables like")
        return 
    regex=u"^census_.*$"
    
    #manual "cffe_pxspread_if1208_if1207_s" do not exist
    tables = ["shfe_pxspread_ru1301_ru1305_s",  "shfe_pxspread_ru1205_ru1201_s",  "shfe_pxspread_rb1301_rb1305_s",  "shfe_pxspread_rb1205_rb1201_s", \
              "cffe_pxspread_if1201_if1202_s",  "cffe_pxspread_if1202_if1201_s",  "cffe_pxspread_if1202_if1203_s", \
              "cffe_pxspread_if1203_if1202_s",  "cffe_pxspread_if1203_if1204_s",  "cffe_pxspread_if1204_if1203_s", \
              "cffe_pxspread_if1204_if1205_s", "cffe_pxspread_if1205_if1204_s",  "cffe_pxspread_if1205_if1206_s", \
              "cffe_pxspread_if1206_if1205_s",  "cffe_pxspread_if1206_if1207_s",  "cffe_pxspread_if1207_if1206_s", \
              "cffe_pxspread_if1207_if1208_s",  "cffe_pxspread_if1208_if1209_s", \
              "cffe_pxspread_if1209_if1208_s",  "cffe_pxspread_if1209_if1210_s",  "cffe_pxspread_if1210_if1209_s", \
              "cffe_pxspread_if1210_if1211_s",  "cffe_pxspread_if1211_if1210_s",  "cffe_pxspread_if1211_if1212_s"]
    
    for t in tables:
#        if re.match(regex, t[0]) == None:
#            traverse_table_get_trend(m,  t[0])
        traverse_table_get_trend(m,  t)


def calculate_the_trend_avg(m):
    contraList = ["cffe_if",  "shfe_ru",  "shfe_rb"]
    startTm = "2012-08-01 00:00:00"
    endTm = "2012-10-27 23:59:59"
    for conName in contraList:
        for typeStr in Constant.dictSpreadSType.keys():
            for trendStr in Constant.kLineTrendType.keys():
            
                selectSql = "SELECT MAX(LineNum), AVG(LineNum), MIN(LineNum) FROM census_pxspread_regression \
                WHERE InsID = '%s' AND TYPE = %s AND RegType = %s AND BeginTM > '%s' AND EndTm < '%s'"\
                    %( conName, Constant.dictSpreadSType[typeStr],  Constant.kLineTrendType[trendStr], startTm,  endTm)
                retNum = m.cur_s.execute(selectSql)
                
                retRec = m.cur_s.fetchone()
                
                if None == retRec or retNum ==0:
                    msg("no data in census_pxspread_regression of insId %s type %s regType %s beginTm %s, endTm %s"\
                        %(conName,  typeStr,  trendStr,  startTm,  endTm))
                    continue 
                    
                print "insId %s type %s regType %s (max, avg, min)"%\
                    (conName,  typeStr,  trendStr) + str(retRec)
                    
                    
def calculate_the_profit(m):
    contraList = ["cffe_if",  "shfe_ru",  "shfe_rb"]
    startTm = "2012-08-01 00:00:00"
    endTm = "2012-10-27 23:59:59"
    for conName in contraList:
        for typeStr in Constant.dictSpreadSType.keys():
            
            # upToDown  
            selectSql = "SELECT sum(BeginPx - EndPx - %s) FROM census_pxspread_regression \
            WHERE InsID = '%s' AND TYPE = %s AND RegType = 2 AND BeginTM > '%s' AND EndTm < '%s'"\
                %(str(Constant.tradeCost[conName]),  conName, Constant.dictSpreadSType[typeStr], startTm,  endTm)
            retNum = m.cur_s.execute(selectSql)
            
            upToDown = m.cur_s.fetchone()
            
            if None == upToDown or retNum ==0:
                msg("no data in census_pxspread_regression of insId %s type %s regType %s beginTm %s, endTm %s"\
                    %(conName,  typeStr,  "upToDown",  startTm,  endTm))
                continue 
                
            print "insId %s type %s (upToDown)"%\
                (conName,  typeStr) + str(upToDown)
            
            # downToUp   
            selectSql = "SELECT sum(EndPx - BeginPx - %s) FROM census_pxspread_regression \
            WHERE InsID = '%s' AND TYPE = %s AND RegType = 4 AND BeginTM > '%s' AND EndTm < '%s'"\
                %(str(Constant.tradeCost[conName]),  conName, Constant.dictSpreadSType[typeStr], startTm,  endTm)
            retNum = m.cur_s.execute(selectSql)
            
            downToUp = m.cur_s.fetchone()
            
            if None == downToUp or retNum ==0:
                msg("no data in census_pxspread_regression of insId %s type %s regType %s beginTm %s, endTm %s"\
                    %(conName,  typeStr,  "downToUp",  startTm,  endTm))
                continue 
                
            print "insId %s type %s (downToUp)"%\
                (conName,  typeStr) + str(downToUp)
                

def draw_lines(m):
    sqlStr = "SELECT Tm, Px, MA10, BollUp, BollBo from shfe_pxspread_rb1301_rb1305_s where type = 3 and Tm > '2012-10-11 00:00:00'  \
    and Tm < '2012-10-12 23:59:59' and MA10 is not NULL order by Tm"
    m.cur_s.execute(sqlStr)
    
    rec = m.cur_s.fetchall()
    
    tm = []; px = []; ma = []; up = []; down = [];c = [];
    count = 1
    for i in rec:
        tm.append(i[0])
        c.append(count)
        px.append(i[1])
        ma.append(i[2])
        up.append(i[3])
        down.append(i[4])
        count = count + 1
        
    import numpy as np
    import pylab as pl
    from matplotlib.dates import DayLocator, HourLocator, DateFormatter, drange
    
    pl.plot(c, px, 'ro')
    pl.plot(c, px,  'r')
    pl.plot(c, ma,  'g:')
    pl.plot(c,  up,  'b--') 
    pl.plot(c,  down,  'y--')
    axes = pl.gca()
    axes.set_xticks(range(1, count + 1))
    axes.set_xticklabels(tm, rotation=90, size=6)
    pl.grid(True)
    pl.show()
                

if __name__ == "__main__":
    msg(" census start work~")
    m = PreProcess()
    m.connectToMysql()
    
#    census_the_trend(m)
#    traverse_table_get_trend(m,  "cffe_pxspread_if1210_if1303_s")
  
#    calculate_the_trend_avg(m)
#
#    calculate_the_profit(m)
    
    draw_lines(m)
    
    
    m.close()
    msg("end of work of census")
