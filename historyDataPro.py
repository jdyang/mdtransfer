import sys

from util import Constant

import util,  preProcess

from preProcess import msg,  PreProcess

import string
import pymysql, datetime, time,  re


def clean_by_date(m,  date,  tableName):
    contractName = tableName.split("_")[0] + "_" + tableName.split("_")[2][:2]
    #invalid data filter threshold
    thresHold = Constant.normalThreshold
        
    if Constant.invalidDataRule.has_key(contractName):   
        thresHold = Constant.invalidDataRule[contractName]
    
    leftDateStr = date + " 00:00:00"
    dateStr = date + " " + thresHold
    
    delSql = "DELETE FROM %s WHERE Tm \
    >= '%s' AND Tm < '%s' AND Cnt < 3"%(tableName,  leftDateStr,  dateStr)
    m.cur_s.execute(delSql)

def clean_by_table(m,  tableName):
    m.cur_s.execute("SELECT Date FROM %s WHERE id = (SELECT MAX(ID) FROM %s)" % (tableName,  tableName))
        
    try:
        maxDate = str(m.cur_s.fetchone()[0])
    except TypeError, e:
        msg("no result in " + tableName)
        return
    
    m.cur_s.execute("SELECT Date FROM %s WHERE id = (SELECT MIN(ID) FROM %s)" % (tableName,  tableName))
    try:
        minDate = str(m.cur_s.fetchone()[0])
    except TypeError, e:
        msg("no result in " + tableName)
        return
#        print minDate,  maxDate
    
    minDateList = minDate.split("-")
    maxDateList = maxDate.split("-")

    digitMinYear = int(minDateList[0])
    digitMaxYear = int(maxDateList[0])
    
    #debug
#    print tableName,  digitMinYear,  digitMaxYear,  minDateList,  digitMaxYear
    
    for year in range(digitMinYear,  digitMaxYear + 1):
        if (year == digitMinYear):
            for month in range(int(minDateList[1]),  13):
                for day in range(1,  32):
                    try:
                        leftParenthesis = datetime.date(year,month,day)
                    except ValueError, e:
                        continue
                    clean_by_date(m,  str(leftParenthesis),  tableName)
        elif (year == digitMaxYear):
            for month in range(1,  int(maxDateList[1]) + 1):
                for day in range(1,  32):
                    try:
                        leftParenthesis = datetime.date(year,month,day)
                    except ValueError, e:
                        continue
                    clean_by_date(m,  str(leftParenthesis),  tableName)
                    
        else:
            for month in range(1,  13):
                for day in range(1,  32):
                    try:
                        leftParenthesis = datetime.date(year,month,day)
                    except ValueError, e:
                        continue
                    clean_by_date(m,  str(leftParenthesis),  tableName)
    

def data_clean(m):
    sqlCmd = "SHOW TABLES LIKE	'%pxspread%'"

    retNum = m.cur_s.execute(sqlCmd)
    tables = m.cur_s.fetchall()
    
    if tables == None or retNum == 0:
        msg("no tables like")
        return 
    
    regex=u"^census_.*$"
    for t in tables:
        if re.match(regex, t[0]) != None:
            continue
        clean_by_table(m,  t[0])
 


def do_the_data_adding_job(m,  tableName, reverseName):
    
    msg("do_the_data_adding_job from table %s to %s"%(reverseName,  tableName))
    
    for typeStr in Constant.dictSpreadSType.keys():
        
        selectSql = "SELECT Tm, Px  FROM %s WHERE Type = %s ORDER BY Tm limit %s"\
        %( tableName, Constant.dictSpreadSType[typeStr],  str(Constant.MANum - 1))
        
        retNum = m.cur_s.execute(selectSql)
        
        retRec = m.cur_s.fetchall()
        
        if None == retRec or retNum ==0:
            msg("no manum data in table %s"%(tableName))
            return 
            
        hisSql = "SELECT Tm, Px FROM %s WHERE TYPE = %s ORDER	 BY Tm DESC \
        LIMIT %s"%(reverseName, Constant.dictSpreadSType[typeStr],  str(Constant.MANum - 1))
        
        histNum = m.cur_s.execute(hisSql)
        hisRec = m.cur_s.fetchall()
        
        list(hisRec).reverse()
        
        globalList = hisRec + retRec
        
       
        if len(globalList) < Constant.MANum:
            msg("no valid ma10 typeStr:%s, table :%s: "%(typeStr,  tableName))
            continue
        
        pxList = [0] * Constant.MANum
        for i in range(0,  Constant.MANum):
            if i < len(hisRec): 
                pxList[i] = globalList[i][1] * -1
            else:
                pxList[i] = globalList[i][1]
        
        pList = Constant.MANum - 1
        while pList < len(globalList):
            
            stdDif = util.stdDeviation(pxList)
            ma10 = util.MA_10(pxList)
            
            updateSql = "UPDATE %s SET MA10 = %s, BollUp = %s, BollBo = %s \
            WHERE Tm = '%s' AND TYPE=%s"\
            %(tableName, str(ma10), str(ma10 + Constant.stdDeCount * stdDif),  str(ma10 - Constant.stdDeCount * stdDif),  str(globalList[pList][0]),  Constant.dictSpreadSType[typeStr] )
            
            m.cur_s.execute(updateSql)
            
            if pList + 1 >= len(globalList):
                break;
            tmpList = pxList[1:] + [globalList[pList + 1][1]]
            pxList = tmpList
            
            pList = pList + 1


def add_reverse_data(m):
    sqlCmd = "SHOW TABLES LIKE	'%pxspread%'"

    retNum = m.cur_s.execute(sqlCmd)
    tables = m.cur_s.fetchall()
    
    if tables == None or retNum == 0:
        msg("no tables like")
        return 
    regex=u"^census_.*$"
    for t in tables:
        tableName = t[0]
        if re.match(regex, t[0]) != None:
            continue
        splitName = tableName.split("_")
        name1 = splitName[2]
        name2 = splitName[3]
        if name1 > name2:
            reverseName = splitName[0] + "_" + splitName[1] + "_" + splitName[3] + "_" \
            + splitName[2] + "_" + splitName[4]
            if tables.count((reverseName, )) > 0:
                do_the_data_adding_job(m,  tableName,  reverseName)
            
def rectify_by_date(m,  date,  tableName):
    
    msg("rectify the table %s of date %s"%(tableName,  date))
    contractName = tableName.split("_")[0] + "_" + tableName.split("_")[2][:2]
    #rectify type
    rectifyType = Constant.normalRectify
        
    if Constant.rectifyRule.has_key(contractName):   
        rectifyType = Constant.rectifyRule[contractName]
        
    # step 1 rectify the last 1hour k-line with the last 1min k-line
    selectSql = "SELECT CPx, Px, HPx, LPx, Cnt FROM %s WHERE Tm = '%s' and Type = 1"%\
    (tableName, date + " " + rectifyType[-1])
    recNum = m.cur_s.execute(selectSql)
    if recNum > 0:
        bMin = m.cur_s.fetchone()
        selectSql = "SELECT CPx, Px, HPx, LPx, Cnt, Tm FROM %s WHERE Date = '%s' AND TYPE = 5 order by Tm desc limit 1"%\
                (tableName,  date)
        recNum = m.cur_s.execute(selectSql)
        if recNum > 0:
            bHour = m.cur_s.fetchone()
            newPx = (bMin[1] * bMin[4] + bHour[1] * bHour[4]) * 1.0/(bMin[4] + bHour[4])
            if bMin[2] > bHour[2]:
                newHPx = bMin[2]
            else:
                newHPx = bHour[2]
            if bMin[3] < bHour[3]:
                newLPx = bMin[3]
            else:
                newLPx = bHour[3]
            updateSql = "UPDATE %s SET CPx = %s, Px = %s, HPx = %s, LPx = %s, Cnt = %s \
            WHERE Tm = '%s' AND TYPE=5"%(tableName,  str(bMin[0]),  str(newPx),  str(newHPx), \
                                                               str(newLPx),  str((bMin[4] + bHour[4])), str(bHour[5]) )
            m.cur_s.execute(updateSql)
    
    for boundary in rectifyType:
        bTm = date + " " + boundary
        #step 2 rectify the 1min, 3min, 5min, 15min
        for i in xrange(1,  5):
            selectSql = "SELECT CPx, Px, HPx, LPx, Cnt FROM %s WHERE Tm = '%s' AND TYPE = %s"%\
            (tableName,  bTm,  str(i))
            recNum = m.cur_s.execute(selectSql)
            if recNum > 0:
                bRec = m.cur_s.fetchone()
                selectSql = "SELECT CPx, Px, HPx, LPx, Cnt, Tm FROM %s WHERE Tm < '%s' AND \
                TYPE = %s ORDER BY tm DESC LIMIT 1"%(tableName,  bTm, str(i))
                recNum = m.cur_s.execute(selectSql)
                if recNum > 0:
                    lRec = m.cur_s.fetchone()
                    newPx = (bRec[1] * bRec[4] + lRec[1] * lRec[4]) * 1.0/(bRec[4] + lRec[4])
                    if bRec[2] > lRec[2]:
                        newHPx = bRec[2]
                    else:
                        newHPx = lRec[2]
                    if bRec[3] < lRec[3]:
                        newLPx = bRec[3]
                    else:
                        newLPx = lRec[3]
                    updateSql = "UPDATE %s SET CPx = %s, Px = %s, HPx = %s, LPx = %s, Cnt = %s \
                    WHERE Tm = '%s' AND TYPE=%s"%(tableName,  str(bRec[0]),  str(newPx),  str(newHPx), \
                                                                       str(newLPx),  str((bRec[4] + lRec[4])), str(lRec[5]),  str(i) )
                    
                    m.cur_s.execute(updateSql)
        #step 3 delete the boundary record
        deleteSql = "DELETE FROM %s WHERE Tm = '%s'"%(tableName,  bTm)  
        m.cur_s.execute(deleteSql)


def rectify_by_table(m,  tableName):
    m.cur_s.execute("SELECT Date FROM %s WHERE id = (SELECT MAX(ID) FROM %s)" % (tableName,  tableName))
        
    try:
        maxDate = str(m.cur_s.fetchone()[0])
    except TypeError, e:
        msg("no result in " + tableName)
        return
    
    m.cur_s.execute("SELECT Date FROM %s WHERE id = (SELECT MIN(ID) FROM %s)" % (tableName,  tableName))
    try:
        minDate = str(m.cur_s.fetchone()[0])
    except TypeError, e:
        msg("no result in " + tableName)
        return
    
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
                    rectify_by_date(m, str(leftParenthesis),  tableName)
        elif (year == digitMaxYear):
            for month in range(1,  int(maxDateList[1]) + 1):
                for day in range(1,  32):
                    try:
                        leftParenthesis = datetime.date(year,month,day)
                    except ValueError, e:
                        continue
                    rectify_by_date(m, str(leftParenthesis),  tableName)
                    
        else:
            for month in range(1,  13):
                for day in range(1,  32):
                    try:
                        leftParenthesis = datetime.date(year,month,day)
                    except ValueError, e:
                        continue
                    rectify_by_date(m, str(leftParenthesis),  tableName)


def rectify_the_data(m):
    
    msg("start rectify the data")
    sqlCmd = "SHOW TABLES LIKE	'%pxspread%'"

    retNum = m.cur_s.execute(sqlCmd)
    tables = m.cur_s.fetchall()
    
    if tables == None or retNum == 0:
        msg("no tables like")
        return 
    regex=u"^census_.*$"
    for t in tables:
        if re.match(regex, t[0]) == None:
            rectify_by_table(m,  t[0])


if __name__ == "__main__":
    msg(" history data process start work~")
    m = PreProcess()
    m.connectToMysql()
   
    #step 1
#    m.calculateHistoryMaxV()
    
    #step 2
#    for s in Constant.preContractID:
#        msg("begin generate main contract " + s)
#        m.findTheMainContractOfVariety(s.lower())
        
    #step 3
#    for s in Constant.preContractID:
#        msg("begin generate generateSpreadOfProduct " + s)
#        m.generateSpreadOfProduct(s)
        
    #step 4
    #calculate the spreadPx statistics
#    m.traverse_db_pxspread_for_k()

    #step 5 rectify the last k-line
#    rectify_the_data(m)
#    rectify_by_date(m,  "2012-03-15",  "cffe_pxspread_if1204_if1206_s")


    #step 6 clean dirty data
    data_clean(m)
    
    #step 7 
    #calculate the 10ma data
    m.traverse_db_pxspread_for_10MA()
    



    #step 8 add reverse data to boll & ma 
    add_reverse_data(m)

    m.close()
    msg("end of work history data process")
