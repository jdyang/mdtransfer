import sys

from util import Constant

import util,  preProcess

from preProcess import msg,  PreProcess

import string
import pymysql, datetime, time,  re

defaultDate = 'NotDefine'

definedToday = defaultDate

definedYestoday = defaultDate

def getToday():
    if defaultDate == definedToday:
        return time.strftime('%Y-%m-%d',time.localtime(time.time()))
    else:
        return definedToday
    
def getYestoday():
    if defaultDate == definedYestoday:
        lastDate = datetime.date.today() - datetime.timedelta(days=1)
        return str(lastDate)
    else:
        return definedYestoday
    
def insertDailyView(m):
    for s in Constant.preContractID:
        tableNameLike = "%" + s + "%"
        sqlCmd = "show tables like " + "'" + tableNameLike + "'"

        regex = '^.*_daily_view$'
        regex2 = '^.*_main_contracts$'
    
        retNum = m.cur.execute(sqlCmd)
        tables = m.cur.fetchall()
        if tables == None or retNum == 0:
            msg("no tables like %s "%tableNameLike)
            continue 
        for tmpT in tables:
            tableName = tmpT[0]
            if re.match(regex, tableName) == None and re.match(regex2,  tableName) == None:
                m.dealWithTheDay(getYestoday() + " 23:00:00",  getToday() + " 23:00:00", tableName,  s.lower())
    
def generateSpreadOfDay(m,  contra,  today):
    
    # index 0 indicate the main contract
    contraList = ["", "", "", ""]
    maxVList = [0, 0, 0, 0]
    tableName = contra.lower() + "_main_contracts"
    #get last day's main contracts
    retNum = m.cur.execute("SELECT * FROM %s WHERE id < (SELECT id FROM %s \
    WHERE DATE = '%s') ORDER BY id DESC LIMIT  1" % (tableName,  tableName,  today))
    if retNum == 0:
        return
    rec = m.cur.fetchone()  
    
    contraList[0] = rec[2]
    maxVList[0] = rec[3]
    contraList[1] = rec[5]
    maxVList[1] = rec[6]
    contraList[2] = rec[8]
    maxVList[2] = rec[9]
    contraList[3] = rec[11]
    maxVList[3] = rec[12]
    mainContra = contraList[0]
    
    m.doTheSpreadJobWithMainContraWithSeq(contraList,  maxVList,  mainContra, contra, today)
    
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



def rectify_the_data(m,  date):
    
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
            rectify_by_date(m,  date,  t[0])


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

def data_clean(m,  date):
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
        clean_by_date(m,  date,  t[0])


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


def doTheDataJob(m):
    
    today = getToday()
     
    #step 1
    #prepare the daily view
    msg("start step 1  prepare the daily view ")
    insertDailyView(m)
    msg("finish step 1  prepare the daily view ")
    
    #step 2
    #calculate the max V
    msg("start step 2  calculate the max V ")
    for s in Constant.preContractID:
        msg("begin generate main contract " + s)
        m.findTheMainContractOfDay(s.lower(), today)
    msg("finish step 2  calculate the max V ")
        
    #step 3
    #generate the Px spread
    msg("start step 3  generate the Px spread ")
    for s in Constant.preContractID:
        msg("begin generate generateSpreadOfProduct " + s)
        generateSpreadOfDay(m,  s,  today)
    
    msg("finish step 3  generate the Px spread ")
 
    
    #step 4
    #calculate the spreadPx statistics k-line
    msg("start step 4  calculate the spreadPx statistics k-line")
    sqlCmd = "SHOW TABLES LIKE	'%pxspread%'"

    retNum = m.spreadcur.execute(sqlCmd)
    tables = m.spreadcur.fetchall()
    
    if tables == None or retNum == 0:
        msg("no tables like  ")
        return 
    
    for t in tables:
        m.calculate_the_k_by_date(getToday(),  t[0])
    msg("finish step 4  calculate the spreadPx statistics k-line")
    
    #step 5 rectify the last k-line
    rectify_the_data(m,  today)

    
    #step 6 
    #calculate the 10ma data
    msg("start step 6 calculate the 10ma data")
    sqlCmd = "SHOW TABLES LIKE	'%pxspread%'"
    retNum = m.cur_s.execute(sqlCmd)
    tables = m.cur_s.fetchall()
    
    regex=u"^census_.*$"
    if tables == None or retNum == 0:
        msg("no tables like ")
        return 
    
    for t in tables:
        if re.match(regex, t[0]) != None:
            continue
        m.calculate_10MA_by_date(getToday(),  t[0])
    msg("finish step 6 calculate the 10ma data")   
        
    #step 7 clean dirty data
    msg("start step 7 clean dirty data")
    data_clean(m,  today)

    #step 8 add reverse data to boll & ma 
    msg("start step 8 add reverse data to boll & ma ")
    add_reverse_data(m)

        
    




if __name__ == "__main__":
    
    msg("daily data process start work~")
    m = PreProcess()
    m.connectToMysql()
    
    
    #can define definedToday and definedYestoday to calculate andy date you want
    definedToday = '2012-10-23'; definedYestoday = '2012-10-22'
#    definedToday = defaultDate; definedYestoday = defaultDate
    
#    print getToday(),  getYestoday()
    
    doTheDataJob(m)

    m.close()
    msg("daily data process end of work")
    
    
