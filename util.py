class Constant():
    """contains constants"""
    preContractID = ("CFFE_IF", "SHFE_al",  "SHFE_au",  "SHFE_cu",  "SHFE_cu",  "SHFE_fu",  "SHFE_pb", 
                      "SHFE_rb", "SHFE_ru",  "SHFE_wr",  "SHFE_zn")
                      
    preContractIDForTest = ("CFFE_IF", )
                      
    likelyYears = (11,  12,  13)
    
    mysqlHost = "localhost"
    
    dbName = "mdata"
    
    #market data table structure
    tableStruct = ("ConstrID","PreStlPx","PreClsPx","PreOI","Px","OPx","HPx","LPx","MaxV","Tov","LatestOI","CPx","SPx","ULPx","LLPx","Tm","Date","BPx1","BV1","APx1","AV1","SEQ")

    #main contract table structure
    mainContractTableStruct = ("Date",  "ConstrID1",  "MaxV1",  "LatestOI1",  "ConstrID2",  "MaxV2",  "LatestOI2","ConstrID3",  "MaxV3",  "LatestOI3","ConstrID4",  "MaxV4",  "LatestOI4")

    #px spread table structure
    pxSpreadTableStruct = ("PxSpread","SpreadTm","Vper","Date","MainTm","MainEX","MainInsID","MainPreStlPx","MainPreClsPx","MainPreOI","MainPx","MainOPx","MainHPx","MainLPx","MainV","MainTov","MainOI","MainCPx","MainSPx","MainULPx","MainLLPx","MainBPx1","MainBV1","MainAPx1","MainAV1","MainSEQ","Tm","EX","InsID","PreStlPx","PreClsPx","PreOI","Px","OPx","HPx","LPx","V","Tov","OI","CPx","SPx","ULPx","LLPx","BPx1","BV1","APx1","AV1","SEQ")
    
    #px spread statistic table stucture
    pxSpreadSTableStruct = ("Tm","Vper","Date","OPx","CPx","Px","HPx","LPx","Type","Cnt")
    
    #px spread database name
    pxSpreadDBName = "pxspread"
    
    #px spread statistic database name
    pxSpreadSdbName = "pxspreadstatistic"
    
    #a dict for the type of pxspread statistics
    dictSpreadSType = {"1min":"1",  "3min":"2",  "5min":"3",  "15min":"4",  "1hour":"5",  "1day":"6"}
    
    #different contract has different type of k-line generate rules, noramal futures will be treat as default process ruls
    ifList = {"09:15:00":"10:15:00",  "10:15:00":"11:15:00", "11:15:00":"13:45:00",  \
            "13:45:00":"14:45:00",  "14:45:00":"15:15:00"}
    normalList = {"09:00:00":"10:00:00",  "10:00:00":"11:00:00",  "11:00:00":"13:00:00", \
                   "13:00:00":"14:00:00",  "14:00:00":"15:00:00"}
    hourKLineRule = {"cffe_if":ifList}
   
  
  

def stdDeviation(a):
    '''
        biaozhuncha
    '''
    l=len(a)
    m=sum(a) * 1.0/l
    d=0
    for i in a: 
        d = d + (i-m)**2
    return (d*(1.0/l))**0.5

def MA_10(a):
    '''
        10ma, -65535 indicate the error input
        return a double if OK
    '''
    l = len(a)
    if l != 10:
        return -65535
    ma = sum(a) * 1.0/l
    return ma
        

if __name__ == "__main__":
    a=[5,6,8,9]
    print 'helo'
    
    
    
