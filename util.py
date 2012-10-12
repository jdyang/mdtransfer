class Constant():
    """contains constants"""
    preContractID = ("CFFE_IF", "SHFE_al",  "SHFE_au",  "SHFE_cu",  "SHFE_cu",  "SHFE_fu",  "SHFE_pb", 
                      "SHFE_rb", "SHFE_ru",  "SHFE_wr",  "SHFE_zn")
                      
    preContractIDForTest = ("CFFE_IF", "SHFE_al",  )
                      
    likelyYears = (11,  12,  13)
    
    mysqlHost = "localhost"
    
    dbName = "mdata"
    
    #market data table structure
    tableStruct = ("ConstrID","PreStlPx","PreClsPx","PreOI","Px","OPx","HPx","LPx","MaxV","Tov","LatestOI","CPx","SPx","ULPx","LLPx","Tm","Date","BPx1","BV1","APx1","AV1","SEQ")

    #main contract table structure
    mainContractTableStruct = ("Date",  "ConstrID1",  "MaxV1",  "LatestOI1",  "ConstrID2",  "MaxV2",  "LatestOI2","ConstrID3",  "MaxV3",  "LatestOI3","ConstrID4",  "MaxV4",  "LatestOI4")

    #px spread table structure
    pxSpreadTableStruct = ("PxSpread","SpreadTm","Vper","Date","MainTm","MainEX","MainInsID","MainPreStlPx","MainPreClsPx","MainPreOI","MainPx","MainOPx","MainHPx","MainLPx","MainV","MainTov","MainOI","MainCPx","MainSPx","MainULPx","MainLLPx","MainBPx1","MainBV1","MainAPx1","MainAV1","MainSEQ","Tm","EX","InsID","PreStlPx","PreClsPx","PreOI","Px","OPx","HPx","LPx","V","Tov","OI","CPx","SPx","ULPx","LLPx","BPx1","BV1","APx1","AV1","SEQ")
    
    #px spread database name
    pxSpreadDBName = "pxspread"
