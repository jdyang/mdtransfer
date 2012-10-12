class Constant():
    """contains constants"""
    preContractID = ("CFFE_IF", "SHFE_al",  "SHFE_au",  "SHFE_cu",  "SHFE_cu",  "SHFE_fu",  "SHFE_pb", 
                      "SHFE_rb", "SHFE_ru",  "SHFE_wr",  "SHFE_zn")
                      
    preContractIDForTest = ("SHFE_rb",  )
                      
    likelyYears = (11,  12,  13)
    
    mysqlHost = "localhost"
    
    dbName = "mdata"
    
    #market data table structure
    tableStruct = ("ConstrID","PreStlPx","PreClsPx","PreOI","Px","OPx","HPx","LPx","MaxV","Tov","LatestOI","CPx","SPx","ULPx","LLPx","Tm","Date","BPx1","BV1","APx1","AV1","SEQ")

    #main contract table structure
    mainContractTableStruct = ("Date",  "ConstrID1",  "MaxV1",  "LatestOI1",  "ConstrID2",  "MaxV2",  "LatestOI2","ConstrID3",  "MaxV3",  "LatestOI3","ConstrID4",  "MaxV4",  "LatestOI4")
