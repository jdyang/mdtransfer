import sys

from util import Constant

import util,  preProcess

from preProcess import msg,  PreProcess

import string
import pymysql, datetime, time,  re


if __name__ == "__main__":
    msg(" history data process start work~")
    m = PreProcess()
    m.connectToMysql()
   
    #step 1
    m.calculateHistoryMaxV()
    
    #step 2
    for s in Constant.preContractID:
        msg("begin generate main contract " + s)
        m.findTheMainContractOfVariety(s.lower())
        
    #step 3
    for s in Constant.preContractID:
        msg("begin generate generateSpreadOfProduct " + s)
        m.generateSpreadOfProduct(s.lower())
        
    #step 4
    #calculate the spreadPx statistics
    m.traverse_db_pxspread_for_k()
    
    #step 5 
    #calculate the 10ma data
    m.traverse_db_pxspread_for_10MA()
    

    m.close()
    msg("end of work history data process")
