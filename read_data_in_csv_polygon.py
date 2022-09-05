# -*- coding: utf-8 -*-

from web3 import Web3
import os,json,time,datetime,bz2
import pandas as pd
from pandas.core.frame import DataFrame

#code_list = ['sh_600000','sh_600009','sh_600016','sh_600028','sh_600030','sh_600031','sh_600036','sh_600048','sh_600050','sh_600104','sh_600196','sh_600276','sh_600309','sh_600519','sh_600547','sh_600570','sh_600585','sh_600588','sh_600690','sh_600703','sh_600745','sh_600837','sh_600887','sh_600918','sh_601012','sh_601066','sh_601088','sh_601138','sh_601166','sh_601186','sh_601211','sh_601236','sh_601288','sh_601318','sh_601319','sh_601336','sh_601398','sh_601601','sh_601628','sh_601668','sh_601688','sh_601816','sh_601818','sh_601857','sh_601888','sh_603160','sh_603259','sh_603288','sh_603501','sh_603986']
#code_list = ['sz_399987','sh_512690']
#code_list = ['sh_512500','sh_510500']
code_list = ['sz_159915','sz_159952','sz_399987','sh_512690']

def main_func(code_list):
    for code in code_list:
        ttime = time.time()
        print('in')
        
        if os.path.exists('./'+ code +'_base.csv'):
            is_new_file = False
            data_pre = pd.read_csv('./'+code+'_base.csv')
            time_struct = time.localtime(data_pre.iloc[-2]['服务器时间戳'])
        else:
            print('base file not exist! new file!...')
            is_new_file = True
            time_struct = time.localtime(time.time()-3600*24*30*7) #7month long time
            
                
        web3_mychain = Web3(Web3.HTTPProvider('https://polygon-rpc.com')) #matic chain rpc
        print(web3_mychain.isConnected())
        print(web3_mychain.eth.blockNumber)
        
        #contract call code
        data_json = json.load(open('./Mydatabase_new.json'))
        abi = data_json["abi"]
        contract_address = web3_mychain.toChecksumAddress('0x93C165704Bb4CF087ec822D9c3321D646C844610') #polygon
        #contract_address = web3_mychain.toChecksumAddress('0xcf67a3bc838dba49c8a8c089a00ef2b7b6ef559d') #bsc
        #contract_address = web3_mychain.toChecksumAddress('0xd49eC5Da3F082B52E5ec2Fa852A227afbd6AE100')
        contract_instance = web3_mychain.eth.contract(address=web3_mychain.toChecksumAddress(contract_address),abi=abi)
        
        
        data_dict = {}
        print('wait...')
        print(code)
        my_data = []
        begin_date = datetime.date(time_struct.tm_year,time_struct.tm_mon,time_struct.tm_mday)
        end_date = datetime.date.today()
        print('begin_date:',begin_date,',end_date:',end_date)
        
        delta_date = datetime.timedelta(days=1)
        day_date = begin_date + delta_date
        front_string = 'new_'
        while day_date<=end_date:
            if day_date==datetime.date(2022,8,26):
                front_string = 'new__'
            else:
                front_string = 'new_'
            ts_db_name = front_string+day_date.strftime("%Y-%m-%d")
            print(ts_db_name,code)
            day_date += delta_date
            length_data = int(contract_instance.functions.get_length(ts_db_name,code).call())
            print(length_data)
            for i in range(length_data):
                while True:
                    try:
                        data_raw = contract_instance.functions.read(ts_db_name,code,i).call()
                        data = bz2.decompress(data_raw.encode('ISO-8859–1')).decode('utf8')
                        data_l = []
                        if len(data.split('@'))>2:
                            is_zhishu = data.split('@')[0]
                            ts_begin = data.split('@')[1]
                            data = data.split('@')[2]
                        elif len(data.split('@'))>1:
                            ts_begin = data.split('@')[0]
                            data = data.split('@')[1]
                        
                        for i in data.split('\n')[0:-1]:
                            data_l.append(i.split(','))
                            if len(data_l)==1:
                                data_l[-1][-1] = round(float(data_l[-1][-1]),3)+round(float(ts_begin),3)
                            else:
                                data_l[-1][-1] = round(float(data_l[-1][-1]),3)+round(float(data_l[-2][-1]),3)
                        my_data += data_l
                    except:
                        print('error')
                        time.sleep(1)
                        continue
                    break
        
        data = DataFrame(my_data)
        if is_zhishu=='0':
            data.rename(columns={0:'卖1价',1:'买1价',2:'最新价',3:'服务器时间戳'},inplace=True)
        else:
            data.rename(columns={0:'最新价',1:'服务器时间戳'},inplace=True)
        
        if not is_new_file:
            new_data = pd.concat([data_pre,data],axis=0)
        else:
            #new_dataframe.to_csv('./csv_download/'+zs_code+'__'+etf_code+'_base.csv')
            new_data = data
        new_data.to_csv('./'+code+'.csv')
        
        print(time.time() - ttime)
        print('out')



while True:
    time.sleep(1)
    try:
        main_func(code_list)
        print(datetime.datetime.now())
        time.sleep(3600)
    except Exception as e:
        print(e)
        time.sleep(60)





