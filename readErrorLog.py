#coding:utf-8

#
# A trial of read apache error log and  save as a csv file 
#
"""
（概要）
Apache のエラーログファイルを読み込んでcsvファイルに書き出す、試作品。
書き出した csvファイルを 、後で、excel等で読み込んでアクセス内容を分析することを想定している。

（例）
python3 readErrorLog.py -w "logs-error/" 

-w で　apacheのエラーログファイルのあるディレクトリーを指定する。

error_output_all.csvに書き出される。

（注意）
エラーログファイルのフォーマットに合わせて pattern0　を定義すること。

エラーログのサンプル例:

[Thu Jun 06 17:00:01.485968 2019] [autoindex:error] [pid 93728] [client xxxx.xxxx.xxx.xxx:0] AH01276: Cannot serve directory /home/xxx/www/: No matching DirectoryIndex (index.html,index.htm,index.shtml,index.shtm,index.cgi,index.php,index.hdml) found, and server-generated directory index forbidden by Options directive
[Mon Jun 24 04:34:51.599312 2019] [cgi:error] [pid 26626] [client xxx.xxx.xxx.xxx:0] AH02811: script not found or unable to stat: /home/xxx/www/xxxxxx.php
[Fri Jan 31 22:10:45.801132 2020] [access_compat:error] [pid 49252] [client xxx.xxx.xxx.xxx:0] AH01797: client denied by server configuration: /home/xxx/www/xxxxxx.html

"""
#Check version
# Python 3.6.4, 64bit on Win32 (Windows 10)
# numpy (1.16.3)
# pandas (0.23.4)

import os
import sys
import argparse
import glob
import gzip
import re
import datetime
import numpy as np
import pandas as pd


pattern0= r'\[(.+)\] \[(.+)\] \[(.+)\] \[(.+)\] (.+): (.+): (.+)'

def prase0( line ):
    match= re.findall( pattern0, line)
    return match[0]

def trans_datetime( l0):
    if 1:  # transform date time to 2019-xx-xx xx:xx:xx
        dt0=datetime.datetime.strptime(l0, '%a %b %d %H:%M:%S.%f %Y')
        dt1object= datetime.datetime(dt0.year, dt0.month, dt0.day, dt0.hour, dt0.minute, dt0.second)
        return str(dt1object)
    else:   # return as is input
        return l0

def get_files( path ):
    # get log file name list
    return glob.glob( path + "error_log*")

def ip_reverse_lookup(ip):
	try:
		return socket.gethostbyaddr(ip)[0]
	except:
		return ip #np.nan

def get_ip(df):
    list0= df['ip'].unique().tolist()
    dic0={}
    print ('enter ip reverse lookup')
    for i in range (len(list0)):
        print (i)
        """
        This cause not work ip_reverse_lookup ?
        sys.stdout.write("\r%d" % i)
        sys.stdout.flush()
        """
        dic0[ list0[i] ]= ip_reverse_lookup(list0[i])
    
    return dic0


if __name__ == "__main__":
    
    parser = argparse.ArgumentParser(description='apache error log reading')
    parser.add_argument('--log_file', '-w', default='logs-error/', help='specify error log file directory')
    args = parser.parse_args()
    
    
    log_file_list=get_files(args.log_file)
    #print ( log_file_list)
    
    dic0={}
    list_time=[]
    list_error=[]
    list_pid=[]
    list_ip=[]
    list_code=[]
    list_object=[]
    list_description=[]
    
    c0=0
    
    for file0 in log_file_list:
        # print (os.path.splitext(file0)[1])
        if os.path.splitext(file0)[1] == '.gz':
            with gzip.open( file0, "rt") as fi:
            	for line in fi:
                    p=prase0(line)
                    
                    list_time.append( trans_datetime(p[0]) )
                    list_error.append(p[1])
                    list_pid.append(p[2])
                    list_ip.append(p[3])
                    list_code.append(p[4])
                    list_description.append(p[5])
                    list_object.append(p[6])
                    
                    c0 +=1
        else:
            with open( file0, "rt") as fi:
                for line in fi:
                    p=prase0(line)
                    
                    list_time.append( trans_datetime(p[0]) )
                    list_error.append(p[1])
                    list_pid.append(p[2])
                    list_ip.append(p[3])
                    list_code.append(p[4])
                    list_description.append(p[5])
                    list_object.append(p[6])
                	
                    c0 +=1
    
    
    # show input whole count
    print ('input line count ', c0)
    
    df0= pd.DataFrame( [ list_time, list_error, list_pid, list_ip, list_code,  list_description, list_object], index=['datetime','error','pid','ip', 'code','description','object'])
    #df0.values.tolist()
    df= df0.transpose()  # exchange row, column 
    
    #erase embellish
    df['pid']=df['pid'].str.replace('pid ','')
    df['ip']=df['ip'].str.replace('client ','')
    df['ip']=df['ip'].str.replace(':0','')
    df['error']=df['error'].str.replace(':error','')
    
    #save as a csv file
    df.to_csv('error_output_all.csv')
    
    # convert and save csv as output
    if 0:
        # convert ip2host  IPからhost名変換に失敗した場合はIPが入る。
        df3=df.copy()
        dic=get_ip(df3)
        df3=df3.replace(dic)
        dic_rename={'ip':'host'}
        df3=df3.rename(columns=dic_rename)
        df3.to_csv('error_output_ip-reversed.csv')
        
        print ('')
        print ('fnish') 