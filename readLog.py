#coding:utf-8

#
# A trial of read apache log and  save as a csv file about specified URL address access information only.
#
"""
（概要）
Apache のログファイルを読み込んで、指定した特定のURLアドレスだけのアクセス情報を、 csvファイルに書き出す、試作品。
書き出した csvファイルを 、後で、excel等で読み込んでアクセス内容を分析することを想定している。

（例）
python3 readLog.py -w "Logs/" -a "/, /index.html" 

-w で　apacheのログファイルのあるディレクトリーを指定する。
-a で　（先頭を除いた）URLアドレスを指定する。

IPからhost名に変換した　output_ip-reversed.csv　又は、IPが入った　output.csv　が出力される。( if 1: 又は　if 0:で切り替え）

（注意）
Logのフォーマットに合わせて、パーサに渡す format　を定義すること。
ログファイルは名称の中に　log　を含むものを対象としている。拡張子.gzも想定。
リターンコードが200のみを抽出。
IPからhost名変換に失敗した場合はIPが入る。
小さい規模の入力を想定している。リスト追加など処理が遅い部分があり大規模な用途には不向き。
画像データなど(html以外）の除外が不完全。
botの除外が不完全。

"""
#Check version
# Python 3.6.4, 64bit on Win32 (Windows 10)
# numpy (1.14.0)
# pandas (0.23.4)
# apachelog (1.1)

import os
import sys
import argparse
import socket
import glob
import gzip
import numpy as np
import pandas as pd
import apachelog2
"""
You can get apachelog.py from the [apachelog](http://code.google.com/p/apachelog/) project on Google Code of which License is Artistic License/GPL.
In order to use it in Python 3.x, change from    except Exception, e:    to   except Exception as e:   at line 170.
Rename it to apachelog2.py and put it in the same  directory of this py source.

"""


def erase_bot(df, path='test_unclude_bot.csv'):
    # erase line that includes bot in User-Agent
    df2=df[  ~( df['agent'].str.contains('bot')              |   df['agent'].str.contains('Bot'))]  # use ~, not use "not" or !
    df2=df2[ ~(df2['agent'].str.contains('Hatena-Favicon2')  |  df2['agent'].str.contains('com.google.GoogleMobile'))]  # use ~, not use "not" or !
    df2=df2[ ~(df2['agent'].str.contains('Google Favicon')   |  df2['agent'].str.contains('spider'))]  # use ~, not use "not" or !
    if path is not None:
        df2.to_csv(path)
    return df2
    
def get_code_200(df, path='test_code_200.csv'):
    # get line that includes 200 in code
    df2=df[ (df['code'].str.contains('200'))]
    if path is not None:
        df2.to_csv(path)
    return df2

def erase_hit_figure(df, path='test_unclude_hit_figure.csv'):
    # erase line that includes figure
    df2=df[  ~( df['get'].str.contains('.jpg')  |   df['get'].str.contains('.gif'))]  # use ~, not use "not" or !
    df2=df2[ ~(df2['get'].str.contains('.png')  |  df2['get'].str.contains('.zip'))]  # use ~, not use "not" or !
    df2=df2[ ~(df2['get'].str.contains('.bmp')  |  df2['get'].str.contains('.pdf'))]  # use ~, not use "not" or !
    df2=df2[ ~(df2['get'].str.contains('.wav')  |  df2['get'].str.contains('.mp3'))]  # use ~, not use "not" or !
    df2=df2[ ~(df2['get'].str.contains('.ico')  |  df2['get'].str.contains('.css'))]  # use ~, not use "not" or !
    if path is not None:
        df2.to_csv(path)
    return df2

def get_html(df, label, path='test_html.csv'):
    # get line that includes specified label
    df2=df[ df['get'].str.contains(label)  ]
    if path is not None:
        df2.to_csv(path)
    return df2

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
        dic0[ list0[i] ]= ip_reverse_lookup(list0[i])
    
    return dic0

def local_ip2host(df):
    # change ip to host, locally
    #df.ip[ df['ip'].str.startswith('192.168.1.1') ]='xxxx'
    pass

def get_files( path ):
    # get log file name list
	return glob.glob( path + "*log*")


if __name__ == "__main__":
    
    parser = argparse.ArgumentParser(description='apachelog reading')
    parser.add_argument('--log_file', '-w', default='logs/', help='specify log file directory')
    parser.add_argument('--address0', '-a', default='/,/index.html', help='specify URL address')
    args = parser.parse_args()
    
    # Log Output format Choice:
    #
    # (1)
    # format = r'%h %l %u %t \"%r\" %>s %b \"%{Referer}i\" \"%{User-Agent}i\"'
    
    # (2) Virtual Host + (1)
    format = r'%v %h %l %u %t \"%r\" %>s %b \"%{Referer}i\" \"%{User-Agent}i\"'
    
    p = apachelog2.parser(format)
    
    dic0={}
    
    list_ip=[]
    list_site=[]
    list_time=[]
    list_get=[]
    list_code=[]
    list_agent=[]
    list_refer=[]
    
    log_file_list=get_files(args.log_file)
    #print ( log_file_list)
    
    c0=0
    
    for file0 in log_file_list:
        # print (os.path.splitext(file0)[1])
        if os.path.splitext(file0)[1] == '.gz':
            with gzip.open( file0, "rt") as fi:
            	for line in fi:
                    try:
                        data = p.parse(line)
                        #
                        list_ip.append(data['%h'])
                        list_time.append(data['%t'])
                        list_get.append(data['%r'])
                        list_code.append(data['%>s'])
                        list_agent.append(data['%{User-Agent}i'])
                        list_refer.append(data['%{Referer}i'])
                        
                    except:
                        print("error: unable to parse %s" % line)
                    c0 +=1
        else:
            with open( file0, "rt") as fi:
                for line in fi:
                    try:
                        data = p.parse(line)
                        #
                        list_ip.append(data['%h'])
                        list_time.append(data['%t'])
                        list_get.append(data['%r'])
                        list_code.append(data['%>s'])
                        list_agent.append(data['%{User-Agent}i'])
                        list_refer.append(data['%{Referer}i'])
                        
                    except:
                        print("error: unable to parse %s" % line)
                    c0 +=1
                    
    # show input whole count
    print ('input line count ', c0)
    
    df0= pd.DataFrame( [ list_ip, list_time, list_get, list_code, list_refer, list_agent], index=['ip','time','get','code','refere','agent'])
    #df0.values.tolist()
    df= df0.transpose()  # exchange row, column 
    
    # erase bot, select only code 200, and erase hit figure
    df=erase_bot(df, path=None)
    df=get_code_200(df, path=None)
    df=erase_hit_figure(df, path=None)
    if 0:
        df.to_csv('mid_output_all.csv')
    
    # get specified address line
    for i, addres0 in enumerate(args.address0.split(',')):
        label= 'GET ' + addres0.strip(' ') + ' HTTP'
        print ('specified url address ', label)
        df2=get_html(df, label, path=None)
        
        if i == 0:
            df3=df2.copy()
        else:
            df3=pd.concat([df3,df2])
    
    # convert and sav csv as output
    if 1:
        # convert ip2host
        dic=get_ip(df3)
        df3=df3.replace(dic)
        local_ip2host(df3)
        dic_rename={'ip':'host'}
        df3=df3.rename(columns=dic_rename)
        df3.to_csv('output_ip-reversed.csv')
    else:
        df3.to_csv('output_.csv')
