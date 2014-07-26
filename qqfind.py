#!/usr/bin/env python
# -*- coding: utf-8 -*-
import urllib,urllib2,threading,Queue,socket,cookielib,time,thread,json
from time import sleep
Q=Queue.Queue()
class mThread(threading.Thread):
    def addGETdata(self,url,data):
        return url+'?'+urllib.urlencode(data)
    def __init__(self,name):
        threading.Thread.__init__(self)
        self.name=name
        self.lock=thread.allocate_lock()
    def run(self):
        global Q
        url=r'http://cgi.find.qq.com/qqfind/buddy/search_v3'
        params={
            'num':20,
            'page':0,
            'sessionid':0,
            'keyword':88620720,
            'agerg':0,
            'sex':0,
            'firston':1,
            'video':0,
            'country':0,
            'province':0,
            'city':0,
            'district':0,
            'hcountry':0,
            'hprovince':0,
            'hcity':0,
            'hdistrict':0,
            'online':1,
            'ldw':1142599158
        }
        socket.setdefaulttimeout(20)
        self.cookie=cookielib.LWPCookieJar()
        self.opener=urllib2.build_opener(urllib2.HTTPCookieProcessor(self.cookie))
        urllib2.install_opener(self.opener)
        while True:
            self.opener.addheaders = [("Cookie",'itkn=993570040; ptisp=ctc; RK=110vvydhQ+; ptcz=4962a32e97e94ff34ba7faeb32d9f68462222a9b7f037bb2a73a3d04c1ff7afb; pt2gguin=o2104356310; uin=o2104356310; skey=@6FelCALas')]
            #            self.opener.addheaders = [("User-agent",'Mozilla/5.0 (Windows; U; Windows NT 5.1; it; rv:1.8.1.11) Gecko/20071127 Firefox/2.0.0.11'),("Accept","*/*")]
            if Q.qsize()==0:
                break
            p=Q.get()
            h=p.split('\t')
            if h[2]=='0\n':
                continue
            params['keyword']=h[0]
            count=0
            while True:
                try:
                    q=urllib2.urlopen(url,urllib.urlencode(params),timeout=50000000)
                    s=q.read()
                    js=json.loads(s)
                    if js['retcode']!=0:
                        continue
                    x=str(h[0]+' '+js['result']['buddy']['info_list'][0]['city'].encode('gbk')+' '+js['result']['buddy']['info_list'][0]['province'].encode('gbk')+' '+str(js['result']['buddy']['info_list'][0]['birthday']['year']))
                    print self.name,
                    self.lock.acquire()
                    w=open('c:\\qmsg.txt','a')
                    w.write(x+'\n')
                    w.close()
                    self.lock.release()
                    Q.task_done()
                    sleep(1)
                    break
                except Exception,e:
                    print e
                    Q.task_done()
                    sleep(2)
                    continue
if __name__=='__main__':
    global Q
    f=open('c:\\msg.txt')
    for p in f.readlines():
        Q.put(p)
    f.close()
    for i in range(10):
        mThread(i).start()
