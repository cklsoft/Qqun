#!/usr/bin/python
# -*- coding: utf-8 -*-

import pyodbc
qqdict=[]
qcnt=0
class ODBC_QQ:
    def __init__( self, DRIVER, SERVER, DATABASE, UID, PWD):
        ''' initialization '''
        self.DRIVER = DRIVER
        self.SERVER = SERVER
        self.DATABASE = DATABASE
        self.UID = UID
        self.PWD = PWD

    def Connect(self):
        ''' Connect to the DB '''
        if not self.DATABASE:
            raise(NameError,"no setting db info")
        self.connect = pyodbc.connect(DRIVER=self.DRIVER, SERVER=self.SERVER, DATABASE=self.DATABASE, UID=self.UID, PWD=self.PWD, charset="UTF-8")

    def GetConnect( self ):
        return self.connect

    def closeConnect( self ):
        return self.connect.close()

    def fetchall( self, sql):
        ''' Perform one Sql statement '''
        cursor = self.connect.cursor() #get the handle
        cursor.execute(sql)
        rows = cursor.fetchall()
        return rows

    def ExecNoQuery(self,sql):
        ''' Person one Sql statement like write data, or create table, database and so on'''
        try:
            cursor = self.connect.cursor() #get the handle
            cursor.execute(sql)
            self.connect.commit()
        except Exception,e:
            print e
    def fetchone_cursor( self, sql ):
        ''' use less mem when one by one to read the data '''
        cursor = self.connect.cursor() #get the handle
        cursor.execute(sql)
        return cursor
    def getAllQQDataBaseName( self, condition ) :
        selectsql = "select name from sys.databases "
        names = self.fetchall( selectsql + condition )
        return names
    def getDataBaseAllTableName( self, databasename) :
        use = "USE "
        self.ExecNoQuery( use + databasename )
        select = "SELECT name FROM sys.objects Where Type='U' and name!='dtproperties'"
        names = self.fetchall( select )
        return names
    def getallDataBaseName( self ):
        conditionstr = "where name like 'GroupData%' "
        self.GroupDataNames = self.getAllQQDataBaseName(conditionstr)
        conditionstr = "where name like 'QunInfo%' "
        self.QunInfoNames = self.getAllQQDataBaseName(conditionstr)
    def getQunNumOfQQnumber( self, QQNumber):
        QunNumbers = []
        for dataname in self.GroupDataNames :
            print u"正在查找数据库：" + dataname[0]
            TableNames = self.getDataBaseAllTableName( dataname[0] )
            for tablename in TableNames :
                selectsql = "select QQNum, Nick, QunNum from " + tablename[0] + " where QQNum = "
                rows = self.fetchall( selectsql + QQNumber )
                if len( rows ) > 0:
                    print u"在该表找到该号码的群：" + tablename[0]
                    QunNumbers.extend(rows)
        return QunNumbers
    def getQunNumOfNick( self, Nick):
        QunNumbers = []
        f=open('c:\\res.txt','a')
        try:
            for dataname in self.GroupDataNames :
                print u"正在查找数据库：" + dataname[0]
                TableNames = self.getDataBaseAllTableName( dataname[0] )
                for tablename in TableNames :
                    selectsql = "select QQNum, Nick, QunNum from " + tablename[0] + " where Nick LIKE "
                    print selectsql + '\''+Nick+'\''
                    rows = self.fetchall( selectsql + '\''+Nick+'\'' )
                    if len( rows ) > 0:
                        print u"在该表找到该号码的群：" + tablename[0]
                        if not rows[0][0] in qqdict:
                            f.write(str(rows[0][0])+rows[0][1].decode('gb2312','ignore').encode('utf-8')+str(rows[0][2])+"\n")
                            qqdict.append(rows[0][0])
                            qcnt+=1
                            if qcnt%10==0:
                                f.close()
                                f=open('c:\\res.txt','a')
                        else:
                            continue
                        print str(rows[0][0])+str(rows[0][1]).decode('gbk')+str(rows[0][2])+"\n"
                        QunNumbers.extend(rows)
        finally:
            f.close()
        return QunNumbers
    def getQunNumOfNicks( self, Nicks):
        QunNumbers = []
        f=open('c:\\res.txt','a')
        global qqdic,qcnt
        try:
            for dataname in self.GroupDataNames :
                print u"正在查找数据库：" + dataname[0]
                TableNames = self.getDataBaseAllTableName( dataname[0] )
                for tablename in TableNames :
                    selectsql = "select QQNum, Nick, QunNum,Gender from " + tablename[0] + " where Nick in "
                    cnt=len(Nicks)
                    selectsql+='('
                    for p in Nicks:
                        selectsql+='\''+p+'\''
                        cnt-=1
                        if cnt>0:
                            selectsql+=','
                    selectsql+=')'
                    print selectsql
                    rows = self.fetchall( selectsql)
                    if len( rows ) > 0:
                        print u"在该表找到该号码的群：" + tablename[0]
                        if not rows[0][0] in qqdict:
                            f.write(rows[0][1].decode('gb2312','ignore').encode('utf-8')+'\t'+str(rows[0][0])+'\t'+str(rows[0][2])+'\t'+str(rows[0][3])+"\n")
                            qqdict.append(rows[0][0])
                            qcnt+=1
                            if qcnt%10==0:
                                f.close()
                                f=open('c:\\res.txt','a')
                        else:
                            continue
                        print str(rows[0][0])+str(rows[0][1]).decode('gbk')+str(rows[0][2])+"\n"
                        QunNumbers.extend(rows)
        finally:
            f.close()
        return QunNumbers
    def getQunMembersofQunNumber( self, QunNumber):
        QunMembers = []
        for dataname in self.GroupDataNames :
            print u"正在查找数据库：" + dataname[0]
            TableNames = self.getDataBaseAllTableName( dataname[0] )
            for tablename in TableNames :
                selectsql = "select min(QunNum), max(QunNum) from " + tablename[0]
                min_max = self.fetchall( selectsql)
                if min_max[0][0] != None : # avoid empty table
                    if int(min_max[0][0]) <= int(QunNumber) and int(QunNumber) <= int(min_max[0][1]) :
                        print u"在该表找到群成员：" + tablename[0]
                        selectsql = "select QQNum, Nick from " + tablename[0] + " where QunNum = "
                        rows = self.fetchall( selectsql + QunNumber )
                        if len( rows ) > 0:
                            QunMembers.extend(rows)
        return QunMembers
    def getQunInformation( self,  QunNumber):
        QunInformation = []
        hadFound = False
        for dataname in self.QunInfoNames :
            TableNames = self.getDataBaseAllTableName( dataname[0] )
            for tablename in TableNames :
                selectsql = "select * from " + tablename[0] + " where QunNum = "
                #selectsql = "select MastQQ, CreateDate, Title, QunText from " + tablename[0] + " where QunNum = "
                rows = self.fetchall( selectsql + str(QunNumber) )
                if len( rows ) > 0:
                    print u"在该表找到群信息：" + tablename[0]
                    QunInformation.extend(rows)
                    hadFound = True
                    break;
            if hadFound:
                break
        return QunInformation
    def createAllDataGroupIndex( self ):
        for dataname in self.GroupDataNames :
            if dataname[0] in ['GroupData1','GroupData10','GroupData11']:
                continue
            print u"正在为该数据库的所有表添加索引：" + dataname[0]
            TableNames = self.getDataBaseAllTableName( dataname[0] )
            for tablename in TableNames :
                indexsql = "CREATE INDEX QQNumIndex ON " + tablename[0] + "(QQNum)"
                print indexsql
                self.ExecNoQuery(indexsql)
    def createAllQunInfoIndex( self ):
        for dataname in self.QunInfoNames :
            print u"正在为该数据库的所有表添加索引：" + dataname[0]
            TableNames = self.getDataBaseAllTableName( dataname[0] )
            for tablename in TableNames :
                indexsql = "CREATE INDEX QunNumIndex ON " + tablename[0] + "(QunNum)"
                self.ExecNoQuery(indexsql)
def checkQQqun( QQ, QQnumber ):
    while 1 :
        QunNumbers = QQ.getQunNumOfQQnumber( QQnumber )
        print u"\nQQ号码    QQ昵称    QQ群    Q群人数    Q群名称    Q群公告"
        for qun in QunNumbers:
            QunInformation = QQ.getQunInformation( qun.QunNum )
            if len(QunInformation) > 0:
                print qun.QQNum, qun.Nick.decode('gb2312','ignore').encode('utf-8'), qun.QunNum,\
                QunInformation[0].MastQQ,\
                QunInformation[0].Title.decode('gb2312','ignore').encode('utf-8'),\
                QunInformation[0].QunText.decode('gb2312','ignore').encode('utf-8')
            else:
                print qun.QQNum, qun.Nick.decode('gb2312','ignore').encode('utf-8'), qun.QunNum
        handle = raw_input( u"\n是否继续查询(y/n):")
        if handle == "y" or handle == "Y":
            QQnumber =  raw_input(u"请输入你想查询的QQ号码：")
        else:
            break
def checkQQqun2( QQ, Nick ):
    while 1 :
        QunNumbers = QQ.getQunNumOfNicks( Nick )
        print u"\nQQ号码    QQ昵称    QQ群    Q群人数    Q群名称    Q群公告"
        for qun in QunNumbers:
            QunInformation = QQ.getQunInformation( qun.QunNum )
            if len(QunInformation) > 0:
                print qun.QQNum, qun.Nick.decode('gb2312','ignore').encode('utf-8'), qun.QunNum,\
                QunInformation[0].MastQQ,\
                QunInformation[0].Title.decode('gb2312','ignore').encode('utf-8'),\
                QunInformation[0].QunText.decode('gb2312','ignore').encode('utf-8')
            else:
                print qun.QQNum, qun.Nick.decode('gb2312','ignore').encode('utf-8'), qun.QunNum
        handle = raw_input( u"\n是否继续查询(y/n):")
        if handle == "y" or handle == "Y":
            Nick =  raw_input(u"请输入你想查询的QQ号码：")
        else:
            break
def checkQunMembers( QQ, Qunnumber ):
    QunMembers = QQ.getQunMembersofQunNumber( Qunnumber )
    print u"\n以下信息为群内的QQ号码+QQ昵称："
    for Member in QunMembers:
        print Member.QQNum,Member.Nick.decode('gb2312','ignore').encode('utf-8')
def checkQunInformation( QQ, Qunnumber ):
    QunInformation = QQ.getQunInformation( Qunnumber )
    if len(QunInformation) == 0:
        print u"\n!!!!!!!!!!!上百G的数据库里面没该群的信息!!!!!!!!!"
        return
    print u"\n群号码    群人数    建群时间    群昵称    Class（不知道是什么）    群公告："
    print QunInformation[0].QunNum, QunInformation[0].MastQQ, QunInformation[0].CreateDate,\
    QunInformation[0].Title.decode('gb2312','ignore').encode('utf-8'),  QunInformation[0].Class,\
    QunInformation[0].QunText.decode('gb2312','ignore').encode('utf-8')
def main():
    QQ = ODBC_QQ('{SQL SERVER}', r'127.0.0.1', 'master', 'sa',  '123456789')
    QQ.Connect()
    QQ.getallDataBaseName()
   # QQ.createAllDataGroupIndex()#you should use the function once when you first time attach the QQ database
   # QQ.createAllQunInfoIndex()
    while 1 :
        print u"\n1---根据QQ号码，查询QQ号码所在的群"
        print u"2---根据群号，查询群成员"
        print u"3---根据群号，查询群信息"
        print u"4---根据群昵称，查询群成员"
        print u"其他输入为退出本系统"
        handle = raw_input(u"输入你想要的操作类型（1 or 2 or 3 or 4 other）：".encode('gbk'))
        if 1 == int(handle):
            QQnumber = raw_input(u"请输入你想查询的QQ号码：".encode('gbk'))
            checkQQqun( QQ, QQnumber)
        elif 2 == int(handle):
            Qunnumber = raw_input(u"请输入你想查询的QQ群号：".encode('gbk'))
            checkQunMembers( QQ, Qunnumber)
        elif 3 == int(handle):
            Qunnumber = raw_input(u"请输入你想查询的QQ群号：".encode('gbk'))
            checkQunInformation( QQ, Qunnumber)
        elif 4 == int(handle):
            global qqdic,qcnt
            qqdict=[]
            qcnt=0
            p = raw_input(u"请输入你想查询的QQ在群中的昵称：".encode('gbk'))
            Nicks=p.decode('gb2312').split()
            checkQQqun2( QQ, Nicks)
            print qqdict
        else:
            break
    QQ.closeConnect()
if __name__ == '__main__':
    #print ('\xb2\xd9\xd7\xf7\xca\xa7\xb0\xdc\xa3\xac\xd2\xf2\xce\xaa\xd4\xda \xb1\xed'+ 'Group174'+' \xc9\xcf\xd2\xd1\xb4\xe6\xd4\xda\xc3\xfb\xb3\xc6\xce\xaa '+'QQNumIndex'+'\xb5\xc4\xcb\xf7\xd2\xfd\xbb\xf2\xcd\xb3\xbc\xc6\xd0\xc5\xcf\xa2\xa1\xa3').decode('gbk')
    main()
