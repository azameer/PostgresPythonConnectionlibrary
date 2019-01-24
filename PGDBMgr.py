"""

This modules connects to the Postgres server and executes queries. The following are the inputs that are required to connect to Postgres server. 
dbname: Optional
userName: Mandatory
password: Optional
hostname: Optional ( defaults to local host )
port: optional
log: Mandatory

INPUTS: 
1) User credentials to PGDBMgr class should be passed as dictionary. ie: 
pgresobj=PGDBMgr({'username':'postgres','password':'postgres','logger':log,'dbname':'postgres'})
pgresobj=PGDBMgr({'username':'postgres','logger':log})

Log object should be of FileLogger class.

QRY. OUTPUT: 
1) List of tuples in case of SELECT

LIMITATIONS:
1) This module works only on server side and a connection from client side cannot be made.


WRAPPER:
#pgresobj=PGDBMgr({'username':'postgres','password':'postgres','logger':log,'dbname':'postgres'})
pgresobj=PGDBMgr({'username':'postgres','logger':log})
ConnHandle,msg=pgresobj.PGConnect()
print ("Connection object handle %s"%ConnHandle)
if ConnHandle != -1:
    (Status,Output)=pgresobj.PGUpdateQry("DROP table COMPANY;")
    (Status,Output)=pgresobj.PGUpdateQry("CREATE TABLE COMPANY(ID INT PRIMARY KEY NOT NULL,NAME TEXT    NOT NULL,AGE INT NOT NULL, ADDRESS  CHAR(50),SALARY REAL);")
    (Status,Output)=pgresobj.PGUpdateQry("INSERT INTO COMPANY (ID,NAME,AGE,ADDRESS,SALARY) values (001,'Kodiak',15,'Mfar Manyata Bangalore',200.5);")
    (Status,Output)=pgresobj.PGUpdateQry("INSERT INTO COMPANY (ID,NAME,AGE,ADDRESS,SALARY) values (002,'Motorola',02,'Mfar Manyata Bangalore 02',600.5);")
    (Status,Output)=pgresobj.PGUpdateQry("INSERT INTO COMPANY (ID,NAME,AGE,ADDRESS,SALARY) values (003,'Crass Systems',03,'Mfar Manyata Bangalore 03',800.5);")
    pgresobj.PGCommit()
    (Status,Output,msg)=pgresobj.PGFetchDataQry("SELECT * FROM COMPANY;",1)

"""

import sys
sys.path.append("/DG/activeRelease/lib/python_lib/")
import psycopg2

class PGDBMgr:
    def __init__(self,inputparamdict):
            ## Check if key exists & update conn. object
            ## Username is mandatory
            self.dbname = ''
            self.password = ''
            self.host = ''
            self.port = ''
            self.username = ''
            self.dbobj=None
            self.cursor=None
            self.retry_counter=0
            self.logenable=0        ### If 1(Default) log in logfile relevent to passed logobject  
            
            if inputparamdict.get('dbname'): self.dbname=inputparamdict['dbname'] 
            if inputparamdict.get('username'): self.username=inputparamdict['username'] ## Mandatory
            if inputparamdict.get('password'): self.password=inputparamdict['password']
            if inputparamdict.get('hostname'): self.host=inputparamdict['hostname']
            if inputparamdict.get('port'): self.port=inputparamdict['port']
            if 'logger' in inputparamdict.keys():
               self.log=inputparamdict['logger']
               self.logenable=1

    def PrintLogMsg(self,msg,Flag):
        if Flag == 0: 
            self.log.info(msg)
        else: 
            self.log.error(msg)
    
    def PGRetryCount(self,retryvalue):
        self.retry_counter=retryvalue
        msg="SUCCESS: Set Retry count to %s "%int(self.retry_counter)
        if self.logenable == 1:self.PrintLogMsg(msg,0)
        return 0,msg
    
    def PGConnect(self):
        self.dbobj=-1
        try:
            ## username is mandatory
            self.dbobj=psycopg2.connect(database=self.dbname,user=self.username,password=self.password, host=self.host, port=self.port)
            msg='SUCCESS: Connection to Postgres Svr. established'
            if self.logenable == 1:self.PrintLogMsg(msg,0)
        except psycopg2.Error as error:
            msg="FAILURE: Failed to Connect to Postgres Svr. %s"%str(error)
            if self.logenable == 1:self.PrintLogMsg(msg,1)
            return -1,msg
        self.cursor=self.dbobj.cursor()
        return 0,msg   # Return cur=conn.cursor()

    def PGReconnect(self):
       for i in range(0,self.retry_counter):
            msg="Reconnection Cnt: %s"%str(i+1)
            if self.logenable == 1:self.PrintLogMsg(msg,0)
            if(self.dbobj != -1):
                self.PGDisConnect()
                msg="Existing Postgres Connection Closed. Reconnecting ....."
                if self.logenable == 1:self.PrintLogMsg(msg,0)
            status=self.PGConnect()
            msg="Estabilishing Postgres Svr. Connection Again"
            if self.logenable == 1:self.PrintLogMsg(msg,0)
            if(status!=-1):
                msg="SUCCESS: Successfully Reconnected to Postgres SQL at Cnt %s"%str(i+1)
                if self.logenable == 1:self.PrintLogMsg(msg,0)
                return 0,msg
       msg="FAILURE: Retry count over. Failed to reconnect to Postgres Svr."
       if self.logenable == 1:self.PrintLogMsg(msg,1)
       return -1,msg

    def PGFetchDataQry(self,qrystrng):
        try:
           self.cursor.execute(qrystrng)
           data_list=self.cursor.fetchall()
           msg="Successfully Executed Qry %s: "%str(qrystrng)
           if self.logenable == 1:self.PrintLogMsg(msg,0)
           return 0,data_list,msg
        except psycopg2.Error as exc:
            err=exc
            msg="ERROR: Execution error. Calling Rollback %s: "%str(err)
            if self.logenable == 1:self.PrintLogMsg(msg,1)
            self.PGRollback()
            data_list = []
            return -1,data_list,msg

    def PGUpdateQry(self,qrystrng):
        try:
           self.cursor.execute(qrystrng)
           msg="Successfully Executed Update Qry: "+str(qrystrng)
           if self.logenable == 1:self.PrintLogMsg(msg,0)
           return 0,msg
        except psycopg2.Error as exc:
            err=exc
            msg="ERROR:Updation Qry Execution error. Calling Rollback %s: "%str(err)
            if self.logenable == 1:self.PrintLogMsg(msg,1)
            self.PGRollback()
            return -1,msg 

    def PGRollback(self):
        try:
            self.dbobj.rollback()
            msg="SUCCESS: Rollback"
            if self.logenable == 1:self.PrintLogMsg(msg,0)
            return 0,msg
        except psycopg2.Error as exc:
            err=exc
            msg="ERROR: Rollback Execution error.  %s: "%str(err)
            if self.logenable == 1:self.PrintLogMsg(msg,1)
            return -1,msg
 
    def PGCommit(self):
        try:
            msg="Committing to Postgres DB ....."
            self.dbobj.commit()
            if self.logenable == 1:self.PrintLogMsg(msg,0)
            return 0,msg
        except psycopg2.Error as exc:
            err=exc
            msg="ERROR: Committing to Postgres DB.  %s: "%str(err)
            if self.logenable == 1:self.PrintLogMsg(msg,1)
            return -1,msg 
    
    def PGDisConnect(self):
        try:
            msg="Closing all connection handles/Disconnecting from Postgres Svr ...."
            self.dbobj.cursor().close()
            self.dbobj.close()
            if self.logenable == 1:self.PrintLogMsg(msg,0)
            return 0,msg
        except psycopg2.Error as exc:
            err=exc
            msg="ERROR: Closing all connection handles/Disconnecting from Postgres Svr %s....: "%str(err)
            if self.logenable == 1:self.PrintLogMsg(msg,1)
            return -1,msg

## Changed on 4/20/2018 4:55
