This connection library enables us to connect to Postgres server and execute queries. The Usage is as shown below.

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

