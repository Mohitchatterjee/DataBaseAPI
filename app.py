import json
import pip
from flask import Flask, render_template, request, jsonify
import pandas as pd
import mysql.connector as conn
import pymongo
import cassandra
import logging
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider


class cassandraClass:
    logging.basicConfig(filename='cassandraConnect.log', level=logging.DEBUG, format='%(asctime)s %(levelname)s %(message)s')

    def __init__(self, cassandraApp):
        self.cassandraApp = cassandraApp

    def connectCassandra(self,clientId,clientSecret,zipPath):
        self.clientId = clientId
        self.clientSecret = clientSecret
        self.zipPath = zipPath
        try:
            cloud_config = {'secure_connect_bundle': self.zipPath}
            auth_provider = PlainTextAuthProvider(self.clientId,self.clientSecret)
            cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
            session = cluster.connect()
            row = session.execute("select release_version from system.local").one()
            logging.info("Initialize Cassandra...")
            return 'Done'
        except Exception as e:
            logging.info('Not Cassandra Connected..')
            return (str(e))

    def cassandraShowDb(self):
        try:
            cloud_config = {'secure_connect_bundle': self.zipPath}
            auth_provider = PlainTextAuthProvider(self.clientId,self.clientSecret)
            cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
            session = cluster.connect()
            row = session.execute("SELECT * FROM system_schema.keyspaces;")
            dbList = []
            for r in row:
                dbList.append(r[0])
            return dbList
        except Exception as e:
            return str(e)

    def cassandraCreateTable(self,dbName,column):
            try:

                self.dbName = dbName
                self.column = column
                cloud_config = {'secure_connect_bundle': self.zipPath}
                auth_provider = PlainTextAuthProvider(self.clientId, self.clientSecret)
                cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
                session = cluster.connect(self.dbName)

                row = session.execute("use "+self.dbName+";")
                row = session.execute(self.column+";")
                return 'Done'
            except Exception as e:
                logging.info('Error for table creation')
                return str(e)

    def cassandraOnlyOneInsertion(self,dbName,table,column,values):
        try:
            self.dbName = dbName
            self.table = table
            self.column = column
            self.values = values

            cloud_config = {'secure_connect_bundle': self.zipPath}
            auth_provider = PlainTextAuthProvider(self.clientId, self.clientSecret)
            cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
            session = cluster.connect(self.dbName)

            row = session.execute("use " + self.dbName + ";")

            row = session.execute("INSERT INTO "+self.dbName+"."+self.table+self.column+" VALUES "+self.values+";")
            return "Insertion Done"

        except Exception as e:
            return str(e)

    def cassandraBulkOneInsertion(self,dbName,table,columns,values):
        try:
            self.dbName = dbName
            self.table = table
            self.columns = columns
            self.values = values

            cloud_config = {'secure_connect_bundle': self.zipPath}
            auth_provider = PlainTextAuthProvider(self.clientId, self.clientSecret)
            cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
            session = cluster.connect(self.dbName)

            row = session.execute("use " + self.dbName + ";")
            val = (self.values).split(";")
            for v in val:
                row = session.execute("insert into " + self.dbName + "." + self.table + self.columns+" values(" + v + ")")

            return "Insertion Done"

        except Exception as e:
            return str(e)

    def downloadCassandraData(self,dbName,table,fileName):
        try:
            self.dbName = dbName
            self.table = table
            self.fileName = fileName
            cloud_config = {'secure_connect_bundle': self.zipPath}
            auth_provider = PlainTextAuthProvider(self.clientId, self.clientSecret)
            cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
            session = cluster.connect(self.dbName)
            query = "select * from "+self.dbName+"."+self.table+";"
            df = pd.DataFrame(list(session.execute(query)))
            df.to_csv(self.fileName + ".csv")
            return 'Done'
        except Exception as e:
            return str(e)



class mongoDbClass:
    logging.basicConfig(filename='mongoConnect.log', level=logging.DEBUG, format='%(asctime)s %(levelname)s %(message)s')

    def mongoLogin(self, mongoApp, hostName, userName,passwd):
        self.mongoApp = mongoApp
        self.hostName = hostName
        self.userName = userName
        self.passwd = passwd
        try:
            client = pymongo.MongoClient(self.hostName)
            db = client.test
            return 'All initialization is done in MongoDB'
        except Exception as e:
            return "Failed"

    def createMongoDB(self,dbName, collectionName, records):
        try:
            self.dbName = dbName
            self.collectionName = collectionName
            self.records = records

            myclient = pymongo.MongoClient(self.hostName)
            mydb = myclient[self.dbName]

            collection = mydb[collectionName]
            record = self.records
            collection.insert_one(record)
            return "Insertion Done in MongoDB"

        except Exception as e:
            return str(e)

    def insertMongoData(self,dbName,collectionName,records):
        try:
            self.dbName = dbName
            self.collectionName = collectionName
            self.records = records
            myclient = pymongo.MongoClient(self.hostName)
            mydb = myclient[self.dbName]
            dblist = myclient.list_database_names()
            if self.dbName in dblist:
                collection = mydb[collectionName]
                record = self.records
                collection.insert_one(record)
                return "Insertion Done in MongoDB"
            else:
                return "DataBase Does Not Exist"
        except Exception as e:
            return str(e)
    def showMongoDB(self):
        myclient = pymongo.MongoClient(self.hostName)
        dblist = myclient.list_database_names()
        return dblist

    def downloadMongoDBData(self,dbName,collectionName,fileName):
        myclient = pymongo.MongoClient(self.hostName)
        self.dbName = dbName
        self.collectionName = collectionName
        self.fileName = fileName
        dblist = myclient.list_database_names()
        if self.dbName in dblist:
            db = myclient[self.dbName]
            col = db[self.collectionName]
            datas =  col.find()
            dataList = []
            for data in datas:
                dataList.append(data)
            df = pd.DataFrame(dataList)
            df.to_csv(self.fileName + ".csv")
            return "Downloaded Successfully"
        else:
            return "DataBase Does Not Exist"




class sqlClass:
    logging.basicConfig(filename='sqlConnect.log', level=logging.DEBUG, format='%(asctime)s %(levelname)s %(message)s')

    def sqlLogin(self, app, host, user, passwd):
        self.app = app
        self.host = host
        self.user = user
        self.passwd = passwd
        logging.info("All initialization is done.")


        try:
            mydb = conn.connect(host=self.host, user=self.user, passwd=self.passwd)
            cursor = mydb.cursor()
            return 'Done'
        except Exception as e:
            return "Failed"
            logging.exception("Not Able to connect ", e)

    def showsqlDB(self):
        try:
            mydb = conn.connect(host=self.host, user=self.user, passwd=self.passwd)
            cursor = mydb.cursor()
            cursor.execute('show databases')
            showDB = cursor.fetchall()

            return showDB
        except Exception as e:
            return "Failed"
            logging.exception("Not Able to Fetch DataBases ", e)

    def createsqlDB(self,newDBName):
        try:
            self.newDBName = newDBName
            mydb = conn.connect(host=self.host, user=self.user, passwd=self.passwd)
            cursor = mydb.cursor()
            cursor.execute('create database '+self.newDBName)
            return 'DataBase created Successful'
        except Exception as e:
            logging.exception("Not Able to Create DataBases due to ", e)
            return "Failed"

    def create_sql_table(self,dbName,tableName):
        try:
            self.dbName = dbName
            self.tableName = tableName
            mydb = conn.connect(host=self.host, user=self.user, passwd=self.passwd)
            cursor = mydb.cursor()
            cursor.execute(
                "create table "+self.dbName+"."+self.tableName+"(EmpID INT(10),Name VARCHAR(20),Role VARCHAR(20),Salary INT(10),FTE VARCHAR(3))")
            logging.info("Table is created..")
            return 'Done'
        except Exception as e:
            logging.exception("Table Creation may Occure problem ", e)
            return str(e)

    def onlyOneInsertSql(self, dbName, tableName, values):
        try:
            self.dbName = dbName
            self.tableName = tableName
            self.values = values
            mydb = conn.connect(host=self.host, user=self.user, passwd=self.passwd)
            cursor = mydb.cursor()

            cursor.execute(
                "insert into "+ self.dbName+"."+self.tableName+" values("+self.values+")")
            logging.info("Table is created..")

            mydb.commit()
            return 'Done'
        except Exception as e:
            logging.exception("Table Creation may Occure problem ", e)
            return str(e)


    def bulkInsetionSql(self, dbName, tableName, values):
        try:
            self.dbName = dbName
            self.tableName = tableName
            self.values = values
            mydb = conn.connect(host=self.host, user=self.user, passwd=self.passwd)
            cursor = mydb.cursor()

            for value in values:
                cursor.execute(
                    "insert into "+ self.dbName+"."+self.tableName+" values("+value+")")
                logging.info("Table is created..")
            mydb.commit()
            return 'Done'
        except Exception as e:
            logging.exception("Table Creation may Occure problem ", e)
            return str(e)

    def downloadSQLData(self, dbName, tableName,fileName):
        try:
            self.dbName = dbName
            self.tableName = tableName
            self.fileName = fileName
            mydb = conn.connect(host=self.host, user=self.user, passwd=self.passwd)
            cursor = mydb.cursor()
            sql_query = pd.read_sql("select * from "+self.dbName+"."+self.tableName, mydb)
            df = pd.DataFrame(sql_query)
            df.to_csv(self.fileName+".csv")
            return 'Done'
        except Exception as e:
            logging.exception("Table Creation may Occure problem ", e)
            return str(e)

appObj = Flask(__name__)

app = sqlClass()
mongoApp = mongoDbClass()
cassandraApp = cassandraClass(appObj)


#----------------------------------------------------------------SQL ALL API-------------------------------------------------------------------------------

@appObj.route('/', methods=['GET', 'POST']) # To render Homepage
def home_page():
    return render_template('homeTemplates/index.html')

@appObj.route('/sqlTemplates/sqlLoginPg.html', methods=['POST'])
def sqlloginPg1():
    return render_template('/sqlTemplates/sqlLoginPg.html')

@appObj.route('/sqlTemplates/sql_pg1.html', methods=['POST'])
def sql_pg1():
    return render_template('sqlTemplates/sql_pg1.html')

@appObj.route('/sqlTemplates/sql_pg2.html', methods=['POST'])
def sql_pg2():
    return render_template('sqlTemplates/sql_pg2.html')

@appObj.route('/sqlTemplates/createTable.html', methods=['POST'])
def sql_createTable_html():
    return render_template('sqlTemplates/createTable.html')

@appObj.route('/sqlTemplates/onlyOneInsertion.html', methods=['POST'])
def sql_onlyOneInsertion_html():
    return render_template('sqlTemplates/onlyOneInsertion.html')

@appObj.route('/sqlTemplates/bulkInsertion.html', methods=['POST'])
def sql_bulkInsertion_html():
    return render_template('sqlTemplates/bulkInsertion.html')

@appObj.route('/sqlTemplates/downloadSql.html', methods=['POST'])
def sql_downloadData_html():
    return render_template('sqlTemplates/downloadSql.html')


@appObj.route('/sqlTemplates/sqlLogin', methods=['POST'])  # This will be called from UI
def sqlLogin():

    hostname = request.form['hostname']
    username = request.form['username']
    password = request.form['password']

    val = app.sqlLogin(appObj,hostname,username,password)
    if val == None:
        return render_template('homeTemplates/dbFailurePg.html', result=val)
    else:
        return render_template('sqlTemplates/sql_pg1.html', result=val)

@appObj.route('/sqlTemplates/sql_createDB', methods=['POST'])  # This will be called from UI
def sql_createDB():

    dbName = request.form['dbName']
    val = app.createsqlDB(dbName)
    if val == None:
        return render_template('homeTemplates/dbFailurePg.html', result=val)
    else:
        return render_template('homeTemplates/dbSuccessPg.html', result=val)

@appObj.route('/sqlTemplates/sql_showDB', methods=['POST'])  # This will be called from UI
def sql_showDB():
    val = app.showsqlDB()
    if val == None:
        return render_template('homeTemplates/dbFailurePg.html', result=val)
    else:
        return render_template('homeTemplates/dbSuccessPg.html', result=val)

@appObj.route('/sqlTemplates/sql_createTable', methods=['POST'])  # This will be called from UI
def sql_createTable():

    dbName = request.form['dbName']
    tableName = request.form['tableName']
    val = app.create_sql_table(dbName,tableName)
    if val == None:
        return render_template('homeTemplates/dbFailurePg.html', result=val)
    else:
        return render_template('homeTemplates/dbSuccessPg.html', result=val)


@appObj.route('/sqlTemplates/sql_onlyOneInsert', methods=['POST'])  # This will be called from UI
def sql_onlyOneInsertion():

    dbName = request.form['dbName']
    tableName = request.form['tableName']
    values = request.form['Values']
    val = app.onlyOneInsertSql(dbName,tableName, values)
    if val == None:
        return render_template('homeTemplates/dbFailurePg.html', result=val)
    else:
        return render_template('homeTemplates/dbSuccessPg.html', result=val)


@appObj.route('/sqlTemplates/sql_bulkInsert', methods=['POST'])  # This will be called from UI
def sql_bulkInsertion():

    #valuesList = []
    dbName = request.form['dbName']
    tableName = request.form['tableName']
    values = request.form['Values']
    valuesList = values.split(';')

    val = app.bulkInsetionSql(dbName,tableName, valuesList)
    if val == None:
        return render_template('homeTemplates/dbFailurePg.html', result=val)
    else:
        return render_template('homeTemplates/dbSuccessPg.html', result=val)

@appObj.route('/sqlTemplates/downloadSqlData', methods=['POST'])  # This will be called from UI
def sql_downloadData():


    dbName = request.form['dbName']
    tableName = request.form['tableName']
    filename = request.form['fileName']
    val = app.downloadSQLData(dbName,tableName,filename)
    if val == None:
        return render_template('homeTemplates/dbFailurePg.html', result=val)
    else:
        return render_template('homeTemplates/dbSuccessPg.html', result=val)






#---------------------------------------------------------------------CASSANDRA ALL API-----------------------------------------------------------------------------------------------


@appObj.route('/cassandraTemplates/cassandraConnectivityPg1.html', methods=['POST'])
def cassandra_pg1():
    return render_template('cassandraTemplates/cassandraConnectivityPg1.html')

@appObj.route('/cassandraTemplates/cassandraCreateTable.html', methods=['POST'])
def cassandra_pg2():
    return render_template('/cassandraTemplates/cassandraCreateTable.html')

@appObj.route('/cassandraTemplates/cassandraInsertionPg3.html', methods=['POST'])
def cassandra_pg3():
    return render_template('/cassandraTemplates/cassandraInsertionPg3.html')


@appObj.route('/cassandraTemplates/cassandraInsertionPg4.html', methods=['POST'])
def cassandra_pg4():
    return render_template('/cassandraTemplates/cassandraInsertionPg4.html')

@appObj.route('/cassandraTemplates/downloadCassandraData.html', methods=['POST'])
def cassandra_downloadData_html():
    return render_template('/cassandraTemplates/downloadCassandraData.html')

@appObj.route('/cassandraTemplates/insertDataCassandra', methods=['POST'])  # This will be called from UI
def inserCassandraDataPg():
    dbName = request.form['dbName']
    table = request.form['table']
    column = request.form['column']
    value = request.form['value']
    val = cassandraApp.cassandraOnlyOneInsertion(dbName, table, column,value)
    if val == None:
        return render_template('homeTemplates/dbFailurePg.html', result=val)
    else:
        return render_template('homeTemplates/dbSuccessPg.html', result=val)

@appObj.route('/cassandraTemplates/bulkInsertDataCassandra', methods=['POST'])  # This will be called from UI
def bulkInserCassandraDataPg():
    dbName = request.form['dbName']
    table = request.form['table']
    column = request.form['column']
    value = request.form['value']
    valuesList = value.split(';')


    val = cassandraApp.cassandraBulkOneInsertion(dbName, table, column,value)
    if val == None:
        return render_template('homeTemplates/dbFailurePg.html', result=val)
    else:
        return render_template('homeTemplates/dbSuccessPg.html', result=val)


@appObj.route('/cassandraTemplates/connectivityPg', methods=['POST'])  # This will be called from UI
def connectCassandraPg():
    clientId = request.form['clientId']
    clientSecret = request.form['clientSecret']
    zipPath = request.form['zipPath']
    val = cassandraApp.connectCassandra(clientId, clientSecret, zipPath)
    if val == None:
        return render_template('homeTemplates/dbFailurePg.html', result=val)
    else:
        return render_template('cassandraTemplates/cassandraOption.html', result=val)


@appObj.route('/cassandraTemplates/cassandraCreateTable', methods=['POST'])  # This will be called from UI
def cassandraCreateTablePg():
    dbName = request.form['dbName']
    column = request.form['column']

    val = cassandraApp.cassandraCreateTable(dbName, column)
    if val == 'Done':
        return render_template('homeTemplates/dbSuccessPg.html', result=val)
    else:
        return render_template('homeTemplates/dbFailurePg.html', result=val)


@appObj.route('/cassandraTemplates/cassandra_showDB', methods=['POST'])  # This will be called from UI
def cassandrashowDbPg():
    val = cassandraApp.cassandraShowDb()
    if val == None:
        return render_template('homeTemplates/dbFailurePg.html', result=val)
    else:
        return render_template('homeTemplates/dbSuccessPg.html', result=val)

@appObj.route('/cassandraTemplates/downloadCassandraData', methods=['POST'])  # This will be called from UI
def cassandra_download():

    dbName = request.form['dbName']
    collectionName = request.form['table']
    fileName = request.form['fileName']
    val = cassandraApp.downloadCassandraData(dbName,collectionName,fileName)
    if val == None:
        return render_template('homeTemplates/dbFailurePg.html', result=val)
    else:
        return render_template('homeTemplates/dbSuccessPg.html', result=val)


#--------------------------------------------------------------MONGO ALL API--------------------------------------------------------------------------------------------------------------------



@appObj.route('/mongoTemplates/mongoLogin.html', methods=['POST'])
def mongologinPg1():
    return render_template('/mongoTemplates/mongoLogin.html')

@appObj.route('/mongoTemplates/mongo_pg1.html', methods=['POST'])
def mongo_pg1():
    return render_template('mongoTemplates/mongo_pg1.html')

@appObj.route('/mongoTemplates/createMongoDB.html', methods=['POST'])
def mongo_pg2():
    return render_template('mongoTemplates/createMongoDB.html')

@appObj.route('/mongoTemplates/insertDataMongoDB.html', methods=['POST'])
def mongo_pg3():
    return render_template('mongoTemplates/insertDataMongoDB.html')

@appObj.route('/mongoTemplates/downloadMongoData.html', methods=['POST'])
def mongo_pg5():
    return render_template('mongoTemplates/downloadMongoData.html')


@appObj.route('/mongoTemplates/mongo_showDB', methods=['POST'])
def mongo_pg4():
    val = mongoApp.showMongoDB()
    if val == None:
        return render_template('homeTemplates/dbFailurePg.html', result=val)
    else:
        return render_template('homeTemplates/dbSuccessPg.html', result=val)

@appObj.route('/mongoTemplates/insertDataMongoDB', methods=['POST'])  # This will be called from UI
def mongo_insertDB():
    dbName = request.form['dbName']
    collection = request.form['collection']
    record = request.form['Record']
    record = json.loads(record)
    val = mongoApp.insertMongoData(dbName, collection, record)
    if val == None:
        return render_template('homeTemplates/dbFailurePg.html', result=val)
    else:
        return render_template('homeTemplates/dbSuccessPg.html', result=val)

@appObj.route('/mongoTemplates/createMongoDB', methods=['POST'])  # This will be called from UI
def mongo_createDB():

    dbName = request.form['dbName']
    collection = request.form['collection']
    record = request.form['Record']
    record = json.loads(record)
    val = mongoApp.createMongoDB(dbName,collection,record)
    if val == None:
        return render_template('homeTemplates/dbFailurePg.html', result=val)
    else:
        return render_template('homeTemplates/dbSuccessPg.html', result=val)

@appObj.route('/mongoTemplates/downloadMongoData', methods=['POST'])  # This will be called from UI
def mongo_showDB():

    dbName = request.form['dbName']
    collectionName = request.form['collectionName']
    fileName = request.form['fileName']
    val = mongoApp.downloadMongoDBData(dbName,collectionName,fileName)
    if val == None:
        return render_template('homeTemplates/dbFailurePg.html', result=val)
    else:
        return render_template('homeTemplates/dbSuccessPg.html', result=val)

@appObj.route('/mongoTemplates/mongoLogin', methods=['POST'])  # This will be called from UI
def mongoLogin():

    hostname = request.form['hostname']
    username = request.form['username']
    password = request.form['password']

    val = mongoApp.mongoLogin(appObj,hostname,username,password)
    if val == None:
        return render_template('homeTemplates/dbFailurePg.html', result=val)
    else:
        return render_template('mongoTemplates/mongo_pg1.html', result=val)


if __name__ == '__main__':
    appObj.run()

