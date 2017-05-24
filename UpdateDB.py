
from __future__ import print_function
import os
import os.path
import MySQLdb as mysqldb

from text_processing import get_text, convert_pdf_to_txt


#
# Database configuraitons
#

LOCAL_DB = {
    'name': 'board_decisions',
    'host': 'localhost',
    'user': 'root',
    'pw': 'root'
}

AWS_DB = {
    'name': 'board_decisions',
    'host': 'decisions-db.cqf5j25daolp.us-west-1.rds.amazonaws.com',
    'user': 'dbuser',
    'pw': 'dbuserpassword'
}

# Select working database

DB_INFO = LOCAL_DB


#
# DATABASE LOADING FUNCTIONS
#

def InitializeDB():

    conn = mysqldb.connect(host=DB_INFO['host'], user=DB_INFO[
                                   'user'], passwd=DB_INFO['pw'],db=DB_INFO['name'],use_unicode=True, charset="utf8")
    c = conn.cursor()

    # Create database if necessary.

    try:
        conn.db = DB_INFO['name']
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_BAD_DB_ERROR:
            try:
                c.execute("CREATE DATABASE {} DEFAULT CHARACTER SET 'utf8'".format(
                    DB_INFO['name']))
            except mysql.connector.Error as err:
                print("Failed creating database: {}".format(err))
                exit(1)
            conn.db = DB_INFO['name']
        else:
            print(err)
            exit(1)

    # create Decisions table if it doesn't exist
    c.execute(
        'CREATE TABLE IF NOT EXISTS Decisions (id INT(11) NOT NULL AUTO_INCREMENT, PRIMARY KEY (id))')

    # create columns if they don't exist

    table_columns = [('local_filepath', 'TINYTEXT'), ('relative_path',
                                                        'TINYTEXT'), ('filename', 'TINYTEXT'), ('decision_text', 'TEXT')]
    for n, t in table_columns:
        try:
            c.execute("ALTER TABLE Decisions ADD COLUMN {n} {t};"
                      .format(n=n, t=t))
        except:
            pass

    c.close()
    conn.close()


def UpdateDBwithDecisions():

    source_folder="/Users/bradfordadams/Cloud Drives/Dropbox/Decisions/boards.law.af.mil"

    #connect to database
    conn = mysqldb.connect(host=DB_INFO['host'], user=DB_INFO[
                                   'user'], passwd=DB_INFO['pw'],db=DB_INFO['name'],use_unicode=True, charset="utf8")
    c = conn.cursor()


    # get list of files in mirror
    print('Making list of files in mirror...')
    mirror_list = [os.path.join(root, file) for root,dirs,files in os.walk(source_folder) for file in files if not file[0] == '.'] 
    print(len(mirror_list),'files in mirror.')
   
    # get list of records in db
    print('Getting list of files in database...')
    c.execute('select distinct local_filepath from Decisions')
    db_files_list = c.fetchall()
    db_list = [i[0].encode('ascii', 'ignore') for i in db_files_list]
    print(len(db_list),'files in database.')

    # make list of files to add
    print('Making list of missing files...')
    db_list=set(db_list)
    file_list=[x for x in mirror_list if x not in db_list]
    print('Missing',len(file_list),'files from database.')


    # process files
    print('Adding files to database:')
    for file_path in file_list:

        # create relative paths from the origin of the original folder
        file_rel_path = os.path.relpath(file_path, source_folder)

        # get filename
        file_name = os.path.basename(file_path)

        # get text of decision
        doctext = get_text(file_path)

        # add new record to db
        #try:
        c.execute("insert into Decisions (local_filepath,relative_path,filename,decision_text) values (%s,%s,%s,%s)",
                      (file_path, file_rel_path, file_name, doctext))
        print(len(doctext),'chars uploaded.')
        #except:
        #    c.execute("insert into Decisions (local_filepath,relative_path,filename,decision_text) values (%s,%s,%s,%s)",
        #              (file_path, file_rel_path, file_name, "Load error"))
        #    print('Load error')
        
        conn.commit()

        file_list.remove(file_path)
        print(len(file_list), "files remaining.")


    c.close()
    conn.close()

#InitializeDB() #Commented about b/c only need it when first creating DB
UpdateDBwithDecisions()

