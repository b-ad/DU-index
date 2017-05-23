
from __future__ import print_function
import os
import os.path
import re
import MySQLdb as mysqldb


from decision_templates import decision_format, template_parsing

from text_processing import document_to_text,convert_pdf_to_txt


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
# SERVER CONNECTION FUNCTIONS
#

def ConnectToServer():

    global conn
    global c

    conn = mysqldb.connect(host=DB_INFO['host'], user=DB_INFO[
                                   'user'], passwd=DB_INFO['pw'],db=DB_INFO['name'])
    c = conn.cursor()


def DisconnectFromServer():

    global conn
    global c

    conn.commit()
    c.close()
    conn.close()


#
# DATABASE LOADING FUNCTIONS
#

def InitializeDB():

    global c
    global conn

    ConnectToServer()

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

    DisconnectFromServer()


def UpdateDBwithDecisions(source_folder):

    global conn
    global c

    ConnectToServer()

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
    mirror_list=set(mirror_list)
    db_list=set(db_list)
    file_list=[x for x in mirror_list if x not in db_list]
    print('Missing',len(file_list),'files from database.')


    # process files
    print('Adding files to database...."')
    for file_path in file_list:

        print(len(file_list), " files remaining")

        # create relative paths from the origin of the original folder
        file_rel_path = os.path.relpath(file_path, source_folder)

        # get filename
        file_name = os.path.basename(file_path)

        # get text of decision
        doctext = document_to_text(file_path)

        # add new record to db
        try:
            c.execute("insert into Decisions (local_filepath,relative_path,filename,decision_text) values (%s,%s,%s,%s)",
                      (file_path, file_rel_path, file_name, doctext))
        except:
            c.execute("insert into Decisions (local_filepath,relative_path,filename,decision_text) values (%s,%s,%s,%s)",
                      (file_path, file_rel_path, file_name, "Load error"))
        conn.commit()

    DisconnectFromServer()


#
# METADATA EXTRACTION FUNCTIONS
#

def regextract(regex_query, source_column, destination_column, sqlfilter=''):

    global conn
    global c

    ConnectToServer()

    if sqlfilter != '':
        sqlfilter = ' and ' + sqlfilter
    else:
        pass

    # create column if it doesn't exist already
    try:
        c.execute("ALTER TABLE Decisions ADD COLUMN {} TEXT".format(
            destination_column))
    except:
        pass

    # make list of records where that column's cell is empty and where sql
    # filter is satisfied
    c.execute('select id from Decisions where {} is null and {} is not null {}'
              .format(destination_column, source_column, sqlfilter))
    empty_records = [i[0] for i in c.fetchall()]
    start_amount = len(empty_records)

    # for each row, run regex on the text

    for record in empty_records:
        print(start_amount)
        start_amount -= 1
        # Get source text
        c.execute("select {} from Decisions where id ='{}'"
                  .format(source_column, record))
        try:
            targettext = c.fetchone()[0]

            # Apply search
            rt = re.search(regex_query, targettext)

            if rt:

                #Obtain the last result group that isn't "None"
                rt=[group for group in rt.groups() if group != None]
                rt=rt[-1]

                # Double-up single quotes so that it doesn't confuse SQL and
                # remove whitespace
                rt = re.sub("'", "''", rt).strip()

                # insert results back into table
                c.execute("update Decisions set {} = '{}' where id = '{}'"
                          .format(destination_column, rt, record))
        except:
            continue

        conn.commit()

    # close database
    DisconnectFromServer()


def dischargeDate():
    regextract(
        "(?i)(discharge|separation)[^\n]*?[0-9]{6,8}", "decision_text", "Discharge")
    regextract("[0-9]{6,8}", "Discharge", "Discharge_date")


# def enterDate ():


def Branch():
    regextract("AF|ARMY|CG|Navy|Marines", "local_filepath", "Branch")


def Board():
    regextract("(DRB)|(BC[MN]R)", "local_filepath", "Board")


def DecisionDate():
    regextract("(?<=CY)[0-9]{4}", "local_filepath", "Year")


def ReasonForSeparation():

    # ARMY DRBs:
    regextract("(?i)(narrative|reason).*:.*", "decision_text",
               "Reason1", "and BRANCH = 'ARMY' and VENUE = 'DRB' and YEAR ")

    regextract("(?i)(narrative|reason).*:.*", "decision_text", "Reason1")
    regextract("(?i)(?<=:).*", "Reason1", "Reason_for_separation")


def BoardDecision():
    # regextract("(?s)(?<=\.)[^\.()]*? character(?!.* character).*?[\.\n]","decision_text","Character_decision")
    # regextract("(?s)(?<=\.)[^\.()]*?reason(?!.*reason).*?[\.\n]","decision_text","NR_decision")
    regextract("(?i)\.[^\n\.]*?(change[^\.]*characte|character[^\.]*change)[^\.\n]*to [A-Za-z].*?\.",
               "decision_text", "Character_change")
    regextract("(?i)\.[^\n\.]*?(change[^\.]*reason|reason[^\.]*change)[^\.\n]*to [A-Za-z].*?\.",
               "decision_text", "NR_change")


def BCMRRelief():
    regextract("[\n\r].*?[a-zA-Z].*?[a-zA-Z].*?GRANT[^\n\r]*?RELIEF",
               "decision_text", "Result", 'local_filepath like "%BC_R%"')

# regextract('RECOMMENDATION.{200}','decision_text','Recommendation')

UpdateDBwithDecisions("/Users/bradfordadams/Cloud Drives/Dropbox/Decisions/boards.law.af.mil")

#print(decision_format['ARMY','DRB'](2008))

#print(template_parsing[decision_format['ARMY','DRB'](2015)]['Original_Char'])

