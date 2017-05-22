
from __future__ import print_function
import os
import os.path

import MySQLdb as mysqldb

import re

from decision_templates import decision_format, template_parsing

# to convert DOC.  Must already have antiword installed directly on system
from subprocess import Popen, PIPE

# to convert DOCX
from docx import opendocx, getdocumenttext

# to convert RTF
from pyth.plugins.rtf15.reader import Rtf15Reader

# to convert TXT
from pyth.plugins.plaintext.writer import PlaintextWriter

# to convert PDFs
from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
from pdfminer.converter import TextConverter
from pdfminer.layout import LAParams
from pdfminer.pdfpage import PDFPage
from cStringIO import StringIO

# to ocr pdfs
from wand.image import Image
from PIL import Image as PI
import pyocr
import pyocr.builders
import io


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
# TEXT CONVERSION FUNCTIONS
#

def convert_pdf_to_txt(file_path):
    rsrcmgr = PDFResourceManager()
    retstr = StringIO()
    laparams = LAParams()
    device = TextConverter(rsrcmgr, retstr, laparams=laparams)
    fp = file(file_path, 'rb')
    interpreter = PDFPageInterpreter(rsrcmgr, device)
    password = ""
    maxpages = 0
    caching = True
    pagenos = set()
    for page in PDFPage.get_pages(fp, pagenos, maxpages=maxpages, password=password, caching=caching, check_extractable=True):
        interpreter.process_page(page)
    fp.close()
    device.close()
    str = retstr.getvalue().decode('utf8')
    retstr.close()

    try:
        if len(str) > 15:
            return str
        else:
            raise ValueError
    except:
        tool = pyocr.get_available_tools()[0]
        lang = tool.get_available_languages()[0]

        req_image = []
        final_text = []

        image_pdf = Image(filename=file_path, resolution=300)
        image_jpeg = image_pdf.convert('jpeg')

        for img in image_jpeg.sequence:
            img_page = Image(image=img)
            req_image.append(img_page.make_blob('jpeg'))

        for img in req_image:
            txt = tool.image_to_string(
                PI.open(io.BytesIO(img)),
                lang=lang,
                builder=pyocr.builders.TextBuilder()
            )
            final_text.append(txt)
        ocr = ''.join(final_text)

        return(ocr)


def document_to_text(file_path):

    try:
        print(file_path[-4:])
        if file_path[-4:] == (".doc" or ".DOC"):
            cmd = ['antiword', file_path]
            p = Popen(cmd, stdout=PIPE)
            stdout, stderr = p.communicate()
            return stdout.decode('utf8', 'ignore')
        elif file_path[-5:] == (".docx" or "DOCX"):
            document = opendocx(file_path)
            paratextlist = getdocumenttext(document)
            newparatextlist = []
            for paratext in paratextlist:
                newparatextlist.append(paratext)
            return '\n\n'.join(newparatextlist)
        elif file_path[-4:] == (".odt" or ".ODT"):
            cmd = ['odt2txt', file_path]
            p = Popen(cmd, stdout=PIPE)
            stdout, stderr = p.communicate()
            return stdout.decode('ascii', 'ignore')
        elif file_path[-4:] == (".pdf" or ".PDF"):
            return convert_pdf_to_txt(file_path)
        elif file_path[-4:] == (".rtf" or ".RTF"):
            doc = Rtf15Reader.read(open(file_path))
            return PlaintextWriter.write(doc).getvalue().decode('utf8')
        elif (file_path[-4:] == ".txt") or (file_path[-4:] == ".TXT"):
            with open(file_path) as t:
                return t.read().decode('latin-1')
        else:
            return 'Could not extract text from file (not recognized): ' + file_path
    except:
        return 'Could not extract text from file (extraction error): ' + file_path


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


    # get list of records in db
    print('Getting list of files in database...')
    c.execute('select distinct local_filepath from Decisions')
    db_files_list = c.fetchall()
    db_list = [i[0].encode('ascii', 'ignore') for i in db_files_list]
    print(len(db_list),' files in database')

    # make list of files to add
    print('Making list of missing files...')
    full_file_list = os.walk(source_folder)
    file_list = []

    for root, dirs, files in full_file_list:

        files = [file for file in files if not file[0] == '.']

        for file in files:

            file_path = os.path.join(root, file)

            if file_path in db_list:
                continue
            else:
                file_list.append(file_path)

    files_left = len(file_list)
    print('Missing ',files_left,' files from database.')


    # process files
    print('Adding files to database...."')
    for file_path in file_list:

        print(files_left, " files remaining")

        # convert the absolute paths to relative paths from the origin of the
        # original folder
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
        files_left -= 1
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

