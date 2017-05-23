
from __future__ import print_function
import os
import os.path
import re
import MySQLdb as mysqldb
import csv

from decision_templates import decision_format, template_parsing


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
# METADATA EXTRACTION FUNCTIONS
#

def regextract(regex_query, source_column, destination_column, sqlfilter=''):

    conn = mysqldb.connect(host=DB_INFO['host'], user=DB_INFO[
                                   'user'], passwd=DB_INFO['pw'],db=DB_INFO['name'])
    c = conn.cursor()

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
        # Get source text
        c.execute("select {} from Decisions where id ='{}'"
                  .format(source_column, record))
        try:
            targettext = c.fetchone()[0]

            # Apply search
            rt = re.search(regex_query, targettext)

            if rt:

                #Obtain the last result group that isn't "None"
                rt=[group for group in rt.groups() if group is not None]
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
    print(start_amount,'extracted from ',destination_column," and ",sqlfilter)
    # close database
    c.close()
    conn.close()

def ExtractParse(destination_column):
    
    #get service,branch,years where there is a formula for the column
    target=[]
    for s,b in decision_format.keys():
        for y in range(1999,2017):
            try:
                if template_parsing[decision_format[s,b](y)][destination_column] != '':
                    target.append((s,b,y))
            except:
                continue
    
    #apply the regex formula to those years
    for s,b,y in target:
        sq="Branch = '"+s+"' and Board = '"+b+"' and Year ="+str(y)

        regextract(template_parsing[decision_format[s,b](y)][destination_column],'decision_text',destination_column,sq)

#ExtractParse('Original_Char')

def ExtractAll():

    #Make list of all tags
    with open('source_data/parser.csv', 'rb') as parsefile:
        parser = csv.DictReader(parsefile)
        fields=parser.fieldnames[1:]

    for field in fields:
        print('Extracting field:',field,'...')
        ExtractParse(field)



ExtractAll()

def Branch():
    regextract("AF|ARMY|CG|Navy|Marines", "local_filepath", "Branch")


def Board():
    regextract("(DRB)|(BC[MN]R)", "local_filepath", "Board")


def DecisionDate():
    regextract("(?<=CY)[0-9]{4}", "local_filepath", "Year")



#print(decision_format['ARMY','DRB'](2008))

#print(template_parsing[decision_format['ARMY','DRB'](2015)]['Original_Char'])

