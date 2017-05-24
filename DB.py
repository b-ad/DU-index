
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
    'password': 'root'
}

AWS_DB = {
    'name': 'board_decisions',
    'host': 'decisions-db.cqf5j25daolp.us-west-1.rds.amazonaws.com',
    'user': 'dbuser',
    'password': 'dbuserpassword'
}

# Select working database

DB_INFO = LOCAL_DB


#
# METADATA EXTRACTION FUNCTIONS
#

def RegexRedux(query,text): #query can be two sequential queries separated by ';'

    queries=query.split(";")

    for query in queries:
        rt = re.search(query, text)
        if not rt:
            return None
        elif len(rt.groups())>0:
            #Obtain the last result group that isn't "None"
            rt=[group for group in rt.groups() if group is not None]
            text=rt[-1]
        else:
            text=rt.group(0)

    # Double-up single quotes so that it doesn't confuse SQL and
    # remove whitespace
    text = re.sub("'", "''", text).strip()

    return text


def regextract(regex_query, source_column, destination_column, sqlfilter=''):

    conn = mysqldb.connect(host=DB_INFO['host'], user=DB_INFO[
                                   'user'], passwd=DB_INFO['password'],db=DB_INFO['name'])
    c = conn.cursor()


    # create column if it doesn't exist already
    try:
        c.execute("ALTER TABLE Decisions ADD COLUMN {} TEXT".format(
            destination_column))
    except:
        pass

    #results variables:
    total_records=0 # all records
    start_amount=0 # records that need to be filled
    extracted_amount=0 #records that pulled a result


    #make list of all records being reviewed
    c.execute('select count(*) from Decisions where {}'.format(sqlfilter))
    total_records = c.fetchone()[0]

    if sqlfilter != '':
        f_sqlfilter = ' and ' + sqlfilter
    else:
        f_sqlfilter = sqlfilter

    # make list of records where that column's cell is empty and where sql
    # filter is satisfied
    c.execute('select id from Decisions where {} is null and {} is not null {}'
              .format(destination_column, source_column, f_sqlfilter))
    empty_records = [i[0] for i in c.fetchall()]
    start_amount = len(empty_records)

    print(sqlfilter,"--",'Total:',str(total_records)+'.','Blanks:',str(start_amount)+'.','Added:',end='')



    # for each row, run regex on the text
    for record in empty_records:
        # Get source text
        c.execute("select {} from Decisions where id ='{}'"
                  .format(source_column, record))
        try:
            targettext = c.fetchone()[0]

            # Apply search
            match=RegexRedux(regex_query,targettext)

            # insert results back into table
            if match:
                extracted_amount+=1
                c.execute("update Decisions set {} = '{}' where id = '{}'"
                          .format(destination_column, match, record))
        except:
            continue

        conn.commit()
    print(extracted_amount)
    # close database
    c.close()
    conn.close()

def ExtractColumn(destination_column):
    
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
        print('Extracting field:',field)
        ExtractColumn(field)



ExtractAll()

def Branch():
    regextract("AF|ARMY|CG|Navy|Marines", "local_filepath", "Branch")


def Board():
    regextract("(DRB)|(BC[MN]R)", "local_filepath", "Board")


def DecisionDate():
    regextract("(?<=CY)[0-9]{4}", "local_filepath", "Year")



#print(decision_format['ARMY','DRB'](2008))

#print(template_parsing[decision_format['ARMY','DRB'](2015)]['Original_Char'])

