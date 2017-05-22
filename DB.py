from DB_functions import *

# Database configuraitons

LOCAL_DB = {
    'name':'board_decisions',
    'host':'localhost',
    'user':'root',
    'pw':'root'
}

AWS_DB={
    'name' : 'board_decisions',
    'host' : 'decisions-db.cqf5j25daolp.us-west-1.rds.amazonaws.com',
    'user' : 'dbuser',
    'pw' : 'dbuserpassword'
}

# Select working database

DB_INFO = AWS_DB

InitializeDB(DB_INFO)

# UpdateDBwithDecisions('/Users/bradfordadams/Cloud Drives/Dropbox/Decisions/boards.law.af.mil/')
