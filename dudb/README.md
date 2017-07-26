
# Description

This is a python 2.7 package to create and develop a database of discharge upgrade and record correction decisions.  It draws from decisions at boards.law.af.mil.


# Functions

1. Create mirror

dudb.sync.UpdateMirror(rootlocal='path/to/local/mirror')

2. Create local database

Create mysql database called "board_decisions".  Update the server connection paramters at dudb.sync.18 and dudb.parse.13

3. Load decision texts into local database

dudb.sync.UpdateDB(source_folder='path/to/local/mirror')

4. 
