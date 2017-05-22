import csv
from bisect import bisect

decision_format = {

    #List decision format letters from old to new.  List years that new formats went into effect

    ('ARMY', 'DRB'): lambda year: 'DCBA'[bisect([2006, 2007, 2012], year)],
    ('ARMY', 'BCMR'): lambda year: 'FE'[bisect([2003], year)],
    ('AF', 'DRB'): lambda year: 'HG'[bisect([2011], year)],
    ('AF', 'BCMR'): 'I',
    ('NAVY', 'DRB'): lambda year: 'PONKJ'[bisect([2002, 2006, 2007, 2008], year)],
    ('Marines', 'DRB'): lambda year: 'QOMLJ'[bisect([2005, 2006, 2007, 2012], year)],
    ('NAVY', 'BCNR'): 'R'

}

#print decision_format['ARMY', 'DRB'](2010)


## Read regex parsers from parser.csv
with open('parser.csv','rb') as parsefile:
  parser=csv.DictReader(parsefile)
  template_parsing={}
  for row in parser:
    key = row['Key']
    row.pop('Key')
    template_parsing[key]=row



#print template_parsing[decision_format['ARMY','DRB'](2015)]['Original_Char']

