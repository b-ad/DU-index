from __future__ import print_function
import httplib2
import os
import wget
from urlparse import urljoin
from BeautifulSoup import BeautifulSoup


def UpdateMirror(rootlocal="/Users/bradfordadams/Cloud Drives/Dropbox/Decisions/boards.law.af.mil/", rootweb="http://boards.law.af.mil/"):

  #create list of all files on remote site
  print("Making list of files on website...")
  http=httplib2.Http()

  urls=[]
  docs=[]

  urls.append(rootweb)
  for url in urls:
    status,response=http.request(url)
    pagehtml=BeautifulSoup(response)
    for link in pagehtml.findAll('a',href=True):
      link=link['href']
      link=urljoin(url,link)
      suffix=link.split(".")[-1][:3]
      if suffix=="htm":
        if link in urls:
          continue
        else:
          urls.append(link)
      elif suffix in ["doc","pdf","txt","rtf"]:
        docs.append(link)
      else:
        continue
  print(len(docs),"files on website.")

  # create list of all files on local site
  print('Making list of files in local mirror...')
  local_list = [file for root,dirs,files in os.walk(rootlocal) for file in files if not file[0] == '.'] 
  print(len(local_list),'files in local mirror.')


  # idenify files missing from local site
  print('Identifying missing files...')
  local_list=set(local_list)
  docs=set(docs)
  missing_list=docs-local_list
  print(len(missing_list),'files need to be downloaded.')


  # copy those files to the appropriate local folder
  print('Downloading files to local mirror...')
  fails=0
  for file in missing_list:
    try:
      wget.download(urljoin(rootweb,file),out=urljoin(rootlocal,file))
    except:
      fails+=1
  print(len(missing_list)-fails,'new files downloaded to local mirror.')


# update the DB with text of the new documents' decisions



