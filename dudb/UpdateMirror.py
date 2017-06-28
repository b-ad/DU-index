from __future__ import print_function
import httplib2
import urllib
import os
from urlparse import urljoin
from BeautifulSoup import BeautifulSoup


def UpdateMirror(rootlocal="/Users/bradfordadams/Cloud Drives/Dropbox/Decisions/boards.law.af.mil/", rootweb="http://boards.law.af.mil/"):

    # create list of all files on remote site
    print("Making list of files on website...")
    http = httplib2.Http()

    urls = []
    docs = []

    urls.append(rootweb)
    for url in urls:
        cachedurl="http://webcache.googleusercontent.com/search?num=1&strip=1&vwsrc=0&q=cache:"+url.split("//")[-1]
        status, response = http.request(url)
        pagehtml = BeautifulSoup(response)
        for link in pagehtml.findAll('a', href=True):
            link = urljoin(url, link['href'])
            suffix = link.split(".")[-1][:3]
            if suffix == "htm":
                if link in urls:
                    continue
                else:
                    urls.append(link)
            elif suffix in ["doc", "pdf", "txt", "rtf"]:
                docs.append(link)
            else:
                continue
    docs = set(docs)
    docs = (doc.split(rootweb)[-1] for doc in docs)
    print(len(docs), "files on website.")

    # create list of all files on local site
    print('Making list of files in local mirror...')
    local_list = [os.path.join(root, file).split(rootlocal)[-1] for root, dirs,
                  files in os.walk(rootlocal) for file in files if not file[0] == '.']
    local_list = set(local_list)
    print(len(local_list), 'files in local mirror.')

    # idenify files missing from local site
    print('Identifying missing files...')
    missing_list = docs - local_list
    print(len(missing_list), 'files need to be downloaded.')

    # copy those files to the appropriate local folder
    print('Downloading files to local mirror...')
    fails = 0
    for file in missing_list:
        filename = os.path.join(rootlocal, file)
        if os.path.exists(filename):
            continue
        if not os.path.isdir(os.path.dirname(filename)):
            os.makedirs(os.path.dirname(filename))
        try:
            urllib.urlretrieve(urljoin(rootweb, file),
                               os.path.join(rootlocal, file))
            print('.', end='')
        except:
            fails += 1
            print(urljoin(rootweb, file), " - ",
                  os.path.join(rootlocal, file), "failed to download.")
    print(len(missing_list) - fails, 'new files downloaded to local mirror.')


# update the DB with text of the new documents' decisions
