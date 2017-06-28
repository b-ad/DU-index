from __future__ import print_function

import regex

# to convert DOC.  Must already have antiword installed directly on system
from subprocess import Popen, PIPE

# to convert DOCX
from docx import opendocx, getdocumenttext

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
            print('')
            return str
        else:
            print(', needs OCR...')
            raise ValueError
    except:
        tool = pyocr.get_available_tools()[0]
        lang = tool.get_available_languages()[0]

        req_image = []
        final_text = []

        with Image(filename=file_path, resolution=150) as image_jpeg:
          image_jpeg.compression_quality = 99
          image_jpeg = image_jpeg.convert('jpeg')

          for img in image_jpeg.sequence:
              with Image(image=img) as img_page:
                req_image.append(img_page.make_blob('jpeg'))
        image_jpeg.destroy()

        for img in req_image:
            txt = tool.image_to_string(
                PI.open(io.BytesIO(img)),
                lang=lang,
                builder=pyocr.builders.TextBuilder()
            )
            final_text.append(txt)
        ocr = ''.join(final_text)

        return(ocr)


def getrtffields(t):
    fieldsearch=regex.compile(r"{\\field[^{]*?({(?>[^{}]+|(?1))*})({(?>[^{}]+|(?1))*})}")

    textboxes,drops,checks=[],[],[]
    checkboxoptions=["No","Yes"]

    #Deal with text boxes
    m = fieldsearch.finditer(t)
    if m:
      for field in m:
        if "FORMTEXT" in field[0]:
          textboxes.append(field[0])
        elif "FORMDROPDOWN" in field[0]:
          drops.append(field[0])
        elif "FORMCHECKBOX" in field[0]:
          checks.append(field[0])
        else:
          pass

      for textbox in textboxes:
        try:
          result = regex.search(r"fldrslt ({(?>[^{}]+|(?1))*})}",textbox)[1]
          if result:
            t=t.replace(textbox,result)
        except:
          pass
      for drop in drops:
        try:
          ddresult = regex.search(r"fftype2.*ffres([0-9]*)",drop)[1]
          if ddresult=="25":
            ddresult=regex.search(r"ffdefres([0-9]*)",drop)[1]
          ddlist = re.findall(r"ffl ([^}]*)}",drop)
          t=t.replace(drop,"{\\rtlch "+ddlist[int(ddresult)]+"}")
        except:
          pass
      for check in checks:
        try:
          result = regex.search(r"fftype1.*ffres([0-9]*)",check)[1]
          if result=="25":
            result=regex.search(r"ffdefres([0-9]*)",check)[1]
          t=t.replace(check,"{\\rtlch "+checkboxoptions[int(ddresult)]+"}")
        except:
          pass

    return t


def striprtf(text):
   text=getrtffields(text)
   pattern = regex.compile(r"\\([a-z]{1,32})(-?\d{1,10})?[ ]?|\\'([0-9a-f]{2})|\\([^a-z])|([{}])|[\r\n]+|(.)", regex.I)
   # control words which specify a "destionation".
   destinations = frozenset((
      'aftncn','aftnsep','aftnsepc','annotation','atnauthor','atndate','atnicn','atnid',
      'atnparent','atnref','atntime','atrfend','atrfstart','author','background',
      'bkmkend','bkmkstart','blipuid','buptim','category','colorschememapping',
      'colortbl','comment','company','creatim','datafield','datastore','defchp','defpap',
      'do','doccomm','docvar','dptxbxtext','ebcend','ebcstart','factoidname','falt',
      'fchars','ffdeftext','ffentrymcr','ffexitmcr','ffformat','ffhelptext','ffl',
      'ffname','ffstattext','field','file','filetbl','fldinst','fldrslt','fldtype',
      'fname','fontemb','fontfile','fonttbl','footer','footerf','footerl','footerr',
      'footnote','formfield','ftncn','ftnsep','ftnsepc','g','generator','gridtbl',
      'header','headerf','headerl','headerr','hl','hlfr','hlinkbase','hlloc','hlsrc',
      'hsv','htmltag','info','keycode','keywords','latentstyles','lchars','levelnumbers',
      'leveltext','lfolevel','linkval','list','listlevel','listname','listoverride',
      'listoverridetable','listpicture','liststylename','listtable','listtext',
      'lsdlockedexcept','macc','maccPr','mailmerge','maln','malnScr','manager','margPr',
      'mbar','mbarPr','mbaseJc','mbegChr','mborderBox','mborderBoxPr','mbox','mboxPr',
      'mchr','mcount','mctrlPr','md','mdeg','mdegHide','mden','mdiff','mdPr','me',
      'mendChr','meqArr','meqArrPr','mf','mfName','mfPr','mfunc','mfuncPr','mgroupChr',
      'mgroupChrPr','mgrow','mhideBot','mhideLeft','mhideRight','mhideTop','mhtmltag',
      'mlim','mlimloc','mlimlow','mlimlowPr','mlimupp','mlimuppPr','mm','mmaddfieldname',
      'mmath','mmathPict','mmathPr','mmaxdist','mmc','mmcJc','mmconnectstr',
      'mmconnectstrdata','mmcPr','mmcs','mmdatasource','mmheadersource','mmmailsubject',
      'mmodso','mmodsofilter','mmodsofldmpdata','mmodsomappedname','mmodsoname',
      'mmodsorecipdata','mmodsosort','mmodsosrc','mmodsotable','mmodsoudl',
      'mmodsoudldata','mmodsouniquetag','mmPr','mmquery','mmr','mnary','mnaryPr',
      'mnoBreak','mnum','mobjDist','moMath','moMathPara','moMathParaPr','mopEmu',
      'mphant','mphantPr','mplcHide','mpos','mr','mrad','mradPr','mrPr','msepChr',
      'mshow','mshp','msPre','msPrePr','msSub','msSubPr','msSubSup','msSubSupPr','msSup',
      'msSupPr','mstrikeBLTR','mstrikeH','mstrikeTLBR','mstrikeV','msub','msubHide',
      'msup','msupHide','mtransp','mtype','mvertJc','mvfmf','mvfml','mvtof','mvtol',
      'mzeroAsc','mzeroDesc','mzeroWid','nesttableprops','nextfile','nonesttables',
      'objalias','objclass','objdata','object','objname','objsect','objtime','oldcprops',
      'oldpprops','oldsprops','oldtprops','oleclsid','operator','panose','password',
      'passwordhash','pgp','pgptbl','picprop','pict','pn','pnseclvl','pntext','pntxta',
      'pntxtb','printim','private','propname','protend','protstart','protusertbl','pxe',
      'result','revtbl','revtim','rsidtbl','rxe','shp','shpgrp','shpinst',
      'shppict','shprslt','shptxt','sn','sp','staticval','stylesheet','subject','sv',
      'svb','tc','template','themedata','title','txe','ud','upr','userprops',
      'wgrffmtfilter','windowcaption','writereservation','writereservhash','xe','xform',
      'xmlattrname','xmlattrvalue','xmlclose','xmlname','xmlnstbl',
      'xmlopen',
   ))
   # Translation of some special characters.
   specialchars = {
      'par': '\n',
      'sect': '\n\n',
      'page': '\n\n',
      'line': '\n',
      'tab': '\t',
      'emdash': u'\u2014',
      'endash': u'\u2013',
      'emspace': u'\u2003',
      'enspace': u'\u2002',
      'qmspace': u'\u2005',
      'bullet': u'\u2022',
      'lquote': u'\u2018',
      'rquote': u'\u2019',
      'ldblquote': u'\201C',
      'rdblquote': u'\u201D', 
   }
   stack = []
   ignorable = False       # Whether this group (and all inside it) are "ignorable".
   ucskip = 1              # Number of ASCII characters to skip after a unicode character.
   curskip = 0             # Number of ASCII characters left to skip
   out = []                # Output buffer.
   
   for match in pattern.finditer(text):
      word,arg,hex,char,brace,tchar = match.groups()
      if brace:
         curskip = 0
         if brace == '{':
            # Push state
            stack.append((ucskip,ignorable))
         elif brace == '}':
            # Pop state
            ucskip,ignorable = stack.pop()
      elif char: # \x (not a letter)
         curskip = 0
         if char == '~':
            if not ignorable:
                out.append(u'\xA0')
         elif char in '{}\\':
            if not ignorable:
               out.append(char)
         elif char == '*':
            ignorable = True
      elif word: # \foo
         curskip = 0
         if word in destinations:
            ignorable = True
         elif ignorable:
            pass
         elif word in specialchars:
            out.append(specialchars[word])
         elif word == 'uc':
            ucskip = int(arg)
         elif word == 'u':
            c = int(arg)
            if c < 0: c += 0x10000
            if c > 127: out.append(unichr(c))
            else: out.append(chr(c))
            curskip = ucskip
      elif hex: # \'xx
         if curskip > 0:
            curskip -= 1
         elif not ignorable:
            c = int(hex,16)
            if c > 127: out.append(unichr(c))
            else: out.append(chr(c))
      elif tchar:
         if curskip > 0:
            curskip -= 1
         elif not ignorable:
            out.append(tchar)
   return ''.join(out)



def get_text(file_path):

    try:
        print('Filetype:',file_path[-3:],end='')
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
            with open(file_path) as f:
               return striprtf(f.read())
        elif (file_path[-4:] == ".txt") or (file_path[-4:] == ".TXT"):
            with open(file_path) as t:
                return t.read().decode('latin-1')
        else:
            return 'Could not extract text from file (not recognized): ' + file_path
    except:
        return 'Could not extract text from file (extraction error): ' + file_path
   

