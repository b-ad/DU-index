from __future__ import print_function

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
            doc = Rtf15Reader.read(open(file_path))
            return PlaintextWriter.write(doc).getvalue().decode('utf8')
        elif (file_path[-4:] == ".txt") or (file_path[-4:] == ".TXT"):
            with open(file_path) as t:
                return t.read().decode('latin-1')
        else:
            return 'Could not extract text from file (not recognized): ' + file_path
    except:
        return 'Could not extract text from file (extraction error): ' + file_path
   

