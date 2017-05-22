from wand.image import Image
from PIL import Image as PI
import pyocr
import pyocr.builders
import io

from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
from pdfminer.converter import TextConverter
from pdfminer.layout import LAParams
from pdfminer.pdfpage import PDFPage
from cStringIO import StringIO


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
        if len(str)>15:
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

print(convert_pdf_to_txt("/Users/bradfordadams/Cloud Drives/Dropbox/Decisions/boards.law.af.mil/NAVY/BCNR/CY2011/02978-11.pdf"))