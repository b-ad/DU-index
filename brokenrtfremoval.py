# remove dead rtf files
import os
from pyth.plugins.rtf15.reader import Rtf15Reader
from pyth.plugins.plaintext.writer import PlaintextWriter


source_folder = '/Users/bradfordadams/Cloud Drives/Dropbox/Decisions/boards.law.af.mil/ARMY/DRB/'


# get recursive list of files and paths
file_list = os.walk(source_folder)
for root, dirs, files in file_list:

  # select only rtfs
  files = [fi for fi in files if fi[-4:] == '.rtf']

  for f in files:

    # get full file path of original file
    file_path = os.path.join(root, f)
    try:
      doc = Rtf15Reader.read(open(file_path))
      text = PlaintextWriter.write(doc).getvalue()
    except:
      os.remove(file_path)