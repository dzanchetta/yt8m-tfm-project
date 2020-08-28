from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
import zipfile
import os

gauth = GoogleAuth()
gauth.LocalWebserverAuth()
drive = GoogleDrive(gauth)

def read_from_drive():
    #Download the file with Comments
    fileList = drive.ListFile({'q':"'1ecZ4850Bx9YtvMSjGRhbbVlhtkVi2O_C' in parents and trashed=false"}).GetList()
    for file in fileList:
        file_id = file['id']
        downloaded = drive.CreateFile({'id': file_id})
        downloaded.GetContentFile(filename=file['title'])
    #Download the files with the video content and transcriptions
    fileList = drive.ListFile({'q': "'1v2ZiYqVY3Ztg-DqioeAQR8w84Zq7FFg1' in parents and trashed=false"}).GetList()
    for file in fileList:
        file_id = file['id']
        downloaded = drive.CreateFile({'id': file_id})
        downloaded.GetContentFile(filename=file['title'])

def unzip(path_to_zip_file,directory_to_extract_to):
    with zipfile.ZipFile(path_to_zip_file,'r') as zip_ref:
        zip_ref.extractall(directory_to_extract_to)
    os.remove(path_to_zip_file)

print('Start Reading from Drive')
read_from_drive()
print('Finished reading')

print('Starting Unzipping Comments')
unzip('/Users/daniel/LocalFiles for TFM/youtubeProjectTFM/src/comments.zip',
      '../data/')
print('Finished')

print('Starting Unzipping Video Content Files')
unzip('/Users/daniel/LocalFiles for TFM/youtubeProjectTFM/src/yt8m.zip',
      '../data/')
print('Finished')