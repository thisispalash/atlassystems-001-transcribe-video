import os
import json
import time
import hashlib
from minio import Minio
from shutil import rmtree
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed


load_dotenv() # load from .env
public_upload = True # TODO set based on sys.argv
worker_count = 16 # TODO set based on sys.argv

URL = os.environ.get('MINIO_PUBLIC_SERVER') if public_upload else os.environ.get('MINIO_SERVER')
USER = os.environ.get('MINIO_PUBLIC_USER') if public_upload else os.environ.get('MINIO_ROOT_USER')
PASS = os.environ.get('MINIO_PUBLIC_PASSWORD') if public_upload else os.environ.get('MINIO_ROOT_PASSWORD')
IS_SECURE = True if public_upload else False

client = Minio(
  URL,
  access_key=USER,
  secret_key=PASS,
  secure=IS_SECURE
)

def calculate_checksum(file_path):
  ''' calculates SHA256 checksum of the file '''
  sha256_hash = hashlib.sha256()
  with open(file_path, 'rb') as f:
    for byte_block in iter(lambda: f.read(4096), b''):
      sha256_hash.update(byte_block)
  return sha256_hash.hexdigest()

def check_checksum(before, after, is_chunk=False):
  ''' 
    Check the checksums for before and after, and correct accordingly.
    Use `is_chunk` to halt full task in case of error. Not implemented yet.
  '''

  if before == after:
    print('File successfully uploaded and verified')
  else:
    print('File verification failed!')


class Minio_Interface():

  def __init__(self,
    public_upload=True,
    always_download=False,
    workers=8
  ):
    if public_upload:
      self.URL = os.environ.get('MINIO_PUBLIC_SERVER') if public_upload else os.environ.get('MINIO_SERVER')
      self.USER = os.environ.get('MINIO_PUBLIC_USER') if public_upload else os.environ.get('MINIO_ROOT_USER')
      self.PASS = os.environ.get('MINIO_PUBLIC_PASSWORD') if public_upload else os.environ.get('MINIO_ROOT_PASSWORD')
      self.IS_SECURE = True if public_upload else False
    else:
      self.URL = os.environ.get('MINIO_PUBLIC_SERVER') if public_upload else os.environ.get('MINIO_SERVER')
      self.USER = os.environ.get('MINIO_PUBLIC_USER') if public_upload else os.environ.get('MINIO_ROOT_USER')
      self.PASS = os.environ.get('MINIO_PUBLIC_PASSWORD') if public_upload else os.environ.get('MINIO_ROOT_PASSWORD')
      self.IS_SECURE = True if public_upload else False
    self.client = Minio(
      self.URL,
      access_key=self.USER,
      secret_key=self.PASS,
      secure=self.IS_SECURE
    )
    self.download = always_download
    self.workers = workers

  def check_checksum(self, before, after, is_chunk=False):
    ''' 
      Check the checksums for before and after, and correct accordingly.
      Use `is_chunk` to halt full task in case of error. Not implemented yet.
    '''
    if before == after:
      print('File successfully uploaded and verified')
    else:
      print('File verification failed!')

  def chunk_file(self, src, size=8*1024*1024):
    pass
  
  def merge_file(self, dst):
    pass
  
  def upload_chunk(self):
    pass
  
  def download_chunk(self):
    pass
  
  def upload_file(self, src, dst):
    pass
  
  def download_file(self, src, dst):
    pass
  
  def run(self, src, dst, check_chunk=False):
    pass


def chunk_file(src, size=20*1024*1024):
  ''' split the given `src` into chunks of size 20MB (default) '''

  file_size = os.path.getsize(src)
  print(f'File: {src}, Total Size: {file_size} bytes')
  chunk_metadata = []
  if os.path.exists('./temp'): rmtree('./temp')
  os.mkdir('./temp') # to store the chunks temporarily

  with open(src, 'rb') as f:
    chunk_number = 0

    while chunk_number * size < file_size:
      chunk_data = f.read(size)
      chunk_file_path = f'./temp/part{chunk_number}'
      with open(chunk_file_path, 'wb') as chunk_file:
        chunk_file.write(chunk_data)
      chunk_metadata.append({
        'chunk_number': chunk_number,
        'chunk_path': chunk_file_path,
        'checksum': calculate_checksum(chunk_file_path)
      })
      chunk_number += 1
    # write out metadata
    with open(f'{src}.metadata.json', 'w') as f:
      json.dump(chunk_metadata, f, indent=2)

  print(f'Chunked into {chunk_number} parts')
  return chunk_metadata

def merge_file(src, metadata):
  pass


def upload_chunk(bucket, dst, chunk, retries=3):
  ''' uploads the chunk to the bucket '''

  chunk_path = chunk['chunk_path']
  chunk_number = chunk['chunk_number']

  for i in range(retries):
    try:
      start_time = time.time()
      client.fput_object(bucket, f'{dst}/part{chunk_number}', chunk_path)
      end_time = time.time()
      print(f'Uploaded chunk {chunk_number} in {end_time - start_time} seconds')
      return
    except Exception as e:
      print(f'Error uploading chunk {chunk_number}: {e}')
      if i == retries - 1:
        print(f'Failed to upload chunk {chunk_number} after {retries} retries')
        return
      else:
        print(f'Retrying chunk {chunk_number} upload...')

def download_chunk():
  pass

def upload_to_minio(src, dst):
  ''' uploads file at `src`, renaming to `dst` in the process '''

  print(f'\n--------- {src} ---------\n')

  bucket = 'atlassystems-video-analytics-dev'
  if not client.bucket_exists(bucket):
    client.make_bucket(bucket)

  # Calculate checksum before upload
  checksum_before = calculate_checksum(src)
  print(f'Checksum before upload: {checksum_before}')

  # Chunk file
  start_time = time.time()
  metadata = chunk_file(src)
  end_time = time.time()
  t1 = end_time - start_time
  print(f'Chunking time: {t1} seconds')
  
  # Upload chunks
  start_time = time.time()
  client.fput_object(bucket, f'{dst}.metadata.json', f'{src}.metadata.json')
  with ThreadPoolExecutor(max_workers=worker_count) as executor:
    futures = [executor.submit(upload_chunk, bucket, dst, chunk) for chunk in metadata]
    for future in as_completed(futures):
      future.result()
  end_time = time.time()
  t2 = end_time - start_time
  print(f'Upload time: {t2} seconds')

  # Cleanup chunks
  start_time = time.time()
  rmtree('./temp')
  end_time = time.time()
  t3 = end_time - start_time
  print(f'Cleanup time: {t3} seconds')

  print(f'Total upload-task time: {t1 + t2 + t3} seconds')
  return checksum_before, t1+t2+t3

def download_from_minio(src, dst):
  ''' download the file from minio, recombine, and return checksum '''
  return '', 0


def perform_task(src, dst):
  ''' 
    Uploads a file and redownloads it to compare checksum. Can be removed by moving checksum 
    operations to chunks to stop uploads midway. See `check_checksum`.
  '''
  checksum_before, t_up = upload_to_minio(src, dst)
  checksum_after, t_down = download_from_minio(src, dst)
  print(f'Total task time: {t_up + t_down} seconds')
  check_checksum(checksum_before, checksum_after)


### Small File ###
# if public_upload: perform_task('./yt_sm.mp4', 'yt_sm')
# else: perform_task('./himalaya_sm.mp4', 'himalaya_sm')

# ### Medium File ###
if public_upload: perform_task('./yt_md.mp4', 'yt_md')
else: perform_task('./himalaya_md.mp4', 'himalaya_md')

### Large File ###
# if public_upload: perform_task('./yt_lg.mp4', 'yt_lg')
# else: perform_task('./himalaya_lg.mp4', 'himalaya_lg')


if __name__ == '__main__':
  # load_dotenv('.env.example') # load from .env
  # TODO get from sys.argv and set public or not
  pass