import os
import json
import time
import hashlib
from minio import Minio
from shutil import rmtree
from dotenv import load_dotenv
from optparse import OptionParser
from concurrent.futures import ThreadPoolExecutor, as_completed


parser = OptionParser()
parser.add_option('-p', '--public', dest='public', default=False, action='store_true')
parser.add_option('-d', '--download', dest='download', default=False, action='store_true')
parser.add_option('-w', '--workers', dest='workers', default=8, type='int')
parser.add_option('-s', '--sm', dest='sm', default=False, action='store_true')
parser.add_option('-m', '--md', dest='md', default=False, action='store_true')
parser.add_option('-l', '--lg', dest='lg', default=False, action='store_true')


def calculate_checksum(file_path):
  ''' calculates SHA256 checksum of the file '''
  sha256_hash = hashlib.sha256()
  with open(file_path, 'rb') as f:
    for byte_block in iter(lambda: f.read(4096), b''):
      sha256_hash.update(byte_block)
  return sha256_hash.hexdigest()


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
  
  def merge_file(self, dst):
    pass
  
  def upload_chunk(self, bucket, dst, chunk, retries=3):
    ''' uploads the chunk to the bucket '''

    chunk_path = chunk['chunk_path']
    chunk_number = chunk['chunk_number']

    for i in range(retries):
      try:
        start_time = time.time()
        self.client.fput_object(bucket, f'{dst}/part{chunk_number}', chunk_path)
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
  
  def download_chunk(self):
    pass
  
  def upload_file(self, src, dst):
    ''' uploads file at `src`, renaming to `dst` in the process '''

    print(f'\n--------- {src} ---------\n')

    bucket = 'atlassystems-video-analytics-dev'
    if not self.client.bucket_exists(bucket):
      self.client.make_bucket(bucket)

    # Calculate checksum before upload
    checksum_before = calculate_checksum(src)
    print(f'Checksum before upload: {checksum_before}')

    # Chunk file
    start_time = time.time()
    metadata = self.chunk_file(src)
    end_time = time.time()
    t1 = end_time - start_time
    print(f'Chunking time: {t1} seconds')
    
    # Upload chunks
    start_time = time.time()
    self.client.fput_object(bucket, f'{dst}.metadata.json', f'{src}.metadata.json')
    with ThreadPoolExecutor(max_workers=self.workers) as executor:
      futures = [executor.submit(self.upload_chunk, bucket, dst, chunk) for chunk in metadata]
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
  
  def download_file(self, src, dst):
    ''' download the file from minio, recombine, and return checksum '''
    return '', 0
  
  def run(self, src, dst, check_chunk=False):
    ''' 
      Uploads a file and redownloads it to compare checksum. Can be removed by moving checksum 
      operations to chunks to stop uploads midway. See `check_checksum`.
    '''
    checksum_before, t_up = self.upload_file(src, dst)
    if self.download:
      checksum_after, t_down = self.download_file(src, dst)
      print(f'Total task time: {t_up + t_down} seconds')
      self.check_checksum(checksum_before, checksum_after)
    

if __name__ == '__main__':
  load_dotenv()
  options, args = parser.parse_args()
  public_upload = options.public
  interface = Minio_Interface(
    public_upload=public_upload,
    always_download=options.download,
    workers=options.workers
  )

  prefix = 'yt' if public_upload else 'himalaya'
  if options.sm:
    interface.run(f'./{prefix}_sm.mp4', f'{prefix}_sm')
  elif options.md:
    interface.run(f'./{prefix}_md.mp4', f'{prefix}_md')
  elif options.lg:
    interface.run(f'./{prefix}_lg.mp4', f'{prefix}_lg')
  else:
    interface.run(f'./{prefix}_sm.mp4', f'{prefix}_sm')
    interface.run(f'./{prefix}_md.mp4', f'{prefix}_md')
    interface.run(f'./{prefix}_lg.mp4', f'{prefix}_lg')
