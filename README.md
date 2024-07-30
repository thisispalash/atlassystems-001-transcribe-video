## Upload Large Files to cloud (and transcribe*)

> All code is on MacOS Sonoma 14.0 on Apple M2

- See minio installation guide [here](https://min.io/download?license=agpl&platform=macos) (download server).
- Installation of server is optional. Can use public development server hosted by minio.
- Youtube Videos used ([download tool](https://en.y2mate.is/v75/)) : [small (720p)](https://youtu.be/s1sv7iFDdxY?si=YNjNti-ZSKpCnxiD), [medium (1080p)](https://youtu.be/e-gwvmhyU7A?si=j77vKsbq7m00mpjs), [large (1080p)](https://youtu.be/Osh0-J3T2nY?si=p-BY-ZjrOITC1aYh).


## Running Instructions

Ensure the videos are downloaded and renamed as `yt_sm.mp4`, `yt_md.mp4`, and `yt_lg.mp4`. Or if using protected videos in conjunction with local server, use `himalaya_sm.mp4`, `himalaya_md.mp4`, and `himalaya_lg.mp4`.

> To start minio server
```bash
$ minio server ./data --console-address :9001
```

> To run script (in another window)
```bash
$ python3 -m venv .venv
$ source .venv/bin/activate
$ pip3 install -r requirements.txt
$ python3 app.py
```

> Available flags

- `-p` This flag uses the public minio server. Default is `False`.
- `-d` This flag makes the script download the uploaded file to verify checksums. Default is `False`. (**not implemented yet**)
- `-w` This flag sets the number of workers for parallel chunk uploads. Default is `8`.
>> using workers reduced upload of 862MB file from ~425s to ~388s.

> Selecting which sized file to upload
>> By default all three sizes (sm, md, lg) are uploaded sequentially

- `-s` This flag only uploads the small file.
- `-m` This flag only uploads the medium file.
- `-l` This flag only uploads the large file.