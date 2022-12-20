#!/bin/zsh
source stream-uploader/bin/activate
dotenv -e .env python3 mirror.py --watch-directory ../network-logs/node/recordStreams/record0.0.3 --s3-endpoint http://localhost:9000 --debug
