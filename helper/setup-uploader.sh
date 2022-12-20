#!/bin/zsh
pip3 install virtualenv && python3 -m venv stream-uploader
source stream-uploader/bin/activate && pip install -r requirements.txt
