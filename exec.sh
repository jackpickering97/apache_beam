#!/bin/bash
mkdir output
py -3.10 -m virtualenv venv
venv\\Scripts\\activate.bat
python -m pip install -r requirements.txt
python main.py