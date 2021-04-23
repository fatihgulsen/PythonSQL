import pandas as pd
import os
import patoolib

dosya_dir = r'C:\Users\fatih\Desktop\PythonSQL'

listdir_zip = [i for i in os.listdir(dosya_dir) if i.endswith('.rar') or i.endswith('.zip')]

for i in listdir_zip:
    patoolib.extract_archive(dosya_dir+r'\\'+i, outdir=dosya_dir+r'\\unzip')

listdir_xlsx = [i for i in os.listdir(dosya_dir+r'\\unzip')]
