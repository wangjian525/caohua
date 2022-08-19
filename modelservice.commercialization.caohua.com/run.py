# -*- coding:utf-8 -*-
import argparse

parser = argparse.ArgumentParser(description='Runing')

parser.add_argument('--vfe', action='store_true', help='Encrypted Version')

parsers = parser.parse_args()

if parsers.vfe:
    from build.web_application import main  # 加密版
else:
    from web_application import main  # 源码版

main()
