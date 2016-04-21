# -*- coding: utf8 -*-

import glob
import os
dir = 'E:/test_python/tt'  #要访问文件夹路径
suffix = 'txt'               #后缀名称
f = glob.glob(dir + '//*.' + suffix)
for file in f :
    filename = os.path.basename(file)
    #print filename
    f =open(dir + '//'+filename,'r')
    for eachline in f:
        print eachline,                # 后面跟 ',' 将忽略换行符
    f.close()
