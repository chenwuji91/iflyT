import time
def file_append(file, data):
    file.write(data)
    file.write('\r\n')

file = open('fw1.txt', 'a')
# file = open('fw1.txt')  
# file.seek(5)  
# file.write('hello, hello\r\n')  
file_append(file, time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())))
file_append(file, 'hello, hello')
file_append(file, 'hello, hello')
file.close()  