#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2022/12/6 22:38
# @Author  : Jonathon
# @File    : server.py
# @Software: PyCharm
# @ Motto : 客又至,当如何
import configparser
import socket
import threading
from concurrent.futures import ThreadPoolExecutor
import datetime

'''
大型综合性科研项目用到Python、Java、C++（可内部转岗）
用Python开发对MySql 服务端的透明代理程序PProxy.exe（100代码左右）
1,通过33060端口对默认的3306端口进行代理转发；
2,统计每个MySql客户端的IP和流量（kB）；
3,打印并记录MySql客户端执行的SQL语句；
'''
import sys


def get_size(arg):
    if arg:
        return sys.getsizeof(arg) / 1024
    else:
        return 0


def write_file(string, db_host):
    db_host = db_host or '127.0.0.1'
    with open(f'./{db_host}_record.txt', 'a', encoding='utf-8') as file:
        file.write(string)


# config = configparser.ConfigParser()
# config.read("config.ini", encoding="utf-8")
# host_port = int(config['db']['host_port'])
# db_user = config['db']['db_user']
# db_host = config['db']['db_host']
# password = config['db']['password']
# database = config['db']['database']
# Db1 = Db(host=db_host, user=db_user, password=password, port=host_port, database=database)
# print(Db1.exec_sql('show databases;'))

# def main(connection, address):
#     print(f'client:{address}')
#     buf = connection.recv(1024)
#     client.send(buf)
#     time.sleep(1)
#     buf1 = client.recv(1024)
#     print(buf1)
#     # cur = Db(host, user, password, port, database)
#     # IP = address[0] + ':' + str(address[1])
#     IP = address[0]
#     # lock.acquire()
#     try:
#         df = pd.read_excel(f'./{address}_ip_size.xlsx', sheet_name='ip', index_col=[0])
#     except:
#         df = dict()
#         df['ip'] = [IP]
#         df['flow'] = [0]
#         df = pd.DataFrame(data=df, index=[0])
#     size = int(df[df['ip'] == IP]['flow'].values[0]) if IP in df['ip'].values else 0
#     try:
#         connection.settimeout(5)
#         buf = connection.recv(2048)
#         buf = buf.decode()
#         try:
#
#             data = 0
#
#             size += get_size(data)
#             if IP not in df['ip'].values:
#                 temp = [IP, size]
#                 df.loc[len(df)] = temp
#             else:
#                 df.loc[df[df.ip == IP].index.tolist(), 'flow'] = size
#             df.to_excel(f'./{address}_ip_size.xlsx', sheet_name='ip')
#             stamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
#             reponse = f'[{stamp}]\n' + 'ip:' + IP + '\n' + '执行sql:' + buf + '\n' + '累计流量:' + str(size) + 'kb' + '\n'
#             print(reponse)
#             write_file(reponse)
#             connection.send(f'server answer:流量{get_size(data)}KB'.encode())
#             connection.close()
#         except Exception as ex:
#             print(ex)
#             connection.send(f'{ex}'.encode())
#
#     except socket.timeout as ex:
#         print(ex + 'time out')
class CToSPacket(threading.Thread):
    def __init__(self, c: socket.socket, s: socket.socket, address):
        threading.Thread.__init__(self)
        self.c = c
        self.s = s
        self.address = address[0] + '_' + str(address[1])

    def run(self) -> None:
        string = ''
        while True:
            packet_len = self.c.recv(3)
            if packet_len == b'':
                break
            len_of_packet = int.from_bytes(packet_len, 'little')
            rest_packet = self.c.recv(len_of_packet + 1)
            packet = packet_len + rest_packet
            string = str(packet).split('\\')[-1].replace(
                'x03', '').replace('\'', '')
            print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]client_sql:{string}")
            write_file(f'[{datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}]' + string + '\n', self.address)
            self.s.sendall(packet)


class SToCPacket(threading.Thread):
    def __init__(self, c: socket.socket, s: socket.socket, address):
        threading.Thread.__init__(self)
        self.c = c
        self.s = s
        self.address = address[0] + ':' + str(address[1])

    def run(self):
        size = 0
        while 1:
            packet_len = self.s.recv(3)
            if packet_len == b'':
                break
            len_of_packet = int.from_bytes(packet_len, 'little')
            rest_packet = self.s.recv(len_of_packet + 1)
            packet = packet_len + rest_packet
            size += get_size(packet)
            print(
                f'[{datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}]' + 'server_info:' + f'ip:{self.address} [flow]:{size}Kb')
            self.c.sendall(packet)


if __name__ == '__main__':
    host1 = '127.0.0.1'  # Local Server IP

    host2 = '127.0.0.1'  # Real Server IP

    port1 = 33060  # Local Server Port

    port2 = 3306  # Real Server Port

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
        server.bind((host1, port1))
        server.listen(2)
        c, addr = server.accept()
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((host2, port2))
        tp_s2c = SToCPacket(c, s, addr)
        tp_c2s = CToSPacket(c, s, addr)
        tp_c2s.start()
        tp_s2c.start()
        tp_s2c.join()
        tp_c2s.join()
