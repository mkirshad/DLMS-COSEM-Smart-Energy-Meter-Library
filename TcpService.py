import socket
import threading
from pprint import pprint
import datetime
from mdc.controllers.DeviceComm import *
from mdc.models.DeviceScheduledCommand import *
from mdc.models.FactReading import *
from mdc.models.DimDevice import *
from mdc.models.DimObis import *
from mdc.models.DeviceSchedules import *
from mdc.models.DimDate import *
from mdc.models.DimTime import *
from django.shortcuts import render
from django.db import connection
from django.template import RequestContext
import datetime,time
import threading
import logging
import subprocess
import time
from django.conf import settings
import json

logger = logging.getLogger(__name__)

class TcpService(object):
    """TCP Service Class"""
    # List Of Device Connections
    vlist = {}

    @staticmethod
    def handle_client_connection(client_socket, address, bind_ip, port_obj):
        """This function is called in thread when a connection from meter is established.
            This function Initiates DeviceCommObject 
            authenticates it 
            and on successfull authentication it set this object to ListDeviceComm
        """
        obj = DeviceComm(client_socket, address,bind_ip, port_obj)
        obj_prev = TcpService.get_comm_obj(obj.msn)
        if(obj_prev is not False):
            obj_prev.close_connection()
        TcpService.set_comm_obj(obj)
        print('Dcitionary Keys are :')
        print(TcpService.vlist.keys())
        #obj.send_command('000100110001000dc001ZZ00030100010800ff0200')

    @staticmethod
    def set_comm_obj(obj):
        """ This is static method & it is used to set the object
            Returns:
                It doesn't return anything
        """
        TcpService.vlist[obj.get_msn()] = obj

    @staticmethod
    def get_comm_obj(msn):
        """ This is static method & it returns comm object
            Returns:
                object which is previous set by setter method
        """
        try:
            obj = TcpService.vlist[msn]
            if(obj.is_alive):
                return obj
            else:
                return False
        except:
            return False

    @staticmethod
    def main(port_obj):
        logger.error('Main is executed from TcpService')
        bind_ip = socket.gethostname()
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.bind((settings.SERVER_IP, port_obj.id))
        server.listen(50000)  # max backlog of connections

        logger.error('Listening on {}:{}'.format(bind_ip,port_obj.id))
    
        while True:
            client_sock, address = server.accept()
            logger.error('Accepted connection from {}:{}'.format(address[0], address[1] ))
            client_handler = threading.Thread(
                target=TcpService.handle_client_connection,
                args=(client_sock,address,bind_ip,port_obj)  # without comma you'd get a... TypeError: handle_client_connection() argument after * must be a sequence, not _socketobject
            )
            client_handler.start();

# TcpService.main()