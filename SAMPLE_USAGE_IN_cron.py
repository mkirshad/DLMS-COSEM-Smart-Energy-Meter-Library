"""Definition of cron(Scheduler) controller for VERTEX_MDC.
"""
from mdc.models.DeviceScheduledCommand import *
from mdc.models.FactReading import *
from mdc.models.DimDevice import *
from mdc.models.DimObis import *
from mdc.models.DeviceSchedules import *
from mdc.models.DimDate import *
from mdc.models.DimTime import *
from mdc.models.DimTcpPort import *
from mdc.models.DimDeviceModel import *
from mdc.models.DimObis import *
from django.shortcuts import render
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.csrf import ensure_csrf_cookie
from django.core import serializers
from django.http import JsonResponse
from django.http import HttpRequest
from django.http import HttpResponse
from django.db import connection
from django.template import RequestContext
import datetime,time
import json
import pytz
import threading
import queue as queue
import logging
from mdc.controllers.TcpService import *
from mdc.controllers.DeviceComm import *
import subprocess
import time
from django.conf import settings
from django.db.models import Q
import time
from django.forms.models import model_to_dict
from mdc.models.DimEvent import *
from mdc.models.FactEvent import *

logger = logging.getLogger(__name__)

def exec_commands(msn, arr_obis, dev_id):
    """Renders exec_commands thread for each msn.
    """
    #logger.error('Exec command is called for msn ' + msn + json.dumps(arr_obis))
    try:
        iter = 0
        #logger.error('Exec command is called for msn ' + msn + json.dumps(arr_obis))
        for obj in arr_obis:
            obis_response = None
            iter = 0
            while obis_response == None and iter < 20:
                dev_comm_obj=TcpService.get_comm_obj(msn)
                if (dev_comm_obj is False):
                    logger.error('comObj not found-batch')
                else:
                    obis_response = dev_comm_obj.send_receive_batch(obj, 0)
                    if(obis_response != None):
                        obj2 = DimDeviceModelObis(obis_id=obj['obis'], obis_command=obj['obis_command'], value_start_pos=obj['value_start_pos'], 
                                                  value_end_pos=obj['value_end_pos'], multiplier=obj['multiplier'], denominator=obj['denominator']);
                        obis_val = DimObis.parse_val(obis_response, obj2)
                        if(obis_val == None):
                            logger.error(str(datetime.datetime.now()) + ' Value is not prased')
                        else:
                            fact_reading_obj = FactReading(id=FactReading.get_next_id(), mdc_date_id=DimDate.get_current_date_key(), mdc_time_id=DimTime.get_current_time_key(), dev_date_id=DimDate.get_current_date_key(), dev_time_id=DimTime.get_current_time_key(), device_id=dev_id, obis_id=obj['obis'], val=obis_val)
                            fact_reading_obj.save()
                            logger.error(str(datetime.datetime.now()) + ' Value is saved to Db')
                if(obis_response == None):
                    time.sleep(10)
    except Exception as e:
        logger.error('Something went wrong exec_commands! ' + str(e))

def scheduled_reads(request):
    """Renders the commands needed 
       for the execution of reading procedure from the meter.
    """
    logger.error(str(datetime.datetime.now()) + 'scheduled_reads is executed')
    currentDT = datetime.datetime.now()
    dte=currentDT.strftime("%Y")+'-'+currentDT.strftime("%m")+'-'+currentDT.strftime("%d")+'T'+currentDT.strftime("%H")+':'+currentDT.strftime("%M")+':'+'00'
    date_object = datetime.datetime.strptime(dte, '%Y-%m-%dT%H:%M:%S')

    format="%Y-%m-%d %H:%M:%S"
    last_device_id=''
    dataset_device_schedule_command = DeviceScheduledCommand.objects.filter(start_datetime= date_object).order_by('msn').values('device_id', 'msn', 'obis', 'obis_command', 'value_start_pos', 'value_end_pos', 'multiplier', 'denominator', 'schedule_id')
    dataset_count = len(dataset_device_schedule_command)

    list_obis = []
    last_msn=0;
    last_schedule_id = 0
    for idx, obj in enumerate(dataset_device_schedule_command):
        if((last_msn != obj['msn'] and idx != 0) or idx == (dataset_count-1)):
            if(idx == (dataset_count-1)):
                last_msn=obj['msn']
                last_device_id=obj['device_id']
                list_obis.append(obj)
            list_obis_copy = list_obis[:]
            thread1 = threading.Thread(target=exec_commands, args=(last_msn, list_obis_copy, last_device_id, )) #create thread
            thread1.start()   #start thread
            del list_obis[:]  #unset list 

        if(idx != (dataset_count-1)):
            list_obis.append(obj)
            last_msn=obj['msn']
            last_device_id=obj['device_id']
        
        if(last_schedule_id != obj['schedule_id']):
            last_schedule_id = obj['schedule_id']
            dev_schedule = DeviceSchedules.objects.get(id=last_schedule_id)
            dev_schedule.start_datetime = date_object
            dev_schedule.save()

    return HttpResponse('execution completed')

def start_tcp_service(request):
    """This Service Method is executed on Web Application Startup in-order to start TCP service
    """
    #Close all Previous Open Connections --Start
    dataset_open_connections = DimDevice.objects.filter(~Q(last_connection_id = None)).values_list('last_connection_id')
    for obj_connection_id in dataset_open_connections:
        FactDeviceConnection.close_connection(obj_connection_id[0])
    #Close all Previous Open Connections --End
    request2 = request
    thread1 = threading.Thread(target=start_tcp_service_thread, args=(0,)) #create thread
    thread1.start()     #start thread

    thread2 = threading.Thread(target=start_scheduled_reads_thread, args=(request2,)) #create thread
    thread2.start() #start thread
    return HttpResponse('service is started')

def start_scheduled_reads_thread(request):
    request2 = request
    while True:
        try:
            thread1 = threading.Thread(target=scheduled_reads, args=(request2,)) #create thread
            thread1.start() #start thread
            time.sleep(60)
        except Exception as e:
            logger.error('Something went wrong! ' + str(e))

def start_tcp_service_thread(port):
    """This Method is executed in-order to Start TCP Service
    """
    logger.error('start_tcp_service_thread is executed error!')
    if port == 0:
        dataset_port = DimTcpPort.objects.all()
        for idx, port_obj in enumerate(dataset_port):
            thread = threading.Thread(target=start_tcp_service_main, args=(port_obj,)) #create thread
            thread.start() #start thread
    else:
        port_obj = DimTcpPort.objects.get(id=port)
        thread = threading.Thread(target=start_tcp_service_main, args=(port_obj,)) #create thread
        thread.start() #start thread

def start_tcp_service_main(port_obj):
    """Start TCP Service
    """
    logger.error('start_tcp_service_main is executed info!')
    TcpService.main(port_obj)

def pull(request):
    """Deploys Code on Server
    """
    subprocess.call([r'D:\work\dev\pull.bat'])
    start_tcp_service(request)
    return HttpResponse('Pull Completed')

def ondeman_exec_command(device_id, obis_code, action='Read', write_param=None):
    """Renders exec_commands thread for each msn.
    """
    print('Dcitionary Keys are :')
    print(TcpService.vlist.keys())
    return_val = 'N/A'
    return_unit = 'N/A'
    obj_device = DimDevice.objects.get(id = device_id)
    obj_obis = DimObis.objects.get(id=obis_code)
    msn = None
    logger.error('device count:')
    logger.error(obj_device)
    if obj_device != None and obj_obis != None:
        return_unit=obj_obis.unit
        msn = obj_device.msn
        print ('msn is :'+msn)
        obj_obis_command = None
        if (action == 'Read'):
            obj_obis_command = DimDeviceModelObis.objects.get(device_model=obj_device.device_model, obis=obj_obis)
        elif (action == 'Write'):
            obj_obis_command_read = DimDeviceModelObis.objects.get(device_model=obj_device.device_model, obis=obj_obis)
            obj_obis_command = DimDeviceModelObisWrite.objects.get(device_model=obj_device.device_model, obis=obj_obis)
            if(obj_obis_command_read.denominator is not None):
                write_param = float(write_param) * obj_obis_command_read.denominator
                logger.error('its going to write:'+str(write_param))
        elif (action == 'Exec'):
            obj_obis_command = DimDeviceModelObisExec.objects.get(device_model=obj_device.device_model, obis=obj_obis)
        obis_command = None
        if obj_obis_command != None:
            obis_command = DimObis.get_obis_command(obj_obis_command,write_param,action ) #obj_obis_command.obis_command
            obj = {'device_id':device_id, 'msn':msn, 'obis':obis_code, 'obis_command':obis_command}
            print(json.dumps(obj))
            obis_response = None
            iter = 0
            while obis_response == None and iter < obj_device.device_model.reponse_none_retry_count:
                try:
                    dev_comm_obj=TcpService.get_comm_obj(msn)
                    if (dev_comm_obj is False):
                        logger.error('comObj not found-demand')
                    else:
                        logger.error('comObj found')
                        obis_response = dev_comm_obj.send_receive_batch(obj, 1)
                        logger.error('obis response is received ****1')
                        if(obis_response != None):
                            obis_val = DimObis.parse_val(obis_response, obj_obis_command)
                            return_val = obis_val
                        logger.error('obis response is parsed ****2')
                    if(obis_response == None):
                        time.sleep(obj_device.device_model.reponse_none_retry_wait)
                    iter = iter + 1
                    logger.error('its iteration :'+str(iter))
                except Exception as e:
                        logger.error('Something went wrong exec_commands! ' + str(e))
    logger.error(return_val)
    return {'val':str(return_val), 'unit':return_unit}

def get_device_param_value(request):
    query_obis=request.GET.get('param',None)
    device_id=request.GET.get('device_id',None)
    action=request.GET.get('action','Read')
    write_param=request.GET.get('write_param',None)
    lst_val_json = ondeman_exec_command(device_id, query_obis, action, write_param)
  
    """
    lst_val_json=[
                        {

                            "value" : "0.456",
                        },
               
                        {
                            "unit" : "Kwh",
                        }
    ]
    """
    return JsonResponse(lst_val_json) 


