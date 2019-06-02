import time
import datetime
import logging
import threading
from mdc.models.DeviceScheduledCommand import *
from mdc.models.FactReading import *
from mdc.models.DimDevice import *
from mdc.models.DimObis import *
from mdc.models.DeviceSchedules import *
from mdc.models.DimDate import *
from mdc.models.DimTime import *
from mdc.models.FactDeviceConnection import *
from mdc.models.FactEvent import *

logger = logging.getLogger(__name__)

class DeviceComm:
    """Device Communication Object. 
    This Class Object will be used to interact with the meter. 
    This Object will be initialzed through TCP Service which is listening meters.
    """

    pwd = '00000000'
    timeout = 5
    timeout_prev_data = 2

    # Convert a String to hex
    # '00'.encode('utf-8').hex()

    def __init__(self, client_socket, address,bind_ip, port_obj):
        """Initializes Communication Object of Meter
        """
        self.v_list_response = {}
        self.port_obj = port_obj
        self.client_socket = client_socket
        self.address = address
        self.heart_received = client_socket.recv(9924).hex().upper();

        self.msn_hex = format(self.heart_received[self.port_obj.msn_start_pos:self.port_obj.msn_end_pos]) # [24:40]
        self.msn = bytearray.fromhex(self.msn_hex).decode()
        print ('Received1 on {}:{} {}'.format(address[0], address[1], self.heart_received))

        self.is_busy = 0
        self.locked_session_id = 0
        self.priority_wait_count = 0
        self.do_listen = False
        self.is_alive = True
        #Make and entry to message Received and Connection Established.
        dataset_device = DimDevice.objects.filter(msn = self.msn, is_active = True)
        #Return if not an active device is found
        if len(dataset_device) == 0:
            fact_device_received = FactDeviceReceived(id=FactDeviceReceived.get_next_id(), date_id=DimDate.get_current_date_key(), time_id=DimTime.get_current_time_key(), command=self.heart_received, send_receive_status = 'Received',
                                                  comments='New Connection')
            fact_device_received.save()
            return
        self.device = dataset_device[0]
        self.pwd = self.device.device_password.encode('utf-8').hex()
        fact_device_received = FactDeviceReceived(id=FactDeviceReceived.get_next_id(), date_id=DimDate.get_current_date_key(), time_id=DimTime.get_current_time_key(), command=self.heart_received, send_receive_status = 'Received',
                                                  comments='New Connection', device_id = self.device.id)
        fact_device_received.save()
        self.device_connection = FactDeviceConnection(id=FactDeviceConnection.get_next_id(), device_id = self.device.id, connection_date_id = DimDate.get_current_date_key(), 
                                                      connection_time_id = DimTime.get_current_time_key(), client_ip = self.address[0], client_port = self.address[1], server_ip = bind_ip, server_port = self.port_obj.id)
        self.device_connection.save()
        self.device.last_connection_id = self.device_connection.id
        self.device.save()

        self.bt_auth = self.device.get_auth_str()
        self.heart_send = self.device.get_heart_beat_send()
        self.heart_receive = self.device.get_heart_beat_receive()
        self.event_identifier = self.device.device_model.event_identifier
        #self.client_socket.settimeout(self.timeout)

        #Authenticate
        self.is_authenticated = True
        beat_obj = {'obis':'Beat_Str', 'obis_command':self.device.get_heart_beat_send_first()}
        self.send_receive_batch(beat_obj, 1)
        self.is_authenticated = False
        self.authenticate()

        #Listen for commands For Future Use
        listen_handler = threading.Thread(
                target=self.fn_keep_listening,
                args=()
            )
        listen_handler.start();
        

    def get_msn(self):
        """Get the MSN from the object of this class
        Returns:
            str: The return value. MSN from the object.
        """
        return self.msn

    def fn_keep_listening(self):
        """Get the status of Meter if it is alive at the time
        Returns:
            boolean: Returns True if Meter is still responging to this connection and returns False if meter is not responding to this connection.
        """
        i = 0
        while self.is_alive:
            # Receive any previous commands in Buffer
            try:
                v_command_response = None
                #print ('receiving str...')
                response = self.client_socket.recv(9924).hex().upper()
                logger.error('Received : '+response)
                #print('received in fucn:'+response)
                v_command_id = None
                if response != '':
                    i = 0
                    if len(response) >= 22 and response[20:22] >= '20' and response.find(self.heart_receive) == -1 and response.find(self.event_identifier) == -1:
                        v_command_id = response[20:22]
                        self.v_list_response[v_command_id] = response
                    fact_device_received = FactDeviceReceived(id=FactDeviceReceived.get_next_id(), date_id=DimDate.get_current_date_key(), time_id=DimTime.get_current_time_key(),
                                                            device_id = self.device.id, connection_id = self.device_connection.id, command=response, send_receive_status = 'Received',
                                                            command_id = v_command_id, comments=None)
                    fact_device_received.save()

                    # sending heart beat
                    if(response.find(self.heart_receive) > -1):
                        logger.error('sending heart beat')
                        fact_device_received = FactDeviceReceived(id=FactDeviceReceived.get_next_id(), date_id=DimDate.get_current_date_key(), time_id=DimTime.get_current_time_key(),
                                                                    device_id = self.device.id, connection_id = self.device_connection.id, command=self.heart_send, send_receive_status = 'Sent',
                                                                    command_id = None, comments = 'Sent: heart-beat')
                        fact_device_received.save()
                        self.client_socket.send(bytes.fromhex(self.heart_send))
                        self.is_authenticated = False
                    elif(self.event_identifier is not None and response.find(self.event_identifier) > -1):
                                    FactEvent.process_event(response, self.device)
                else:
                    i = i + 1
                    if(i>10):
                        self.close_connection()
            except ConnectionAbortedError as error:
                print('Caught this error in send_command command receiving-1-1: ' + repr(error))
                self.close_connection()
            except ConnectionResetError as error:
                print('Caught this error in send_command command receiving-1-2: ' + repr(error))
                self.close_connection()
            except Exception as error:
                print('Caught this error in send_command command receiving-1-3: ' + repr(error))


    def authenticate(self):
        """Authenticates the connection with the meter with meter password.
        Returns:
            boolean: Returns True if Meter Password is correct and meter is authenticated, Returns False if Meter password is wrong and meter is not authenticated
        """
        if(self.is_authenticated == False):
            auth_obj = {'obis':'Auth_Str', 'obis_command':self.bt_auth}
            self.send_receive_batch(auth_obj, 1, self.locked_session_id)
            self.is_authenticated = True
        return self.is_authenticated


    def close_connection(self):
        try:
            print('Close_connection_started')
            self.is_alive = False
            FactDeviceConnection.close_connection(self.device_connection.id)
            if self.is_busy == 0:
                self.client_socket.close()
            print('Close_connection_ended')
        except:
            return

    def send_receive_batch(self, send_batch, is_on_priority, calling_session_id = 0):
        """This function is used to send the command to meter and return it back.
        """
        self.last_comm = time.time()
        if self.is_alive == True:
            obj = send_batch
            send_str = obj['obis_command']
            command_id = None
            if(not(send_str is None)):
                send_str = send_str.upper()
                if('ZZ' in send_str):
                    command_id = self.device.get_next_command_id()
                    send_str = send_str.replace('ZZ',command_id)
                    try:
                        del self.v_list_response[command_id]
                    except KeyError:
                        None
            v_command_response = None
        
            self.priority_wait_count = self.priority_wait_count + is_on_priority
            response = None
            # Wait for the Priority Threads to be completed
            while (is_on_priority == 0 and self.priority_wait_count > 0 and calling_session_id != self.locked_session_id):
                time.sleep(0.1)
            # Keep waiting if busy or listening
            while (self.is_busy > 0 and calling_session_id != self.locked_session_id):
                time.sleep(0.1)
            if self.is_alive == True:
                self.is_busy = self.is_busy + 1
                self.locked_session_id = self.locked_session_id + 1
                # Receive any previous commands in Buffer
                #self.client_socket.settimeout(self.timeout_prev_data)
                
                #self.client_socket.settimeout(self.timeout)
                if(not(send_str is None)):
                    # Send Command
                    try:
                        if(send_str != self.bt_auth  and not(self.is_authenticated)):
                            logger.error('going to be authenticated')
                            self.authenticate()
                        #print('sending: '+send_str)
                        fact_device_received = FactDeviceReceived(id=FactDeviceReceived.get_next_id(), date_id=DimDate.get_current_date_key(), time_id=DimTime.get_current_time_key(),
                                                                        device_id = self.device.id, connection_id = self.device_connection.id, command=send_str, send_receive_status = 'Sent',
                                                                        command_id = command_id, comments = 'Sent: ' + obj['obis'] )
                        fact_device_received.save()
                        self.client_socket.send(bytes.fromhex(send_str))
                    except ConnectionAbortedError as error:
                        print('Caught this error in send_command command receiving-2-1: ' + repr(error))
                        self.close_connection()
                    except ConnectionResetError as error:
                        print('Caught this error in send_command command receiving-2-2: ' + repr(error))
                        self.close_connection()
                    except Exception as error:
                        print('Caught this error in send_command command receiving-2-3: ' + repr(error))
                        print('Caught this error in send_command command sending: ' + repr(error))
                    
                #Receive Response 
                if(command_id is not None):
                    i = 0
                    while (command_id not in self.v_list_response and i < 70):
                        i = i + 1
                        time.sleep(0.1)
                    v_command_response = self.v_list_response.pop(command_id, None)
                self.is_busy = self.is_busy - 1
                self.priority_wait_count = self.priority_wait_count - is_on_priority
                return v_command_response

    