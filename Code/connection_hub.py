"""
connection_hub provides the message exchange functions between hosts

message sequence:
message size (!Q) + message type (!I) + message

message types:
0 - encryption
1 - file dict
2 - file modified
3 - file added
4 - block request
5 - block

encryption:
encryption: ENCRYPTION_NO_ENCRYPTION / ENCRYPTION_WITH_ENCRYPTION

file dict:
file_dict w/ pickle: {file_name: file_info}

file modified:
file_name_size !Q
file_name w/ encode
file_info w/ pickle

file added:
file_name_size !Q
file_name w/ encode
file_info w/ pickle

block request:
block_num !Q
file_name w/ encode

block:
block_num !Q
file_name_size !Q
file_name w/ encode
block_content

outbox message_queue format:
(message_type, message)
"""

from queue import Queue
from threading import Thread
import socket
import struct
import pickle
import file_center, encryption_bureau, compression_station, main, download_manager

PORT = 23456

"""
peer dictionary

* PEER_DICT format:
{peer_host_name: [inbox_thread, outbox_thread]}
"""
PEER_DICT = {}

PEER_DICT_NUM = 2
PEER_DICT_INBOX = 0
PEER_DICT_OUTBOX = 1

MESSAGE_ENCRYPTION = 0
MESSAGE_FILE_DICT = 1
MESSAGE_FILE_MODIFIED = 2
MESSAGE_FILE_ADDED = 3
MESSAGE_BLOCK_REQUEST = 4
MESSAGE_BLOCK = 5

ENCRYPTION_NO_ENCRYPTION = 0
ENCRYPTION_WITH_ENCRYPTION = 1
# encryption default: no encryption
ENCRYPTION_SELF = ENCRYPTION_NO_ENCRYPTION


class Inbox(Thread):
    def __init__(self, inbox_socket, peer_ip):
        Thread.__init__(self)
        self.on = True
        self.encryption = ENCRYPTION_SELF
        self.inbox_socket = inbox_socket
        self.peer_ip = peer_ip

    def is_on(self):
        return self.on

    def off(self):
        self.on = False

    def run(self):
        """
        in case of connection lost (except (ConnectionError, socket.error)): stop
        :return: None
        """
        print('inbox scheduled', self.peer_ip)
        # initialize message size, message type and receive buffer
        message_size = None
        message_type = None
        receive_buffer = ''.encode()
        while True:
            try:
                # stop if self.on is False
                if self.on is False:
                    return None
                receive_stream = self.inbox_socket.recv(524288)
                receive_buffer += receive_stream
                while len(receive_buffer) > 0:
                    if message_size is None and len(receive_buffer) >= 12:
                        header = receive_buffer[:12]
                        message_size, message_type = struct.unpack('!QI', header)
                        receive_buffer = receive_buffer[12:]
                    elif message_size is not None and len(receive_buffer) >= message_size:
                        # unpack message
                        message = receive_buffer[:message_size]
                        receive_buffer = receive_buffer[message_size:]
                        message_size = None
                        print('inbox: message received from:', self.peer_ip, '\tmessage type:',
                              message_type, '\tmessage size:', len(message))

                        # decrypt
                        if self.encryption == ENCRYPTION_WITH_ENCRYPTION and message_type != MESSAGE_ENCRYPTION:
                            message = encryption_bureau.decrypt(message)

                        # process message
                        if message_type == MESSAGE_ENCRYPTION:
                            self.encryption_handler(message)
                        elif message_type == MESSAGE_FILE_DICT:
                            self.file_dict_handler(message)
                        elif message_type == MESSAGE_FILE_MODIFIED or message_type == MESSAGE_FILE_ADDED:
                            self.file_info_handler(message_type, message)
                        elif message_type == MESSAGE_BLOCK_REQUEST:
                            self.block_request_handler(message)
                        elif message_type == MESSAGE_BLOCK:
                            self.block_handler(message)
                    else:
                        break

            except struct.error:
                continue
            except (ConnectionError, TimeoutError, socket.error) as e:  # connection lost: stop
                print('inbox: connection lost', e)
                self.inbox_socket.close()
                return None

    def encryption_handler(self, message):
        encryption = struct.unpack('!I', message)[0]
        if encryption == ENCRYPTION_WITH_ENCRYPTION:
            # update self.encryption
            self.encryption = ENCRYPTION_WITH_ENCRYPTION
            # notify outbox
            outbox_thread = PEER_DICT[self.peer_ip][PEER_DICT_OUTBOX]
            outbox_thread.enable_encryption()

    def file_dict_handler(self, message):
        file_dict = pickle.loads(message)

        package = (self.peer_ip, MESSAGE_FILE_DICT, file_dict)
        download_manager.DOWNLOAD_MANAGER.send(package)

    def file_info_handler(self, message_type, message):
        file_name_size = struct.unpack('!Q', message[:8])[0]
        file_name = message[8:8+file_name_size].decode()
        file_info_pickled = message[8+file_name_size:]
        file_info = pickle.loads(file_info_pickled)

        download_manager_message = (file_name, file_info)
        package = (self.peer_ip, message_type, download_manager_message)
        download_manager.DOWNLOAD_MANAGER.send(package)

    def block_request_handler(self, message):
        # unpack message
        message_header = message[:8]
        block_num = struct.unpack('!Q', message_header)[0]
        file_name = message[8:].decode()

        # get outbox thread
        outbox_thread = PEER_DICT[self.peer_ip][PEER_DICT_OUTBOX]

        # reader: (block_num, receive_thread)
        reader_message = (block_num, outbox_thread)
        reader = file_center.FILE_DICT[file_name][file_center.FILE_DICT_READER]
        reader.send(reader_message)

    def block_handler(self, message):
        # decompress
        if main.compression is True:
            message = compression_station.decompress(message)
        # process message
        block_num, file_name_size = struct.unpack('!QQ', message[:16])
        file_name = message[16:16+file_name_size].decode()
        block = message[16+file_name_size:]

        download_manager_message = (block_num, file_name, block)
        package = (self.peer_ip, MESSAGE_BLOCK, download_manager_message)
        download_manager.DOWNLOAD_MANAGER.send(package)


class Outbox(Thread):
    def __init__(self, peer_ip):
        Thread.__init__(self)
        self.on = True
        self.encryption = ENCRYPTION_SELF
        self.message_queue = Queue(0)
        self.peer_ip = peer_ip

    def is_on(self):
        return self.on

    def off(self):
        self.on = False

    def enable_encryption(self):
        self.encryption = ENCRYPTION_WITH_ENCRYPTION

    def send(self, message):
        self.message_queue.put(message)

    def queue_size(self):
        return self.message_queue.qsize()

    def run(self):
        """
        repeatedly try to connect to target peer
        in case of connection lost (except (ConnectionError, socket.error)): fall back to try connecting
        :return: None
        """
        print('outbox scheduled:', self.peer_ip)
        outbox_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # try to connect
        while True:
            # stop if self.on is False
            if self.on is False:
                while not self.message_queue.empty():
                    self.message_queue.get()
                    self.message_queue.task_done()
                self.message_queue.join()
                return None
            try:
                outbox_socket.connect((self.peer_ip, PORT))
                break
            except (ConnectionError, TimeoutError, socket.error) as e:  # failed to connect
                print('outbox: failed to connect: ', self.peer_ip, e)
                continue

        # at connection establishment: encryption
        encryption = ENCRYPTION_SELF
        outbox_message = struct.pack('!I', encryption)
        encryption_package = (MESSAGE_ENCRYPTION, outbox_message)

        # at connection establishment: send file_dict
        outbox_message = file_center.file_dict_outbox_message()
        file_dict_package = (MESSAGE_FILE_DICT, outbox_message)

        # organize outbox queue:
        # unwanted messages: file added / file modified
        # make sure the encryption message is the first one in the queue
        organized_message_queue = Queue(0)
        organized_message_queue.put(encryption_package)
        organized_message_queue.put(file_dict_package)
        while not self.message_queue.empty():
            package = self.message_queue.get()
            self.message_queue.task_done()
            message_type, _ = package
            if message_type == MESSAGE_FILE_ADDED or message_type == MESSAGE_FILE_MODIFIED:
                continue
            else:
                organized_message_queue.put(package)
        self.message_queue = organized_message_queue

        # connected
        while True:
            # stop if self.on is False
            if self.on is False:
                while not self.message_queue.empty():
                    self.message_queue.get()
                    self.message_queue.task_done()
                self.message_queue.join()
                return None
            # check message_queue and send
            if not self.message_queue.empty():
                # get message from message_queue
                package = self.message_queue.get()
                self.message_queue.task_done()
                message_type, message = package
                # compression
                if message_type == MESSAGE_BLOCK and main.compression is True:
                    message = compression_station.compress(message)
                # encryption
                if self.encryption == ENCRYPTION_WITH_ENCRYPTION and message_type != MESSAGE_ENCRYPTION:
                    message = encryption_bureau.encrypt(message)
                # header
                header = struct.pack('!QI', len(message), message_type)

                try:
                    outbox_socket.send(header)
                    outbox_socket.send(message)
                    print('outbox: message sent to:', self.peer_ip, '\tmessage type:',
                          message_type, '\tmessage size:', len(message))
                except (ConnectionError, TimeoutError, socket.error) as e:  # connection lost
                    print('outbox: connection lost', e)
                    # close the current socket
                    outbox_socket.close()
                    # stop
                    return None


class IOScheduler(Thread):
    def __init__(self):
        Thread.__init__(self)

    def run(self):
        """
        in case of incoming new connection:
        - accept connection socket
        - schedule a new inbox thread
        - update peer_dict

        in case of incoming reconnection
        - accept connection socket
        - schedule a new outbox thread to replace the current outbox thread
        - schedule a new inbox thread to replace the current inbox thread
        - update peer_dict

        :return: None
        """
        scheduler_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        scheduler_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        scheduler_socket.bind(('', PORT))
        scheduler_socket.listen(1)
        print('inbox scheduler is up')

        while True:
            inbox_socket, addr = scheduler_socket.accept()  # addr: ('IP', port)
            peer_ip = addr[0]
            inbox_thread = Inbox(inbox_socket, peer_ip)
            if PEER_DICT[peer_ip][PEER_DICT_INBOX] is None:  # first connection
                PEER_DICT[peer_ip][PEER_DICT_INBOX] = inbox_thread
                inbox_thread.start()
            else:  # reconnection
                # stop old threads
                old_inbox_thread = PEER_DICT[peer_ip][PEER_DICT_INBOX]
                old_outbox_thread = PEER_DICT[peer_ip][PEER_DICT_OUTBOX]
                old_inbox_thread.off()
                old_outbox_thread.off()
                old_inbox_thread.join()
                old_outbox_thread.join()
                # configure new threads
                outbox_thread = Outbox(peer_ip)
                PEER_DICT[peer_ip][PEER_DICT_INBOX] = inbox_thread
                PEER_DICT[peer_ip][PEER_DICT_OUTBOX] = outbox_thread
                outbox_thread.start()
                inbox_thread.start()


def connection_hub_init(peer_list, encryption):
    # encryption configuration
    global ENCRYPTION_SELF
    if encryption is True:
        ENCRYPTION_SELF = ENCRYPTION_WITH_ENCRYPTION
    elif encryption is False:
        ENCRYPTION_SELF = ENCRYPTION_NO_ENCRYPTION

    # initialize PEER_DICT and schedule initial outbox threads
    for peer_ip in peer_list:
        outbox_thread = Outbox(peer_ip)
        peer_threads = [None for _ in range(PEER_DICT_NUM)]
        peer_threads[PEER_DICT_OUTBOX] = outbox_thread
        PEER_DICT[peer_ip] = peer_threads
        outbox_thread.start()

    # start the I/O scheduler
    io_scheduler = IOScheduler()
    io_scheduler.start()
