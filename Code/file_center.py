"""
file_center provides the read and management functions for local files
"""
import math
import os
import pickle
import struct
import time
from queue import Queue
from threading import Thread
import connection_hub, main, download_manager


# config
TEMP_FILE_INFO = 'file_info/'
TEMP_DIRECTORIES = 'directories/'

FILE_INFO_LEN = 3
FILE_INFO_MTIME = 0
FILE_INFO_LAST_MODIFIED = 1
FILE_INFO_NUM_BLOCKS = 2

# size of a file block
BLOCK_SIZE = 20971520  # 20MB

"""
file dictionary

* file_dict format:
{file_name: (file_info, file_reader)}

* file_info format:
[mtime, last modified, num_blocks]
"""
FILE_DICT = {}
FILE_DICT_READER = 1  # DO NOT CHANGE - the code in this file does not rely on this

# grand central dispatch
GCD = None


class FileReader(Thread):
    """
    self.message_queue: (block_num, receive_thread)
    """

    def __init__(self, file_name):
        Thread.__init__(self)
        self.file_name = file_name
        self.message_queue = Queue(0)
        self.prev_time = time.time()
        self.block_status = 0  # 0: run, >0: block

    def send(self, message):
        """
        for other threads: send message to this thread
        :param message: message to send to this thread
        :return: None
        """
        self.message_queue.put(message)

    def block(self):
        self.block_status += 1

    def unblock(self):
        self.block_status -= 1
        if self.block_status < 0:
            raise Exception

    def get_block_status(self):
        """
        block status: 0 - run, >0 - block
        :return: block status
        """
        return self.block_status

    def run(self):
        while True:
            if self.block_status == 0:  # check whether thread is blocked
                if not self.message_queue.empty():
                    message = self.message_queue.get()
                    self.message_queue.task_done()
                    block_num, receive_thread = message
                    # if outbox is recycled, ignore task
                    if not receive_thread.is_on():
                        continue
                    # if outbox too busy, put task back to queue
                    if receive_thread.queue_size() > 5:
                        self.message_queue.put(message)
                        continue
                    # read and send the required block
                    block = self.read(block_num)
                    package = self.message_pack(block_num, block)
                    receive_thread.send(package)
                elif time.time() - self.prev_time > 1:
                    # update previous time (checks modify every >1s)
                    self.prev_time = time.time()
                    self.check_modify()

    def read(self, block_num):
        wait_for_permission(self.file_name)
        f = open(main.FILE_DIR + self.file_name, 'rb')
        f.seek(block_num * BLOCK_SIZE)
        block = f.read(BLOCK_SIZE)
        f.close()
        return block

    def check_modify(self):
        try:
            wait_for_permission(self.file_name)
            # get the file_info
            file_info, _ = FILE_DICT[self.file_name]
            # getmtime() != file_info.mtime: file changed
            if int(os.path.getmtime(main.FILE_DIR + self.file_name)) != file_info[FILE_INFO_MTIME]:
                print('inbox: updating ' + self.file_name + '...')
                # update file_info
                mtime = int(os.path.getmtime(main.FILE_DIR + self.file_name))
                last_modified = mtime
                file_info_update(self.file_name, mtime, last_modified)
        except FileNotFoundError as e:
            # file deleted (probably due to modifying)
            print('inbox: file deleted:', self.file_name, ':', e)

    def message_pack(self, block_num, block):
        """
        packs the information to the outbox format
        block:
        block_num !Q
        file_name_size !Q
        file_name
        block_content
        :return: outbox message
        """
        file_name = self.file_name.encode()
        header = struct.pack('!QQ', block_num, len(file_name))
        outbox_message = header + file_name + block
        package = (connection_hub.MESSAGE_BLOCK, outbox_message)
        return package


class GrandCentralDispatch(Thread):
    def __init__(self):
        Thread.__init__(self)
        self.prev_time = time.time()
        self.block_status = 0  # 0: run, >0: block

    def block(self):
        self.block_status += 1

    def unblock(self):
        self.block_status -= 1
        if self.block_status < 0:
            raise Exception

    def get_block_status(self):
        """
        block status: 0 - run, >0 - block
        :return: block status
        """
        return self.block_status

    def run(self):
        while True:
            # scan the file directory to find new files if not blocked every 1s
            if self.block_status == 0 and time.time() - self.prev_time > 1:
                self.prev_time = time.time()
                self.dispatch()

    def dispatch(self, file_location=''):
        with os.scandir(main.FILE_DIR + file_location) as directory:
            for file in directory:
                if file.is_file():
                    file_name = file_location + file.name
                    # file_name in FILE_DICT: not a new file, continue
                    if file_name in FILE_DICT or self.block_status > 0:  # redundant block_status check
                        continue
                    # new file initiate dispatch
                    print("gcd: adding file: " + file_name + "...")
                    wait_for_permission(file_name)
                    # add to file dict, write file_info to disk, broadcast
                    mtime = int(os.path.getmtime(main.FILE_DIR + file_name))
                    last_modified = mtime
                    num_blocks = get_num_blocks(file_name)
                    file_dict_add(file_name, mtime, last_modified, num_blocks)
                else:
                    dir_name = file.name
                    self.dispatch(file_location + dir_name + '/')


def add_file(file_name, file_info):
    # block the gcd
    GCD.block()
    while GCD.get_block_status == 0:
        continue

    # move file
    download_manager.deliver(file_name)
    # add to file dict
    mtime = int(os.path.getmtime(main.FILE_DIR + file_name))
    last_modified = file_info[FILE_INFO_LAST_MODIFIED]
    num_blocks = file_info[FILE_INFO_NUM_BLOCKS]
    file_dict_add(file_name, mtime, last_modified, num_blocks, write=True, broadcast=False)

    # unblock the gcd
    GCD.unblock()


def update_file(file_name, file_info):
    # block the reader
    _, reader = FILE_DICT[file_name]
    reader.block()
    while reader.get_block_status == 0:
        continue
    # move old file to temp and overwrite
    download_manager.overwrite(file_name)
    # move new file to share
    download_manager.deliver(file_name)
    # update file_info
    mtime = int(os.path.getmtime(main.FILE_DIR + file_name))
    last_modified = file_info[FILE_INFO_LAST_MODIFIED]
    file_info_update(file_name, mtime, last_modified, write=True, broadcast=False)
    # unblock the reader
    reader.unblock()


def wait_for_permission(file_name):
    """
    checks whether the file is still copying
    WARNING: very costy, avoid usage
    :param file_name: the name of the file
    :return: None
    """
    while True:
        try:
            f = open(main.FILE_DIR + file_name)
        except PermissionError:
            continue
        f.close()
        return None


def get_num_blocks(file_name):
    """
    each file is sliced into blocks of BLOCK_SIZE when transmitting
    this function calculates the number of blocks that the file is sliced into
    :param file_name: the name of the file
    :return: the number of blocks that the file is sliced into
    """
    file_size = os.path.getsize(main.FILE_DIR + file_name)
    num_blocks = math.ceil(file_size / BLOCK_SIZE)
    return num_blocks


def file_dict_add(file_name, mtime, last_modified, num_blocks, write=True, broadcast=True):
    file_info = [None for _ in range(FILE_INFO_LEN)]

    file_info[FILE_INFO_MTIME] = mtime
    file_info[FILE_INFO_LAST_MODIFIED] = last_modified
    file_info[FILE_INFO_NUM_BLOCKS] = num_blocks
    file_reader = FileReader(file_name)

    FILE_DICT[file_name] = (file_info, file_reader)

    # write file_info
    if write is True:
        file_info_write(file_name)
    # dispatch reader
    file_reader.start()
    # broadcast
    if broadcast is True:
        broadcast_file_added(file_name)


def file_info_update(file_name, mtime, last_modified, write=True, broadcast=True):
    file_info, _ = FILE_DICT[file_name]
    file_info[FILE_INFO_MTIME] = mtime
    file_info[FILE_INFO_LAST_MODIFIED] = last_modified

    # write file_info
    if write is True:
        file_info_write(file_name)
    # broadcast
    if broadcast is True:
        broadcast_file_modified(file_name)


def file_info_read(file_location=''):
    """
    read existing file info from <temp_dir>/file_info/
    dispatch reader thread for each existing file
    add entry into file_dict
    :return: None
    """
    with os.scandir(main.TEMP_DIR + TEMP_FILE_INFO + file_location) as entries:
        for file in entries:
            if file.is_file():
                file_name = file_location + file.name
                # read file_info from file
                with open(main.TEMP_DIR + TEMP_FILE_INFO + file_name, 'rb') as f:
                    file_info = pickle.load(f)
                mtime, last_modified, num_blocks = file_info
                file_dict_add(file_name, mtime, last_modified, num_blocks, write=False, broadcast=False)

                print("file info read: " + file_name + ' | ' + str(file_info))
            else:
                dir_name = file.name
                file_info_read(file_location + dir_name + '/')


def file_info_write(file_name):
    """
    writes file_info of a file in the file_dict
    to <temp_dir>/file_info/
    :param file_name: the name of the file
    :return: None
    """
    # create file directory if not exist
    file_location = file_name[:len(file_name) - len(file_name.split('/')[-1])]
    if not os.path.exists(main.TEMP_DIR + TEMP_FILE_INFO + file_location):
        os.makedirs(main.TEMP_DIR + TEMP_FILE_INFO + file_location)

    # write file_info
    file_info, _ = FILE_DICT[file_name]
    with open(main.TEMP_DIR + TEMP_FILE_INFO + file_name, 'wb') as f:
        pickle.dump(file_info, f)

    print("file info wrote: " + file_name + ' | ' + str(file_info))


def broadcast_file_modified(file_name):
    outbox_message = file_info_outbox_message(file_name)
    package = (connection_hub.MESSAGE_FILE_MODIFIED, outbox_message)
    for peer_ip in connection_hub.PEER_DICT.keys():
        peer_outbox_thread = connection_hub.PEER_DICT[peer_ip][connection_hub.PEER_DICT_OUTBOX]
        # if outbox is recycled, ignore task
        if not peer_outbox_thread.is_on():
            continue
        peer_outbox_thread.send(package)


def broadcast_file_added(file_name):
    outbox_message = file_info_outbox_message(file_name)
    package = (connection_hub.MESSAGE_FILE_ADDED, outbox_message)
    for peer_ip in connection_hub.PEER_DICT.keys():
        peer_outbox_thread = connection_hub.PEER_DICT[peer_ip][connection_hub.PEER_DICT_OUTBOX]
        # if outbox is recycled, ignore task
        if not peer_outbox_thread.is_on():
            continue
        peer_outbox_thread.send(package)


def file_info_outbox_message(file_name):
    file_info, _ = FILE_DICT[file_name]
    file_name_encoded = file_name.encode()
    file_name_length = len(file_name_encoded)
    pickled_file_info = pickle.dumps(file_info)
    outbox_message = struct.pack('!Q', file_name_length) + file_name_encoded + pickled_file_info

    return outbox_message


def file_dict_outbox_message():
    file_dict = {}
    for file_name in FILE_DICT:
        file_info, _ = FILE_DICT[file_name]
        file_dict[file_name] = file_info
    outbox_message = pickle.dumps(file_dict)
    return outbox_message


def file_center_init():
    """
    initialize the file center
    :return: None
    """
    global GCD

    # initialize the file_info and directories directory
    if not os.path.exists(main.TEMP_DIR + TEMP_FILE_INFO):
        os.makedirs(main.TEMP_DIR + TEMP_FILE_INFO)
    if not os.path.exists(main.TEMP_DIR + TEMP_DIRECTORIES):
        os.makedirs(main.TEMP_DIR + TEMP_DIRECTORIES)

    # read existing file_info if any
    file_info_read()

    # start the grand central dispatch
    GCD = GrandCentralDispatch()
    GCD.start()
