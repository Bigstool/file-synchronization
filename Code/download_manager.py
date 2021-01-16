"""
download_manager provides the download management functions
supports file_center_7
"""
import math
import pickle
import os
import struct
import shutil
from queue import Queue
from threading import Thread
import connection_hub, file_center, main

# config
TEMP_DOWNLOAD_INFO = 'download_info/'
TEMP_DOWNLOADING = 'downloading/'

"""
download dictionary

* download_dict format:
{file_name: (file_info, block_info)}

* file_info format: (reference -> file_center)
[file_type, mtime, last modified, num_blocks]
file_type: 'f' - file, 'd' - directory

* block_info format:
[block_status, block_status, ...]
block_status: 0 - to download 1 - downloading 2 - downloaded
"""
DOWNLOAD_DICT = {}
DOWNLOAD_FILE_INFO = 0
DOWNLOAD_BLOCK_INFO = 1

BLOCK_TO_DOWNLOAD = 0
BLOCK_DOWNLOADING = 1
BLOCK_DOWNLOADED = 2
BLOCK_TO_PARTIAL_UPDATE = 3
BLOCK_PARTIAL_UPDATING = 4
BLOCK_PARTIAL_UPDATED = 5

DOWNLOAD_MANAGER = None


class DownloadManager(Thread):
    """
    message_queue format: (peer_ip, message_type, message)

    message_type: (reference -> connection_hub)
    MESSAGE_FILE_DICT = 1
    MESSAGE_FILE_MODIFIED = 2
    MESSAGE_FILE_ADDED = 3
    MESSAGE_BLOCK = 5

    message:
    file dict: file_dict - {file_name: [file_info]}
    file modified: (file_name, [file_info])
    file added: (file_name, [file_info])
    block: (block_num, file_name, block)
    """

    def __init__(self):
        Thread.__init__(self)
        self.message_queue = Queue(0)

    def send(self, message):
        self.message_queue.put(message)

    def run(self):
        while True:
            if not self.message_queue.empty():
                package = self.message_queue.get()
                self.message_queue.task_done()
                peer_ip, message_type, message = package
                if message_type == connection_hub.MESSAGE_FILE_DICT:
                    file_dict = message
                    file_dict_handler(peer_ip, file_dict)
                elif message_type == connection_hub.MESSAGE_FILE_ADDED:
                    file_name, file_info = message
                    file_added_handler(peer_ip, file_name, file_info)
                elif message_type == connection_hub.MESSAGE_FILE_MODIFIED:
                    file_name, file_info = message
                    file_modified_handler(peer_ip, file_name, file_info)
                elif message_type == connection_hub.MESSAGE_BLOCK:
                    block_num, file_name, block = message
                    block_handler(block_num, file_name, block)

            # check for completed downloads
            check_download_complete()


def file_dict_handler(peer_ip, file_dict):
    """
    does not provide: modify detection (pending)
    :param peer_ip:
    :param file_dict:
    :return:
    """
    for file_name in file_dict.keys():
        if file_name not in file_center.FILE_DICT.keys():
            if file_name not in DOWNLOAD_DICT.keys():
                # download hasn't started, schedule new download
                file_info = file_dict[file_name]
                new_download(peer_ip, file_name, file_info)
            else:
                # download started (downloading or reconnect)
                _, block_info = DOWNLOAD_DICT[file_name]
                if BLOCK_TO_PARTIAL_UPDATE in block_info:  # file was being partial updated
                    # continue to partial update
                    continue_partial_update(peer_ip, file_name)
                elif BLOCK_TO_DOWNLOAD in block_info:  # file was being downloaded
                    # continue to download
                    continue_download(peer_ip, file_name)
                else:  # file is downloading
                    continue


def file_added_handler(peer_ip, file_name, file_info):
    if file_name not in file_center.FILE_DICT.keys():
        if file_name not in DOWNLOAD_DICT.keys():
            # download hasn't started, schedule new download
            new_download(peer_ip, file_name, file_info)


def file_modified_handler(peer_ip, file_name, file_info):
    if file_name in file_center.FILE_DICT.keys():
        # file exists, initialize partial update
        new_partial_update(peer_ip, file_name, file_info)
    else:
        # file does not exist, initialize new download
        file_added_handler(peer_ip, file_name, file_info)


def block_handler(block_num, file_name, block):
    # retrieve block info
    try:
        block_info = DOWNLOAD_DICT[file_name][DOWNLOAD_BLOCK_INFO]
    except IndexError as e:
        print('download manager: block handler: no such downloading file:', file_name, e)
        return None
    # if the block is downloading or partial updating, save and update download_dict
    if block_info[block_num] == BLOCK_DOWNLOADING or block_info[block_num] == BLOCK_PARTIAL_UPDATING:
        # write block to temp
        f = open(main.TEMP_DIR + TEMP_DOWNLOADING + file_name + '_block' + str(block_num), 'wb')
        f.write(block)
        f.close()
        # update download_dict
        if block_info[block_num] == BLOCK_DOWNLOADING:
            download_info_update(file_name, block_num, block_status=BLOCK_DOWNLOADED)
        elif block_info[block_num] == BLOCK_PARTIAL_UPDATING:
            download_info_update(file_name, block_num, block_status=BLOCK_PARTIAL_UPDATED)


def check_download_complete():
    file_names = list(DOWNLOAD_DICT.keys())
    for file_name in file_names:
        file_info, block_info = DOWNLOAD_DICT[file_name]
        # check download finished: no to download & downloading & to partial update & partial updating
        if BLOCK_TO_DOWNLOAD in block_info or BLOCK_DOWNLOADING in block_info or \
                BLOCK_TO_PARTIAL_UPDATE in block_info or BLOCK_PARTIAL_UPDATING in block_info:
            continue
        if BLOCK_PARTIAL_UPDATED not in block_info:  # downloaded: partial updated not in block_info
            # assemble file
            with open(main.TEMP_DIR + TEMP_DOWNLOADING + file_name, 'wb') as f:
                for block_num in range(len(block_info)):
                    with open(main.TEMP_DIR + TEMP_DOWNLOADING + file_name + '_block' + str(block_num), 'rb') as r:
                        block = r.read()
                        f.write(block)
            # call file_center.add_file(file_name, file_info)
            file_center.add_file(file_name, file_info)
            # delete blocks
            for block_num in range(len(block_info)):
                os.remove(main.TEMP_DIR + TEMP_DOWNLOADING + file_name + '_block' + str(block_num))
        else:  # partial updated: partial updated in block_info
            # call file_center.update_file(file_name, file_info)
            file_center.update_file(file_name, file_info)
            # delete blocks
            for block_num in range(len(block_info)):
                if block_info[block_num] == BLOCK_PARTIAL_UPDATED:
                    os.remove(main.TEMP_DIR + TEMP_DOWNLOADING + file_name + '_block' + str(block_num))
                else:
                    break
        # delete download_info from download_dict and file
        DOWNLOAD_DICT.pop(file_name)
        os.remove(main.TEMP_DIR + TEMP_DOWNLOAD_INFO + file_name)


def new_download(peer_ip, file_name, file_info):
    # create file directory if not exist
    file_location = file_name[:len(file_name) - len(file_name.split('/')[-1])]
    if not os.path.exists(main.FILE_DIR + file_location):
        os.makedirs(main.FILE_DIR + file_location)
    if not os.path.exists(main.TEMP_DIR + TEMP_DOWNLOADING + file_location):
        os.makedirs(main.TEMP_DIR + TEMP_DOWNLOADING + file_location)
    # set up download entry
    num_blocks = file_info[file_center.FILE_INFO_NUM_BLOCKS]
    block_info = [BLOCK_DOWNLOADING for _ in range(num_blocks)]
    download_dict_add(file_name, file_info, block_info, write=True)
    # send block request
    for block_num in range(num_blocks):
        send_block_request(peer_ip, block_num, file_name)


def new_partial_update(peer_ip, file_name, file_info):
    num_blocks = file_info[file_center.FILE_INFO_NUM_BLOCKS]
    num_partial_update = math.ceil(num_blocks * 0.002)
    # start new partial update
    block_info = [BLOCK_DOWNLOADED for _ in range(num_blocks)]
    download_dict_add(file_name, file_info, block_info, write=True)
    for block_num in range(num_partial_update):
        block_info[block_num] = BLOCK_PARTIAL_UPDATING
        # send block request
        send_block_request(peer_ip, block_num, file_name)


def continue_download(peer_ip, file_name):
    _, block_info = DOWNLOAD_DICT[file_name]
    for block_num in range(len(block_info)):
        if block_info[block_num] == BLOCK_TO_DOWNLOAD:
            block_info[block_num] = BLOCK_DOWNLOADING
            # send block request
            send_block_request(peer_ip, block_num, file_name)


def continue_partial_update(peer_ip, file_name):
    _, block_info = DOWNLOAD_DICT[file_name]
    for block_num in range(len(block_info)):
        if block_info[block_num] == BLOCK_TO_PARTIAL_UPDATE:
            block_info[block_num] = BLOCK_PARTIAL_UPDATING
            # send block request
            send_block_request(peer_ip, block_num, file_name)


def deliver(file_name):
    # move file
    shutil.move(main.TEMP_DIR + TEMP_DOWNLOADING + file_name, main.FILE_DIR + file_name)


def overwrite(file_name):
    file_info, block_info = DOWNLOAD_DICT[file_name]
    # move the current file to temp
    shutil.move(main.FILE_DIR + file_name, main.TEMP_DIR + TEMP_DOWNLOADING + file_name)
    # overwrite file
    with open(main.TEMP_DIR + TEMP_DOWNLOADING + file_name, 'rb+') as f:
        for block_num in range(len(block_info)):
            if block_info[block_num] == BLOCK_PARTIAL_UPDATED:
                with open(main.TEMP_DIR + TEMP_DOWNLOADING + file_name + '_block' + str(block_num), 'rb') as r:
                    block = r.read()
                    f.write(block)
            else:
                break


def send_block_request(peer_ip, block_num, file_name):
    outbox_message = struct.pack('!Q', block_num) + file_name.encode()
    package = (connection_hub.MESSAGE_BLOCK_REQUEST, outbox_message)
    outbox_thread = connection_hub.PEER_DICT[peer_ip][connection_hub.PEER_DICT_OUTBOX]
    # if outbox is recycled, ignore task
    if not outbox_thread.is_on():
        return None
    outbox_thread.send(package)


def download_dict_add(file_name, file_info, block_info, write=True):
    DOWNLOAD_DICT[file_name] = (file_info, block_info)
    if write is True:
        download_info_write(file_name)


def download_dict_read(file_location=''):
    with os.scandir(main.TEMP_DIR + TEMP_DOWNLOAD_INFO + file_location) as entries:
        for file in entries:
            if file.is_file():
                file_name = file_location + file.name
                # read download_info from file
                with open(main.TEMP_DIR + TEMP_DOWNLOAD_INFO + file_name, 'rb') as f:
                    download_info = pickle.load(f)
                file_info, block_info = download_info
                # restart: downloading -> to download, partial updating -> to partial update
                for i in range(len(block_info)):
                    if block_info[i] == BLOCK_DOWNLOADING:
                        block_info[i] = BLOCK_TO_DOWNLOAD
                    if block_info[i] == BLOCK_PARTIAL_UPDATING:
                        block_info[i] = BLOCK_TO_PARTIAL_UPDATE
                # add entry to download_dict
                download_dict_add(file_name, file_info, block_info, write=False)
            else:
                dir_name = file.name
                download_dict_read(file_location + dir_name + '/')


def download_info_update(file_name, block_num, block_status, write=True):
    # get block_info
    block_info = DOWNLOAD_DICT[file_name][DOWNLOAD_BLOCK_INFO]
    # update block_info
    block_info[block_num] = block_status
    # write block_info
    if write is True:
        download_info_write(file_name)


def download_info_write(file_name):
    # create file directory if not exist
    file_location = file_name[:len(file_name) - len(file_name.split('/')[-1])]
    if not os.path.exists(main.TEMP_DIR + TEMP_DOWNLOAD_INFO + file_location):
        os.makedirs(main.TEMP_DIR + TEMP_DOWNLOAD_INFO + file_location)

    # write download_info
    with open(main.TEMP_DIR + TEMP_DOWNLOAD_INFO + file_name, 'wb') as f:
        pickle.dump(DOWNLOAD_DICT[file_name], f)


def download_manager_init():
    global DOWNLOAD_MANAGER

    # initialize the download_info and downloading folder
    if not os.path.exists(main.TEMP_DIR + TEMP_DOWNLOAD_INFO):
        os.makedirs(main.TEMP_DIR + TEMP_DOWNLOAD_INFO)
    if not os.path.exists(main.TEMP_DIR + TEMP_DOWNLOADING):
        os.makedirs(main.TEMP_DIR + TEMP_DOWNLOADING)

    # read download_info
    download_dict_read()

    # start download_manager
    DOWNLOAD_MANAGER = DownloadManager()
    DOWNLOAD_MANAGER.start()
