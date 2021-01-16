import os
import argparse
import file_center, download_manager, connection_hub


# config
TEMP_DIR = './temp/'
FILE_DIR = './share/'

peer_list = []
compression = False
encryption = False


def get_arguments():
    # initialize argument parser
    parser = argparse.ArgumentParser(description='TwoDrive')
    parser.add_argument('--ip', action='store', default=None, type=str, help='peer ip addresses')
    parser.add_argument('--encryption', action='store', default='', type=str, help='enable encryption [yes | no]')

    # get arguments from parser
    arguments = parser.parse_args()
    arguments_ip = arguments.ip
    arguments_encryption = arguments.encryption

    # process ip
    if arguments_ip is not None:
        ip_list = arguments_ip.split(',')
        for i in range(len(ip_list)):
            ip = ip_list[i].split('.')
            if len(ip) != 4:
                print('IP format incorrect in:', ip_list[i])
                exit(0)
            for j in range(4):
                try:
                    if int(ip[j]) < 0 or int(ip[j]) > 255:
                        print('IP format incorrect in:', ip_list[i])
                        exit(0)
                except ValueError as e:
                    print('IP format incorrect in:', ip_list[i], e)
                    exit(0)
    else:
        ip_list = []

    # process encryption
    use_encryption = False
    if arguments_encryption == 'yes':
        use_encryption = True

    return ip_list, use_encryption


def main_init():
    print('peer_list:', peer_list)
    print('encryption:', encryption)

    # initialize the temp directory
    if not os.path.exists(FILE_DIR):
        os.makedirs(FILE_DIR)
    if not os.path.exists(TEMP_DIR):
        os.makedirs(TEMP_DIR)


if __name__ == '__main__':
    peer_list, encryption = get_arguments()

    main_init()

    file_center.file_center_init()

    download_manager.download_manager_init()

    connection_hub.connection_hub_init(peer_list, encryption)
