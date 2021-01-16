# THIS CODE IS ONLY USED FOR RUNNING PYTHON CODES REMOTELY
# YOU CANNOT USE PARAMIKO FOR YOUR PROJECT !!!

# Settings
py_files = ['main.py', 'modules/A.py']
remote_python_interpreter = '/usr/local/bin/python3'
remote_current_working_directory = '/home/tc/workplace/cw1'
remote_ip = '192.168.56.7'
remote_username = 'tc'
remote_password = '123'


def run():
    from os.path import join
    from paramiko import SSHClient
    from paramiko import AutoAddPolicy
    import threading

    ssh = SSHClient()
    ssh.set_missing_host_key_policy(AutoAddPolicy())
    try:
        ssh.connect(remote_ip, username=remote_username, password=remote_password, port=22, timeout=5)

        # mount /mnt/sda1 to home folder as "workplace"
        ssh.exec_command(f'if [ ! -d "workplace" ]; then\nmkdir -p workplace\necho {remote_password} | sudo mount /mnt/sda1 ~/workplace\nfi')
        # change the ownership of workplace folder
        ssh.exec_command(f'echo {remote_password} | sudo chown tc /home/tc/workplace')
        # make the CWD
        ssh.exec_command(f'mkdir -p {remote_current_working_directory}')

        sftp = ssh.open_sftp()
        for f in py_files:
            components = f.split('/')
            if len(components) > 1:  # Files in folders
                target_dir = join(remote_current_working_directory, '/'.join(components[:-1])).replace('\\', '/')
                ssh.exec_command(f'mkdir -p {target_dir}')
            else:  # Files in CWD
                target_dir = remote_current_working_directory.replace('\\', '/')
            print(f'Send {components[-1]} to {remote_ip}:{target_dir}')
            sftp.put(f, join(target_dir, components[-1]).replace('\\', '/'))

        if len(py_files) > 0:
            # the first file will be the main file
            main_py = join(remote_current_working_directory, py_files[0]).replace('\\', '/')
            print(f'###################### RUN {py_files[0]} ######################')
            # execute the code

            stdin, stdout, stderr = ssh.exec_command(f'cd {remote_current_working_directory}; {remote_python_interpreter} {main_py}', bufsize=1, get_pty=True)

            stdout_iter = iter(stdout.readline, '')
            stderr_iter = iter(stderr.readline, '')

            def print_line(it):
                for out in it:
                    if out:
                        print(out.strip())
            # multi threads to print output and error
            th_out = threading.Thread(target=print_line, args=(stdout_iter,))
            th_err = threading.Thread(target=print_line, args=(stderr_iter,))

            th_out.start()
            th_err.start()
            th_out.join()
            th_err.join()

            exit_code = stderr.channel.recv_exit_status()
            print(f'###################### EXIT Code {exit_code} ######################')
        else:
            print('No py files.')

    except Exception as ex:
        print(ex)
        sftp.close()
        ssh.close()
        return -1

    sftp.close()
    ssh.close()


if __name__ == '__main__':
    run()
