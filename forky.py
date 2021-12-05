import socket, os, sys, array

# https://gist.github.com/jmhobbs/11276249
socket_path = os.path.join(os.environ['XDG_RUNTIME_DIR'], 'echo.socket')

STDIN=0
STDOUT=1
STDERR=2

def _server(preload, payload, timeout=None):
    print(f"Executing preload")
    exec(preload)
    print("Done.")

    # handle the race condition where two programs are launched
    # at the same time, and try to create the same socket. We'll create
    # our socket in {socket_path}.{pid}, then mv to {socket_path}.
    # that way, the last process wins.
    pid = os.getpid()
    spath = f"{socket_path}.{pid}"
    if os.path.exists(spath):
        os.unlink(spath)

    print(f"Opening socket at {socket_path=} with {timeout=}...", end='')
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock:
        sock.settimeout(timeout)
        sock.bind(spath)
        sock.listen()
        os.rename(spath, socket_path)
        print(' done.')
        while True:
            try:
                conn, _ = sock.accept()
            except socket.timeout:
                print("Server timeout. Exiting")
                exit(0)

            print(f"Connection accepted")
            pid = os.fork()
            if pid == 0:
                # Child process
                print(f"Forked child at {os.getpid()=}")
                sock.close()
                # read the command line (see client for protocol description)
                fd = conn.fileno()
                hdr = array.array('i')
                hdr.frombytes(_read_n(fd, 4))
                msglen = hdr[0]
                print(f"Message length is {msglen=}")
                msg = _read_n(fd, msglen).decode('utf-8')
                print(f"Raw message is {msg=}")
                # parse & reset our sys.argv
                sys.argv = msg.split('\0')
                print(f"{sys.argv=}")

                # Next are the pipes to communicate with the master
                msg, (stdin_r, stdout_w), _, _ = socket.recv_fds(conn, 10, maxfds=2)
                print(f"Received pipes {stdin_r=}, {stdout_w=}")

                # Dup them as our stdin/stdout/stderr
                os.dup2(stdin_r, STDIN)
                os.dup2(stdout_w, STDOUT)
#                os.dup2(stdout_w, STDERR)
#                print("Here!")
                os.close(stdin_r)
                os.close(stdout_w)
                print("Welcome to my echo chamber!")
                import time, tqdm
                for _ in tqdm.tqdm(range(100), file=sys.stdout):
                    time.sleep(0.1)
                for line in sys.stdin:
                    print("ECHO:", line, end='')
                    print("RECEIVED:", line, end='', file=sys.stderr)
                print("Exiting.", file=sys.stderr)
                exit(0)

                #conn.send(f"Hello from child at {os.getpid()=}\n".encode('utf-8'))
                # TODO: launch a dameon thread to receive messages over the control socket
                # This is how we'll receive screen resize mesages, etc., in the future.
                conn.close()

                print("Executing payload")
                exec(payload)

                exit(0)
            else:
                # Server
                conn.close()

def _read(fd):
    """Default read function."""
    return os.read(fd, 1024)

def _read_n(fd, length):
    """Read exactly length bytes"""
    b = os.read(fd, length)
    print(f"{b=}")
    while len(b) != length:
        b += os.read(fd, len(b)-length)
    return b

def _write(fd, data):
    """Write all the data to a descriptor."""
    while data:
        n = os.write(fd, data)
        data = data[n:]

#def _communicate(master_fd, saved_mask=set(), master_read=_read, stdin_read=_read):
def _communicate(in_src, in_dest, out_src, out_dest):
    import select

    fds = [in_src, out_src]
    args = [fds, [], []]
    while True:
        rfds = select.select(*args)[0]
#        print(f"{rfds=}")
        if not rfds:
            return
        for (src, dest) in ((in_src, in_dest), (out_src, out_dest)):
            if src in rfds:
                try:
                    data = _read(src)
                except OSError:
                    data = b""
                if not data:
                    fds.remove(src)
                    if src == in_src: # means STDIN has closed & we should terminate
                        args.append(0.5) # set timeout (but give it a chance for one final read)
                else:
                    os.write(dest, data)

#
# Handling broken pipe-related errors:
#   https://bugs.python.org/issue11380,#msg153320
#

def _connect(preload, payload, timeout=None):
    # try connecting to the UNIX socket. If successful, pass it our command
    # line (argv).  If connection is not successful, start the server.
#    if not os.path.exists(socket_path):
#        return _server(preload, payload, timeout)

    # try connecting
    client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    client.connect(socket_path)

    # send our command line
    args = '\0'.join(sys.argv)
    a = array.array('i')
    a.append(len(args))
    client.sendall(a.tobytes()) # header with the command line length
    client.sendall(args.encode('utf-8')) # the command line

    # now create and send the pipes over which we'll communicate
    # stdin, stdout, stderr
    stdin_r, stdin_w = os.pipe()
    stdout_r, stdout_w = os.pipe()
    socket.send_fds(client, [ b'pipes' ], [stdin_r, stdout_w])
#    os.close(stdin_w)
#    os.close(stdout_r)

    # Now enter the communication forwarding loop
#    os.write(stdin_w, b"Test, writing!\n")
    _communicate(STDIN, stdin_w, stdout_r, STDOUT)

def run(module, func):
    # check if we're already running
    _server()

if __name__ == "__main__":
    if os.environ.get("CLIENT", None):
        _connect("", "")
    else:
        _server("import dask.distributed", "print('Hello World!')")
