import socket, os, sys, array, struct, pickle, tty, fcntl, termios

# https://gist.github.com/jmhobbs/11276249
socket_path = os.path.join(os.environ['XDG_RUNTIME_DIR'], 'echo.socket')

STDIN=0
STDOUT=1
STDERR=2

# From http://computer-programming-forum.com/56-python/ef56832eb6f33ba3.htm
# h, w = struct.unpack("hhhh", fcntl.ioctl(0, termios.TIOCGWINSZ ,"\000"*8))[0:2]
#

def debug(*argv, **kwargs):
    kwargs['file'] = sys.stderr
    return print(*argv, **kwargs)

def _getwinsize(fd):
    # Get terminal window size for file descriptor fd
    h, w = struct.unpack("HHHH", fcntl.ioctl(fd, termios.TIOCGWINSZ ,"\000"*8))[0:2]
    return h, w

def _setwinsize(fd, winsz):
    # Set window size of tty with file descriptor fd
    # winsz is a (rows, cols) tuple
    rows, cols = winsz
    s = struct.pack('HHHH', rows, cols, 0, 0)
    res = fcntl.ioctl(fd, termios.TIOCSWINSZ, s)

    r, c = _getwinsize(fd)
    assert r == rows and c == cols

def _getmask():
    """Gets signal mask of current thread."""
    return signal.pthread_sigmask(signal.SIG_BLOCK, [])

def _sigblock():
    """Blocks all signals."""
    signal.pthread_sigmask(signal.SIG_BLOCK, ALL_SIGNALS)

def _sigreset(saved_mask):
    """Restores signal mask."""
    signal.pthread_sigmask(signal.SIG_SETMASK, saved_mask)

def _winchset(slave_fd, saved_mask, handle_winch):
    """Installs SIGWINCH handler. Returns old SIGWINCH
    handler if relevant; returns None otherwise."""
    bkh = None
    if handle_winch:
        def _hwinch(signum, frame):
            """SIGWINCH handler."""
            _sigblock()
            new_slave_fd = os.open(slave_path, os.O_RDWR)
            tty.setwinsize(new_slave_fd, tty.getwinsize(STDIN_FILENO))
            os.close(new_slave_fd)
            _sigreset(saved_mask)

        slave_path = os.ttyname(slave_fd)
        try:
            # Raises ValueError if not called from main thread.
            bkh = signal.signal(SIGWINCH, _hwinch)
        except ValueError:
            pass

    return bkh

def openpty(mode=None, winsz=None):
    """openpty() -> (master_fd, slave_fd)
    Open a pty master/slave pair, using os.openpty() if possible."""

    master_fd, slave_fd = os.openpty()
    debug(f"{master_fd=}, {slave_fd=}")

    if mode:
        termios.tcsetattr(slave_fd, tty.TCSAFLUSH, mode)
    if winsz:
        _setwinsize(slave_fd, winsz)

    return master_fd, slave_fd

def _login_tty(fd):
    """Prepare a terminal for a new login session.
    Makes the calling process a session leader; the tty of which
    fd is a file descriptor becomes the controlling tty, the stdin,
    the stdout, and the stderr of the calling process. Closes fd."""
    # Establish a new session.
    os.setsid()

    # The tty becomes the controlling terminal.
    try:
        fcntl.ioctl(fd, termios.TIOCSCTTY)
    except (NameError, OSError):
        # Fallback method; from Advanced Programming in the UNIX(R)
        # Environment, Third edition, 2013, Section 9.6 - Controlling Terminal:
        # "Systems derived from UNIX System V allocate the controlling
        # terminal for a session when the session leader opens the first
        # terminal device that is not already associated with a session, as
        # long as the call to open does not specify the O_NOCTTY flag."
        tmp_fd = os.open(os.ttyname(fd), os.O_RDWR)
        os.close(tmp_fd)

    # The tty becomes stdin/stdout/stderr.
    os.dup2(fd, STDIN)
    os.dup2(fd, STDOUT)
#    os.dup2(fd, STDERR)

    if fd != STDIN and fd != STDOUT and fd != STDERR:
        os.close(fd)

###########################################################################################

def _run_payload(payload):
    print("Welcome to my echo chamber!")
    import time, tqdm
    for _ in tqdm.tqdm(range(100), file=sys.stdout):
        time.sleep(0.1)
    for line in sys.stdin:
        print("ECHO:", line, end='')
        debug("RECEIVED:", line, end='')
    debug("Exiting.")
    exit(0)

    #conn.send(f"Hello from child at {os.getpid()=}\n".encode('utf-8'))
    # TODO: launch a dameon thread to receive messages over the control socket
    # This is how we'll receive screen resize mesages, etc., in the future.

    print("Executing payload")
    exec(payload)

    exit(0)

def _spawn(payload, conn, mode, winsz):
    #
    # Spawn a process to execute the Python code. The parent will
    # stay behind to receive and pass on SIGWINCH and other signals.
    #
    master_fd, slave_fd = openpty(mode, winsz)
    debug(f"Opened PTY {master_fd=} {slave_fd=}")

    pid = os.fork()
    if pid == 0:
        # child
        os.close(master_fd)

	# make us the session leader, and slave_fd our 
	# controlling terminal and stdin/out/err
        _login_tty(slave_fd)

        _run_payload(payload)

        exit(0)
    else:
        # parent (the control monitor)
        os.close(slave_fd)

	# shovel messages between our STDIN/STDOUT and the pty
        _communicate(STDIN, master_fd, master_fd, STDOUT)

#        # continue reading on the control socket, listening for any
#        # window size change and other signals to deliver.
#        fd = conn.makefile(encoding='utf-8')
#        for line in fd:
#            debug(f"CONTROL MSG: {line}")
#        debug("_spawn() exiting.")

        # TODO: Should we kill the child process here?

        os.close(master_fd)
        conn.close()

def _server(preload, payload, timeout=None):
    debug(f"Executing preload")
    exec(preload)
    debug("Done.")

    # handle the race condition where two programs are launched
    # at the same time, and try to create the same socket. We'll create
    # our socket in {socket_path}.{pid}, then mv to {socket_path}.
    # that way, the last process wins.
    pid = os.getpid()
    spath = f"{socket_path}.{pid}"
    if os.path.exists(spath):
        os.unlink(spath)

    debug(f"Opening socket at {socket_path=} with {timeout=}...", end='')
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock:
        sock.settimeout(timeout)
        sock.bind(spath)
        sock.listen()
        os.rename(spath, socket_path)
        debug(' done.')
        while True:
            try:
                conn, _ = sock.accept()
            except socket.timeout:
                debug("Server timeout. Exiting")
                exit(0)

            debug(f"Connection accepted")
            pid = os.fork()
            if pid == 0:
                # Child process
                debug(f"Forked child at {os.getpid()=}")
                sock.close()
                # read the command line (see client for protocol description)
                fd = conn.fileno()
                hdr = array.array('i')
                hdr.frombytes(_read_n(fd, 4))
                msglen = hdr[0]
                debug(f"Message length is {msglen=}")
                msg = _read_n(fd, msglen).decode('utf-8')
                debug(f"Raw message is {msg=}")
                # parse & reset our sys.argv
                sys.argv = msg.split('\0')
                debug(f"{sys.argv=}")

                # Next are the pipes to communicate with the master
                msg, (stdin_r, stdout_w), _, _ = socket.recv_fds(conn, 10, maxfds=2)
                debug(f"Received pipes {stdin_r=}, {stdout_w=}")

                # Dup them as our stdin/stdout/stderr
                os.dup2(stdin_r, STDIN)
                os.dup2(stdout_w, STDOUT)
#                os.dup2(stdout_w, STDERR)
#                debug("Here!")
                os.close(stdin_r)
                os.close(stdout_w)

                # Next is the window size info and tty attributes
                debug(f"Receiving window size and tty attributes:")
                mode, winsz = pickle.loads(_read_msg(fd))
                debug(f"{winsz=}")

                # Now we fork the process attached to a new pty
                _spawn(payload, conn, mode, winsz)
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
#    debug(f"{b=}")
    while len(b) != length:
        b += os.read(fd, len(b)-length)
    return b

def _read_msg(fd):
    msglen, = struct.unpack('I', _read_n(fd, 4))
#    debug(f"Message length is {msglen=}")
    msg = _read_n(fd, msglen)
#    debug(f"Raw message is {msg=}")
    return msg

def _send_msg(fd, data):
    # msg format: [length][payload]
    _write(fd, struct.pack('I', len(data)))
    _write(fd, data)

def _write(fd, data):
    """Write all the data to a descriptor."""
    while data:
        n = os.write(fd, data)
        data = data[n:]

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

    # Next is the local tty attributes and window size
    winsz = _getwinsize(STDOUT)
    mode = termios.tcgetattr(STDIN)
    _send_msg(client.fileno(), pickle.dumps((mode, winsz)))

    # switch input to raw mode
    # See here for _very_ useful info:
    #   https://stackoverflow.com/questions/51509348/python-tty-setraw-ctrl-c-doesnt-work-getch
    tty.setraw(STDIN)

    # Now enter the communication forwarding loop
#    os.write(stdin_w, b"Test, writing!\n")
    try:
        _communicate(STDIN, stdin_w, stdout_r, STDOUT)
    finally:
        termios.tcsetattr(STDIN, tty.TCSAFLUSH, mode)
#    if bkh:
#        signal.signal(SIGWINCH, bkh)

def run(module, func):
    # check if we're already running
    _server()

if __name__ == "__main__":
    if os.environ.get("CLIENT", None):
        _connect("", "")
    else:
        _server("import dask.distributed", "print('Hello World!')")
