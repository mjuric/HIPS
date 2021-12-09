# FIXME:
#  * Do we need to block/unblock signals in signal handlers?
#  * Full and partial redirect handling

import socket, os, sys, array, struct, pickle, tty, fcntl, termios, select, signal

# https://gist.github.com/jmhobbs/11276249
socket_path = os.path.join(os.environ['XDG_RUNTIME_DIR'], 'echo.socket')

STDIN  = STDIN_FILENO  = 0
STDOUT = STDOUT_FILENO = 1
STDERR = STDERR_FILENO = 2

def debug(*argv, **kwargs):
    kwargs['file'] = sys.stderr
    return print(*argv, **kwargs)


#
# Pty management routines
#

# From http://computer-programming-forum.com/56-python/ef56832eb6f33ba3.htm
# h, w = struct.unpack("hhhh", fcntl.ioctl(0, termios.TIOCGWINSZ ,"\000"*8))[0:2]
#

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

def newpty(termios_attr, winsz):
    #
    # Open a new pty with the given window size and termios spec
    #

    master_fd, slave_fd = os.openpty()
    debug(f"{master_fd=}, {slave_fd=}")

    termios.tcsetattr(slave_fd, termios.TCSAFLUSH, termios_attr)
    _setwinsize(slave_fd, winsz)

    debug(f"{os.ttyname(slave_fd)=}")
    return master_fd, slave_fd

def _login_tty(fd):
    """Prepare a terminal for a new login session.
    Makes the calling process a session leader; the tty of which
    fd is a file descriptor becomes the controlling tty, the stdin,
    the stdout, and the stderr of the calling process."""
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

###########################################################################################

def _run_payload(payload):
    print("Welcome to my echo chamber!")
#    os.execl("/astro/users/mjuric/lfs/bin/joe", "joe")
#    os.execl("/usr/bin/vim", "vim")
    os.execl("/usr/bin/sleep", "sleep", "600")
#    os.execl("/usr/bin/stty", "stty", "-a")

#    import time, tqdm
#    for _ in tqdm.tqdm(range(100), file=sys.stdout):
#        time.sleep(0.1)
#        debug("#", end='', flush=True)
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

def _spawn(payload, remote_pid, conn, termios_attr, winsz):
    #
    # Spawn a child to execute the payload. The parent will
    # stay behind to pass on SIGSTOP and the exit code
    #

    # Open a new PTY and send it back to our ccontroller process
    master_fd, slave_fd = newpty(termios_attr, winsz)
#    debug(f"Opened PTY {master_fd=} {slave_fd=}")

    # send back the master_fd
    socket.send_fds(conn, [ b'm' ], [master_fd])
#    debug(f"Sent {master_fd=}")

    # make us the session leader, and make slave_fd our 
    # controlling terminal and dup it to stdin/out/err
    _login_tty(slave_fd)
    os.close(slave_fd) # as it's been duped to STDIN/OUT/ERR by _login_tty
#    debug(f"{os.tcgetpgrp(0)=} {os.getpid()=}")

    # the parent will set up the child's process group and terminal.
    # while that's going on, the child should wait and not execute
    # the payload. We do this by having the child wait to receive
    # a message via a pipe.
    r, w = os.pipe()
    
#    debug(f"PIPE: {r=}, {w=}")
    # now fork the payload process
    pid = os.fork()
    if pid == 0:
#        debug(f"CHILD: {os.getpid()=}")
        os.close(w)
        os.close(master_fd)
        conn.close()

        # wait until the parent sets us up
#        debug("CHILD: waiting for parent setup")
        while not len(os.read(r, 1)):
            pass
#        debug("CHILD: unblocked!")
        os.close(r)

        # run the payload
        _run_payload(payload)

#        debug("CHILD: exiting!")
        exit(0)
    else:
##        debug(f"PARENT: {os.getpid()=} {r=} {w=}")
        os.close(r)

        # start a new process group
#        debug("setpgid")
        os.setpgid(pid, pid)

        # set child's process group as the foreground group
#        debug("os.tcsetpgrp")
        os.tcsetpgrp(STDIN, pid)

        # now that we're all set up, sent the 
        # child PID back to the client
        _send_object(conn.fileno(), pid)

        # unblock the child by writing, then closing the pipe
        os.write(w, b"x")
        os.close(w)

        # Loop here calling waitpid(-1, 0, WUNTRACED) to handle
        # the child's SIGSTOP (by SIGSTOP-ing the remote_pid) and death (just exit)
        # Really good explanation: https://stackoverflow.com/a/34845669
        # FIXME: We should handle the case where remote_pid is killed, by
        #        periodically timing out and checking if conn is still open...
        #        Or, we could move all this into a SIGCHLD handler, and
        #        constantly listen on conn?
        #        Actually, we should do this: https://docs.python.org/3/library/signal.html#signal.set_wakeup_fd
        while True:
            debug(f"SENTRY: waitpid on {pid=}")
            _, status = os.waitpid(pid, os.WUNTRACED | os.WCONTINUED)
            # stopped?
            debug(f"SENTRY: waitpid returned {status=}")
            if os.WIFSTOPPED(status):
                # make the controller's process group go to sleep
                # why not just the controller? Because it may have been invoked
                # by something like `time foo ...` and in fact runs in a process
                # group
#                debug(f"SENTRY: WIFSTOPPED=True, sending SIGTSTP to pgid={os.getpgid(remote_pid)}")
#                os.killpg(os.getpgid(remote_pid), signal.SIGTSTP)
                _send_object(conn.fileno(), ("stopped", 0))
            elif os.WIFEXITED(status):
                # we've exited. return the status back to the controller
                _send_object(conn.fileno(), ("exited", os.WEXITSTATUS(status)))
                break
            elif os.WIFSIGNALED(status):
                # we've exited. return the status back to the controller
                _send_object(conn.fileno(), ("signaled", os.WTERMSIG(status)))
                break
            else:
                assert 0, f"weird {status=}"

#        debug(f"SENTRY: Closing pty, sockets, and leaving.")
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
                fd = conn.fileno()

                # get command line
                sys.argv = _read_object(fd)
                debug(f"{sys.argv=}")

                remote_pid = _read_object(fd)
                debug(f"{remote_pid=}")

                # Next is the window size info and tty attributes
                debug(f"Receiving window size and tty attributes:")
                termios_attr, winsz = _read_object(fd)
                debug(f"{winsz=}")

                # Now we fork the process attached to a new pty
                _spawn(payload, remote_pid, conn, termios_attr, winsz)
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

def _read_object(fd):
    return pickle.loads(_read_msg(fd))

def _read_msg(fd):
    msglen, = struct.unpack('I', _read_n(fd, 4))
#    debug(f"Message length is {msglen=}")
    msg = _read_n(fd, msglen)
#    debug(f"Raw message is {msg=}")
    return msg

def _send_object(fd, obj):
    return _send_msg(fd, pickle.dumps(obj, -1))

def _send_msg(fd, data):
    # msg format: [length][payload]
    _writen(fd, struct.pack('I', len(data)))
    _writen(fd, data)

def _writen(fd, data):
    """Write all the data to a descriptor."""
    while data:
        n = os.write(fd, data)
        data = data[n:]

def _copy(master_fd, control_fd, termios_attr, remote_pid):
    """Parent copy loop.
    Copies
            pty master -> standard output   (master_read)
            standard input -> pty master    (stdin_read)"""
    fds = [master_fd, STDIN_FILENO, control_fd]
    while fds:
        rfds, _wfds, _xfds = select.select(fds, [], [])
#        debug(f"{rfds=}")

        if master_fd in rfds:
            # Some OSes signal EOF by returning an empty byte string,
            # some throw OSErrors.
            try:
                data = _read(master_fd)
            except OSError:
                data = b""
            if not data:  # Reached EOF.
                debug("CLIENT: zero read on master_fd")
                fds.remove(master_fd)
#                return    # Assume the child process has exited and is
#                          # unreachable, so we clean up.
#                          # FIXME: the process can close stdout and still continue. we should start checking pid if this is closed...
            else:
                os.write(STDOUT_FILENO, data)

        if STDIN_FILENO in rfds:
            data = _read(STDIN_FILENO)
            if not data:
                fds.remove(STDIN_FILENO)
            else:
                _writen(master_fd, data)

        if control_fd in rfds:
            # a control message from the worker. they've
            # paused, exited, etc.
            event, data = _read_object(control_fd)
            debug(f"CLIENT: received {event=}")
            if event == "stopped":
                # it's possible we've been backrounded by the time we got here,
                # so ignore SIGTTOU while mode-setting. This can happen if someone sent
                # us (the client) an explicit SIGTSTP.
                signal.signal(signal.SIGTTOU, signal.SIG_IGN)
                termios.tcsetattr(STDIN, tty.TCSAFLUSH, termios_attr)	# restore tty
                signal.signal(signal.SIGTTOU, signal.SIG_DFL)
                debug("CLIENT: Putting us to sleep")
                os.kill(os.getpid(), signal.SIGSTOP)			# put ourselves to sleep

                # this is where we sleep....
                # ... and continue when we're awoken by SIGCONT (e.g., 'fg' in the shell)

                debug("CLIENT: Awake again")
                tty.setraw(STDIN)					# turn the STDIN raw again

                # set terminal size (in case it changed while we slept)
                s = fcntl.ioctl(STDOUT, termios.TIOCGWINSZ, '\0'*8)
                fcntl.ioctl(master_fd, termios.TIOCSWINSZ, s)

                os.killpg(os.getpgid(remote_pid), signal.SIGCONT)	# wake up the worker process
            elif event == "exited":
                return data # data is the exitstatus
            elif event == "signaled":
#                return -1
                signum = data  # data is the signal that terminated the worker
                termios.tcsetattr(STDIN, tty.TCSAFLUSH, termios_attr)	# restore tty back from the raw mode
                # then restore its default handler and commit a copycat suicide
                signal.signal(signum, signal.SIG_DFL)
                os.kill(os.getpid(), signum)
            else:
                assert 0, "unknown control event {event}"

#
# Handling broken pipe-related errors:
#   https://bugs.python.org/issue11380,#msg153320
#

# modified from https://github.com/python/cpython/blob/3.10/Lib/tty.py#L18
def setraw_except_ISIG(fd, when=tty.TCSAFLUSH):
    """Put terminal into a raw mode."""
    mode = tty.tcgetattr(fd)
    mode[tty.IFLAG] = mode[tty.IFLAG] & ~(tty.BRKINT | tty.ICRNL | tty.INPCK | tty.ISTRIP | tty.IXON)
    mode[tty.OFLAG] = mode[tty.OFLAG] & ~(tty.OPOST)
    mode[tty.CFLAG] = mode[tty.CFLAG] & ~(tty.CSIZE | tty.PARENB)
    mode[tty.CFLAG] = mode[tty.CFLAG] | tty.CS8
    mode[tty.LFLAG] = mode[tty.LFLAG] & ~(tty.ECHO | tty.ICANON | tty.IEXTEN)
    mode[tty.CC][tty.VMIN] = 1
    mode[tty.CC][tty.VTIME] = 0
    tty.tcsetattr(fd, when, mode)

# we need to catch & pass on:
# INTR, QUIT, SUSP, or DSUSP ==> SIGINT, SIGQUIT, SIGTSTP, SIGTERM
# and then we need SIGCONT for recovery
# See: https://www.gnu.org/software/libc/manual/html_node/Signal-Characters.html
# See: https://www.gnu.org/software/libc/manual/html_node/Termination-Signals.html

#def _setup_cont(remote_pid):
#    def _handle_cont(signum, frame):
#        debug(f"_handle_cont: Received {signum=} [{remote_pid=}]")
#        tty.setraw(STDIN)
#        os.killpg(os.getpgid(remote_pid), signal.SIGCONT)
#
#    signal.signal(signal.SIGCONT, _handle_cont)

def _setup_signal_passthrough(remote_pid):
    def _handle_ISIG(signum, frame):
        debug(f"**** ISIG caught, {signum=}")

        # pass on the signal to the remote process
        os.killpg(os.getpgid(remote_pid), signum)

        debug(f"**** Forwarded {signum=}")

        # if the remote process handles the signal by suspending
        # or terminating itself, we'll be told about it via
        # the control socket (and can do the same).

    # forward all signals that make sense to forward
    fwd_signals = set(signal.Signals) - {signal.SIGKILL, signal.SIGSTOP, signal.SIGCHLD, signal.SIGWINCH}
    for signum in fwd_signals:
        signal.signal(signum, _handle_ISIG)

def _setup_winch(master_fd):
    """Sets up SIGWINCH handler which:
      * gets the new size from our terminal
      * sets the same size on PTY (which will trigger a SIGWINCH)
        on the remote process.
    """
    def _copy_ws(signum, frame):
        """SIGWINCH handler."""
        s = fcntl.ioctl(STDOUT, termios.TIOCGWINSZ, '\0'*8)
        #debug(f"CLIENT: _copy_ws:", struct.unpack("HHHH", s))
        fcntl.ioctl(master_fd, termios.TIOCSWINSZ, s)
        #debug("CLIENT: exiting _copy_ws")

    return signal.signal(signal.SIGWINCH, _copy_ws)

#
# Really useful explanation of how SIGTSTP SIGSTOP CTRL-Z work:
#   https://news.ycombinator.com/item?id=8773740
#
def _connect(preload, payload, timeout=None):
    # try connecting to the UNIX socket. If successful, pass it our command
    # line (argv).  If connection is not successful, start the server.
#    if not os.path.exists(socket_path):
#        return _server(preload, payload, timeout)

    # try connecting
    client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    client.connect(socket_path)
    fd = client.fileno()

    # send our command line
    _send_object(fd, sys.argv)

    # send our PID. The remote process will be controlling us
    # (e.g., for things like CTRL-Z handling)
    _send_object(fd, os.getpid())

    # Next is the local tty attributes and window size
    winsz = _getwinsize(STDOUT)
    termios_attr = termios.tcgetattr(STDIN)
    _send_object(fd, (termios_attr, winsz))

    # get the master_fd of the pty we'll be writing to
    _, (master_fd,), _, _ = socket.recv_fds(client, 10, maxfds=1)
    debug(f"Received {master_fd=}")

    # get the pty device (for window size management)

    # get the PID
    remote_pid = _read_object(fd)
    print(f"{remote_pid=}")

    # set up SIGTSTP/SIGCONT handlers to stop/restart the remote process
#    _setup_cont(remote_pid)
    _setup_signal_passthrough(remote_pid)

    # set up the SIGWINCH handler
    _setup_winch(master_fd)

    # switch our input to raw mode
    # See here for _very_ useful info about raw mode:
    #   https://stackoverflow.com/questions/51509348/python-tty-setraw-ctrl-c-doesnt-work-getch
#    setraw_except_ISIG(STDIN)
    tty.setraw(STDIN)

    # Now enter the communication forwarding loop
    try:
        retcode = _copy(master_fd, fd, termios_attr, remote_pid)
    finally:
        # restore our console
        termios.tcsetattr(STDIN, tty.TCSAFLUSH, termios_attr)
#    debug(f"CLIENT EXITING, awaiting retcode")

#    retcode = _read_object(fd)
    debug(f"CLIENT: exited with {retcode=}")

    exit(retcode)

def run(module, func):
    # check if we're already running
    _server()

if __name__ == "__main__":
    if os.environ.get("CLIENT", None):
        _connect("", "")
    else:
        _server("import dask.distributed", "print('Hello World!')")
