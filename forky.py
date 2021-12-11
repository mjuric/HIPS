# FIXME:
#  * Do we need to block/unblock signals in signal handlers?
#  * Full and partial redirect handling

import socket, os, sys, array, struct, marshal, tty, fcntl, termios, select, signal

# https://gist.github.com/jmhobbs/11276249
socket_path = os.path.join(os.environ['XDG_RUNTIME_DIR'], 'echo.socket')

STDIN  = STDIN_FILENO  = 0
STDOUT = STDOUT_FILENO = 1
STDERR = STDERR_FILENO = 2

def debug(*argv, **kwargs):
    kwargs['file'] = sys.stderr
    return print(*argv, **kwargs)


#############################################################################
#
#   Pty management routines
#
#############################################################################

def _getwinsize(fd):
    # Return the terminal window size struct for file descriptor fd
    # the contents are a struct w. "HHHH" signature, mapping to
    # (rows, cols, ws_xpixel, ws_ypixel), but we don't bother
    # to unpack as this will be bed back to _setwinsize.
    return fcntl.ioctl(fd, termios.TIOCGWINSZ ,"\000"*8)

def _setwinsize(fd, winsz):
    # Set window size of tty with file descriptor fd
    # winsz is a struct with "HHHH" signature, as returned by _getwinsize
    return fcntl.ioctl(fd, termios.TIOCSWINSZ, winsz)

#############################################################################

def _run_payload(payload):
    print("Welcome to my echo chamber!")
    os.execl("/astro/users/mjuric/lfs/bin/joe", "joe")
#    os.execl("/usr/bin/vim", "vim")
#    os.execl("/usr/bin/sleep", "sleep", "600")
#    os.execl("/usr/bin/stty", "stty", "-a")

    import time, tqdm
    for _ in tqdm.tqdm(range(100), file=sys.stdout):
        time.sleep(0.1)
        debug("#", end='', flush=True)
    for line in sys.stdin:
        print("ECHO:", line, end='')
        debug("RECEIVED:", line, end='')
    debug("Exiting.")
    exit(0)

    #conn.send(f"Hello from child at {os.getpid()=}\n".encode('utf-8'))
    # TODO: launch a dameon thread to receive messages over the control socket
    # This is how we'll receive screen resize mesages, etc., in the future.

#    print("Executing payload")
    exec(payload)

    exit(0)

def _spawn(payload, remote_pid, conn, fp):
    #
    # Spawn a child to execute the payload. The parent will
    # stay behind to communicate the child's status to the client.
    #

    # Open a new PTY and send it back to our ccontroller process
    master_fd, slave_fd = os.openpty()

    # send back the master_fd, wait for master to set it up and
    # acknowledge.
    socket.send_fds(conn, [ b'm' ], [master_fd])
    ok = _read_object(fp)
    assert ok == "OK"
    os.close(master_fd) # master_fd is with the client now, so we can close it

    # make us the session leader, and make slave_fd our 
    # controlling terminal and dup it to stdin/out/err
    os.setsid()
    fcntl.ioctl(slave_fd, termios.TIOCSCTTY)
    os.dup2(slave_fd, STDIN)
    os.dup2(slave_fd, STDOUT)
#    os.dup2(slave_fd, STDERR)
    os.close(slave_fd)	# slave_fd has been duped to STDIN/OUT/ERR, so we can close it

    # the parent will set up the child's process group and terminal.
    # while that's going on, the child should wait and not execute
    # the payload. We do this by having the child wait to receive
    # a message via a pipe.
    r, w = os.pipe()
    
    # now fork the payload process
    pid = os.fork()
    if pid == 0:
        os.close(w)			# we'll only be reading
        conn.close()			# we won't be directly communicating to the client

        # wait until the parent sets us up
        while not len(os.read(r, 1)):
            pass
        os.close(r)

        # run the payload
        return _run_payload(payload)
    else:
        os.close(r)			# we'll only be writing
        
        os.setpgid(pid, pid)		# start a new process group for the child
        os.tcsetpgrp(STDIN, pid)	# make the child's the foreground process group (so it receives tty input+signals)

        _write_object(fp, pid)		# send the child's PID back to the client

        os.write(w, b"x")		# unblock the child, close the pipe
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
            ##debug(f"SENTRY: waitpid on {pid=}")
            _, status = os.waitpid(pid, os.WUNTRACED | os.WCONTINUED)
            ##debug(f"SENTRY: waitpid returned {status=}")
            ##debug(f"SENTRY: {os.WIFSTOPPED(status)=} {os.WIFEXITED(status)=} {os.WIFSIGNALED(status)=} {os.WIFCONTINUED(status)=}")
            if os.WIFSTOPPED(status):
                # let the controller know we've stopped
                _write_object(fp, ("stopped", 0))
            elif os.WIFEXITED(status):
                # we've exited. return the status back to the controller
                _write_object(fp, ("exited", os.WEXITSTATUS(status)))
                break
            elif os.WIFSIGNALED(status):
                # we've exited. return the status back to the controller
                _write_object(fp, ("signaled", os.WTERMSIG(status)))
                break
            elif os.WIFCONTINUED(status):
                # we've been continued after being stopped
                # TODO: should we make sure the remote_pid is signaled to CONT?
                pass
            else:
                assert 0, f"weird {status=}"

        # the child has exited; clean up and leave
        conn.close()

def _server(preload, payload, timeout=None):
    # Execute preload code
    exec(preload)

    # avoid the race condition where two programs are launched
    # at the same time, and try to create the same socket. We'll create
    # our socket in {socket_path}.{pid}, then mv to {socket_path}.
    # that way, the last process wins.
    pid = os.getpid()
    spath = f"{socket_path}.{pid}"
    if os.path.exists(spath):
        os.unlink(spath)

#    debug(f"Opening socket at {socket_path=} with {timeout=}...", end='')
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock:
        sock.settimeout(timeout)
        sock.bind(spath)
        sock.listen()
        os.rename(spath, socket_path)
#        debug(' done.')

        # Await for client connections
        while True:
            try:
                conn, _ = sock.accept()
            except socket.timeout:
                debug("Server timeout. Exiting")
                exit(0)

#            debug(f"Connection accepted")
            # connection accepted. Fork a child process to handle the work.
            pid = os.fork()
            if pid == 0:
                # Child process
                #debug(f"Forked child at {os.getpid()=}")
                sock.close()
                fp = conn.makefile(mode='rwb', buffering=0)

                # receive the command line
                sys.argv = _read_object(fp)
#                debug(f"{sys.argv=}")

                # receive the client PID (FIXME: we don't really use this)
                remote_pid = _read_object(fp)
#                debug(f"{remote_pid=}")

                # Now we fork the process attached to a new pty
                return _spawn(payload, remote_pid, conn, fp)
            else:
                conn.close()

#############################################################################
#
#   Control socket communication routines
#
#############################################################################

def _read_object(fp):
    """ Read a marshaled object from file fp """
    return marshal.load(fp)

def _write_object(fp, obj):
    """ Write an object to file fp, using marshal """
    return marshal.dump(obj, fp)

def _writen(fd, data):
    """Write all the data to a descriptor."""
    while data:
        n = os.write(fd, data)
        data = data[n:]

def _copy(master_fd, control_fd, control_fp, termios_attr, remote_pid):
    """Copy and control loop
    Copies
            pty master -> standard output   (master_read)
            standard input -> pty master    (stdin_read)
    and also listens for control messages from the child
    on control_fd/fp.
    """
    fds = [master_fd, STDIN_FILENO, control_fd]
    while fds:
        rfds, _wfds, _xfds = select.select(fds, [], [])
#        debug(f"{rfds=}")

        if master_fd in rfds:
            # Some OSes signal EOF by returning an empty byte string,
            # some throw OSErrors.
            try:
                data = os.read(master_fd, 1024)
            except OSError:
                data = b""
            if not data:  # Reached EOF.
#                debug("CLIENT: zero read on master_fd")
                fds.remove(master_fd)
            else:
                os.write(STDOUT_FILENO, data)

        if STDIN_FILENO in rfds:
            data = os.read(STDIN_FILENO, 1024)
            if not data:
                fds.remove(STDIN_FILENO)
            else:
                _writen(master_fd, data)

        if control_fd in rfds:
            # a control message from the worker. they've
            # paused, exited, etc.
            event, data = _read_object(control_fp)
#            debug(f"CLIENT: received {event=}")
            if event == "stopped":
                # it's possible we've been backrounded by the time we got here,
                # so ignore SIGTTOU while mode-setting. This can happen if someone sent
                # us (the client) an explicit SIGTSTP.
                signal.signal(signal.SIGTTOU, signal.SIG_IGN)
                termios.tcsetattr(STDIN, tty.TCSAFLUSH, termios_attr)	# restore tty
                signal.signal(signal.SIGTTOU, signal.SIG_DFL)
#                debug("CLIENT: Putting us to sleep")
#                os.kill(os.getpid(), signal.SIGSTOP)			# put ourselves to sleep
                os.kill(0, signal.SIGSTOP)			# put ourselves to sleep

                # this is where we sleep....
                # ... and continue when we're awoken by SIGCONT (e.g., 'fg' in the shell)

#                debug("CLIENT: Awake again")
                tty.setraw(STDIN)					# turn the STDIN raw again

                # set terminal size (in case it changed while we slept)
                s = fcntl.ioctl(STDOUT, termios.TIOCGWINSZ, '\0'*8)
                fcntl.ioctl(master_fd, termios.TIOCSWINSZ, s)

                # FIXME: we should message the nanny to do this (pid race condition!)
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

# we need to catch & pass on:
# INTR, QUIT, SUSP, or DSUSP ==> SIGINT, SIGQUIT, SIGTSTP, SIGTERM
# and then we need SIGCONT for recovery
# See: https://www.gnu.org/software/libc/manual/html_node/Signal-Characters.html
# See: https://www.gnu.org/software/libc/manual/html_node/Termination-Signals.html

def _setup_signal_passthrough(remote_pid):
    def _handle_ISIG(signum, frame):
        # just pass on the signal to the remote process
        #
        # if the remote process handles the signal by suspending
        # or terminating itself, we'll be told about it via
        # the control socket (and can do the same).
        #
        # FIXME: this signaling should be done through the control socket (pid race contitions!)
        os.killpg(os.getpgid(remote_pid), signum)

    # forward all signals that make sense to forward
    fwd_signals = set(signal.Signals) - {signal.SIGKILL, signal.SIGSTOP, signal.SIGCHLD, signal.SIGWINCH}
    for signum in fwd_signals:
        signal.signal(signum, _handle_ISIG)

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
    fp = client.makefile(mode='rwb', buffering=0)

    # send our command line
    _write_object(fp, sys.argv)

    # send our PID (FIXME: is this necessary?)
    _write_object(fp, os.getpid())

    # get the master_fd of the pty we'll be writing to
    # set up the initial mode and window size
    _, (master_fd,), _, _ = socket.recv_fds(client, 10, maxfds=1)
    termios_attr = termios.tcgetattr(STDIN)
    termios.tcsetattr(master_fd, termios.TCSAFLUSH, termios_attr)
    _setwinsize(master_fd, _getwinsize(STDOUT))
    _write_object(fp, "OK")

    # get the PID
    remote_pid = _read_object(fp)

    # pass any signals we receive back to the worker
    _setup_signal_passthrough(remote_pid)

    # set up the SIGWINCH handler which copies terminal window changes
    # to the pty
    signal.signal(
        signal.SIGWINCH,
        lambda signum, frame: _setwinsize(master_fd, _getwinsize(STDOUT))
    )

    try:
        # switch our input to raw mode
        # See here for _very_ useful info about raw mode:
        #   https://stackoverflow.com/questions/51509348/python-tty-setraw-ctrl-c-doesnt-work-getch
        tty.setraw(STDIN)

        # Now enter the communication forwarding loop
        exitcode = _copy(master_fd, client.fileno(), fp, termios_attr, remote_pid)
    finally:
        # restore our console
        termios.tcsetattr(STDIN, tty.TCSAFLUSH, termios_attr)

    return exitcode

def run(module, func):
    # check if we're already running
    _server()

if __name__ == "__main__":
    if os.environ.get("CLIENT", None):
        ret = _connect("", "")
    else:
        ret = _server("import dask.distributed", "print('Hello World!')")

    exit(ret)
