# FIXME:
#  * Do we need to block/unblock signals in signal handlers?
#  * Sane default for systems w/o XDG_RUNTIME_DIR (macOS)
#  * Proper logging
#  * Tests
#
# # Instastart: Simple, fast, robust prewarming of Python processes for fast startup
#
#  In brief: for a command-line tool that uses it, this module (on first
#  invocation) transparently creates a server process which completes the
#  "heavy" initialization phases (i.e., imports of large modules) and awaits
#  to do actual work.  Subsequent invocations just fork() from this server
#  process, skipping costly re-initializations.  This can cut down
#  startup times from 1-10 seconds to ~70 milliseconds, making the command
#  line code pleasent to use (and fast to use in shell scripts).
#
# ## Quick start
#
#  $ cat slow.py
#  import dask.distributed
#  import astropy
#    
#  if __name__ == "__main__":
#      print("Hello world!")
#
#  $ time python slow.py
#  Hello world!
#
#  real    0m2.087s
#  user    0m2.607s
#  sys     0m11.124s
#
#  $ cat fast.py
#  from instastart.auto import start, done
#  import dask.distributed
#  import astropy
#  
#  if __name__ == "__main__":
#      start()
#      print("Hello world!")
#      done()
#
#  $ time python fast.py
#  Hello world!
#  
#  real    0m0.098s
#  user    0m0.063s
#  sys     0m0.014s
#
# ## Implementation
#
#  Two core ideas are at the center of this package:
#
#    1. An observation that the slow startup time of many Python codes is
#       due to costly, one-time, unchanging, initialization -- specifically
#       imports of large modules.  If there was a way to do this
#       initialization just once, then (quickly) copy the state of the
#       initialized interpreter for subsequent runs, we'd dramatically cut
#       down on the total startup time.
#
#    2. The fact that UNIX systems provide a way to do something like the
#       above, using the fork(2) system call (e.g., see the man page at
#       https://man7.org/linux/man-pages/man2/fork.2.html).  fork() allows
#       one to duplicate (aka "fork") a running process (program).  If we
#       could find a clever way to pause a Python program after it's
#       finished the (costly) initialization, and then fork() from that
#       state the next time it's invoked, we could achieve our goal of
#       cutting down the startup times.
#
#  This techique is sometimes known as "pre-warming". An ideal
#  implementation would be one where a pre-warmed server process awaits in
#  the background.  When a new invocation is launched, it forks the
#  pre-warmed server and replaces itself w.  the forked child process (while
#  maintaining the process ID -- the pid, etc.).  This way the result would
#  seem nearly 100% identical to non-prewarmed runs, from the point of view
#  of the shell and other programs.  It would also work if one could
#  "reparent" the server's child to the subsequent invocation (and bind it
#  to its own terminal).  Alas, neither of those options are allowed on
#  typical UNIX systems (and for good reasons).
#
#  So we have to take a different tack: have the new invocation (which we'll
#  -- call the "client") act as a /transparent proxy/ shutling input,
#  output, and signals between the user and the "worker", the process forked
#  from the server that does the actual work.  This is what instastart does. 
#  Below we discuss how.
#
# ## Gory details
#
#  Figure 1. A diagram of a typical execution (first run):
#
#    <<client>>
#        |
#        | ---> contact server on UNIX socket...fail (doesn't exist)
#        |
#        | -----------> fork <<server>>
#        |                       |
#        | ---> "cmd=run" -----> |
#        |                       | ---> fork <<sentry>>
#        |                       :               | (alloc pty)
#        | ---> argv + cwd + environ + fds ----> |
#        |                       :               |  ---> fork <<worker>>
#        |                       :               :                |
#        | ------------- stdin/signals/pty control -------------> |
#        | <------------------- stdout/stderr ------------------- |
#        |                       :               :
#        | <--------- waitpid() results -------- |
#                                :
#                                o exit after idle timeout
#
#  * server: the pre-warmed process that stick around in the background,
#            ready to fork a child to do the work
#  * worker: the process doing the actual, useful, work. I.e., the thing
#            that would be done w/o instastart.
#  * sentry: the process that watches over the worker, receives and
#            forwards the notifications that the worker has stopped or
#            has exited. A session leader for the worker's pty.
#            Plays the role of a quasi-shell process for the worker.
#  * client: the process the user launches, which has the server fork a
#            child doing the real work and proxies keyboard/signals back to
#            the child
#
#  The sequence begins by the user launching a client on the command line. 
#  Usually the very first thing the client does it import
#  instastart.auto, i.e.:
#
#     import instastart.auto
#
#  This module contains the code that makes the client attempt to connect to
#  the server via a named UNIX socket stored on a well-known path (usually
#  in $XDG_RUNTIME_DIR).  The name of the socket is unique to the
#  combination of the path of the executable script, the python interpreter,
#  and the contents of PYTHONPATH.  If no such socket exists, or if the
#  connection is unsuccessful, instastart.auto forks a server process.
#
#  This server process then returns from the import statement, and continues
#  running the client's code, presumably some heavy imports and one-time
#  computations.  It does so until it reaches an invocation of
#  instastart.auto.start(). Typically:
#
#     import instastart.auto
#
#       ... heavy imports, dask.distributed, vaex, astropy etc ...
#
#     if __name__ == "__main__":
#         instastart.auto.start()
#         ... code to run ...
#
#  This function serves as a barrier -- at this point, the server binds to
#  the named UNIX socket and begins listening for connections, effectivelly
#  pausing execution.  The big idea is that a subsequent client connection
#  will cause the server to fork a worker and simply return from
#  instastart.auto.start(), continuing to run (from the users' code
#  perspective) as if nothing happened.  Through the magic of fork() and
#  UNIX' copy-on-write semantics, this can be done many times -- each time
#  effectivelly skipping all the costly initialization and starting to run
#  from the line where we invoked instastart.auto.start() (thus the name of
#  that function).  I.e., we've effectivelly "pre-warmed" Python VM w.  plus
#  our modules of interest.  The implementation details are a bit more
#  complicated, as discussed next.
#
#  With the server now forked and awaiting connections, the client can
#  connect to it and request to continue running (possibly in a different
#  directory, or w.  different cmd args, or environment; more below).  The
#  server reacts by first forking a sentry process.  This process will serve
#  as a quasi-shell for the actual worker process (to be forked next, by the
#  sentry).  It is also necessary to receive signals about worker's state as
#  well as manage it's pseudo-terminal (pty).  Once the sentry is forked,
#  the client sends it the specifics of how to continue running -- at least
#  the argv, cwd, and environ.  These will be modified once the worker is
#  forked to match the client's environment (i.e., to make the worker's
#  internal state as close to what it would look like if it was launched
#  under in the client's location/environment).
#
#  If any of the client's stdin/out/err are not connected to the terminal
#  (i.e., I/O was redirected to a file or piped), the client sends those
#  file descriptors over as well.  They will be dup2-ed directly to the
#  worker's stdin/out/err (see below), making for truly zero-overhead IO. 
#  If at least one of the client's stdin/out/err are connected to a tty, we
#  need to make the worker believe it's connected to the (analogous)
#  terminal (otherwise fancy progress bars and other UI elements -- curses,
#  mouse control, etc -- won't work).  To do so, the sentry allocates a pty
#  & becomes its session leader.  It then sends back the pty master file
#  descriptor (master_fd) to the client, who copies the properties of the
#  local terminal to it (most importantly, the window size).  That way the
#  worker process will "feel" as if it's running on exactly the same tty as
#  the client.  After the client sets up the pty, the sentry finally forks
#  the worker, moves it into its own process group, sets it as the pty's
#  foreground process, and sends the worker pid back to the client.
#
#  This worker process is finally allowed to return from
#  instastart.auto.start() and continue executing useful code -- from the
#  point of view of Python code, instastart.auto.start() just took a while
#  to return.  The worker's I/O is now connected to the pty which the client
#  controls (or to the duplicated file descriptors, if some std* streams
#  were redirected).  The client communicates with the worker by polling
#  master_fd (the worker's pty master end that the sentry sent back) for any
#  output, and its own STDIN for any input.  The client reads the output
#  from master_fd and writes it to its own terminal.  Similarly, the client
#  reads input from its own terminal and writes it to master_fd.  While not
#  zero, these are fairly low-overhead operations (and no worse than what
#  your typical SSH, tmux, or screen client does).  The sentry stays on,
#  waitpid()-ing on the worker, and passing back to the client any messages
#  on the worker exiting or stopping (i.e., SIGSTOP).
#
#  One tricky aspect is the handling of signals and special characters. We
#  want the worker to receive all of these, but the client must react in
#  synchrony with the worker if they go beyond just what's written out to
#  the screen (e.g., if they kill the worker).  That is acheived as follows:
#  The client's tty is set to raw mode so no special characters (^C, ^Z,
#  etc..) are interpreted on the client side; they're all read and written
#  to the worker's tty (who can then react to them as programmed).  Some of
#  those characters can cause the worker to stop or exit.  The client polls
#  the control connection to the sentry for notifications of those, and
#  replays them on itself (i.e., sends itself a signal with same exit code,
#  or stops itself if the worker has stopped).  For example, if a user types
#  ^Z, the following (typically) happens: ^Z (ASCII 26) is read from the
#  client's STDIN and written to master_fd.  The client pty's line
#  discipline (assuming the pty is in cooked mode) sees ^Z and sends a
#  SIGTSTP to the foreground process -- the worker.  The worker reacts (by
#  default) by sending itself a SIGSTOP and going to sleep.  The sentry's
#  waitpid() returns, with WIFSTOPPED(status)==true.  The sentry sends a
#  message back to the client (via the control socket) that the worker has
#  gone to sleep.  The client receives the message, restores its own tty to
#  original (usually cooked) mode, and sends itself a SIGTSTP, which finally
#  puts it to sleep.  Though the signal route is circuitous, from the user's
#  perspective it all looks good: they've hit ^Z, and the client has gone to
#  sleep (i.e., exactly the same behavior as if there were no client/server
#  split).
#
#  Signals are also proxied from the client to the worker. We install a
#  signal handler on the client for all signals (but a few that wouldn't
#  make sense to proxy -- e.g., SIGCHLD) and pass them on to the worker
#  (i.e.  killpg(worker_pid, signum)).  This makes the client process look
#  and feel even more as if it were the actual worker.  E.g., if used in
#  bash scripts, etc.  -- killing it with anything but SIGKILL produces the
#  same effect as killing the underlying worker.  SIGWINCH is handhled
#  differently -- it's caught, the new terminal window size is read, and
#  written to the worker pty's master_fd (which causes the pty to trigger
#  SIGWINCH on the worker, which can then handle the window size change).
#

import socket, os, sys, marshal, tty, fcntl, termios, select, signal

# construct our unique socket name
def _construct_socket_name():
    import inspect, hashlib

    # the name of the file at the top of the call stack (should be our main file)
    pth = inspect.stack()[-1].filename
    py = sys.executable
    env = "PYTHONPATH=" + os.environ.get("PYTHONPATH",'')

    state=f"{pth}-{py}-{env}"

    # compute the md5 of the elements that affect the execution environment,
    # to get something unique.
    # FIXME: this should also incorporate environ, current python interpreted
    md5 = hashlib.md5(state.encode('utf-8')).hexdigest()

    # take the filename, w/o the extension
    fn = pth.split('/')[-1].split('.')[0]

    return os.path.join(os.environ['XDG_RUNTIME_DIR'], f"{fn}.{md5}.socket")

socket_path = _construct_socket_name()

# Helpful constants
STDIN  = 0
STDOUT = 1
STDERR = 2

# our own fast-ish logging routines
# (importing logging seems to add ~15msec to runtime, tested on macOS/MBA)
#
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
#
#   Child spawner
#
#############################################################################

def _spawn(conn, fp):
    #
    # Spawn a child to execute the payload. The parent will
    # stay behind to communicate the child's status to the client.
    #

    # receive the command line
    sys.argv = _read_object(fp)
    
    # receive the cwd (and change to it)
    cwd = _read_object(fp)
    os.chdir(cwd)

    # receive the environment
    env = _read_object(fp)
    os.environ.clear()
    os.environ.update(env)

    # File descriptors that we should directly dup2-licate
    fdidx = _read_object(fp) # a list of one or more of [STDIN, STDOUT, STDERR]
#    debug(f"{fdidx=}")
    if len(fdidx):
        _, fds, _, _ = socket.recv_fds(conn, 10, maxfds=len(fdidx))
    else:
        fds = []
#    debug(f"{fds=}")
    for a, b in zip(fds, fdidx):
#        debug(f"_spawn: duplicating fd {a} to {b}")
        os.dup2(a, b)

    # receive the client PID (FIXME: we don't really use this)
    remote_pid = _read_object(fp)

    havetty = len(fdidx) != 3
    if havetty:
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

        # duplicate what's needed
        if STDERR not in fdidx: os.dup2(slave_fd, STDERR); #debug(f"{(slave_fd, STDERR)=}")
        if STDIN  not in fdidx: os.dup2(slave_fd, STDIN); #debug(f"{(slave_fd, STDIN)=}")
        if STDOUT not in fdidx: os.dup2(slave_fd, STDOUT); #debug(f"{(slave_fd, STDOUT)=}")

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
        if havetty:
            os.close(slave_fd)		# this has now been duplicated to STD* stream
        global _client_fp
        _client_fp = fp

        # wait until the parent sets us up
        while not len(os.read(r, 1)):
            pass
        os.close(r)

        # return to __main__ to run the payload
        return
    else:
        # change name to denote we're the sentry
        #import setproctitle
        #setproctitle.setproctitle(f"{' '.join(sys.argv)} [sentry for {pid=}]")

        os.close(r)			# we'll only be writing
        
        os.setpgid(pid, pid)		# start a new process group for the child
        if havetty:
            os.tcsetpgrp(slave_fd, pid)	# make the child's the foreground process group (so it receives tty input+signals)

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
#            debug(f"SENTRY: waitpid on {pid=}")
            _, status = os.waitpid(pid, os.WUNTRACED | os.WCONTINUED)
#            debug(f"SENTRY: waitpid returned {status=}")
#            debug(f"SENTRY: {os.WIFSTOPPED(status)=} {os.WIFEXITED(status)=} {os.WIFSIGNALED(status)=} {os.WIFCONTINUED(status)=}")
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
        if havetty: os.close(slave_fd)
        conn.close()

def _unlink_socket():
    # atexit handler registered by _server
    if os.path.exists(socket_path):
        os.unlink(socket_path)

def _server(timeout=None, readypipe=None):
    # Execute preload code
##    exec(preload)

    # avoid the race condition where two programs are launched
    # at the same time, and try to create the same socket. We'll create
    # our socket in {socket_path}.{pid}, then mv to {socket_path}.
    # that way, the last process wins.
    pid = os.getpid()
    spath = f"{socket_path}.{pid}"
    if os.path.exists(spath):
        os.unlink(spath)

    import atexit
    atexit.register(_unlink_socket)

#    debug(f"Opening socket at {socket_path=} with {timeout=}...", end='')
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock:
        sock.settimeout(timeout)
        os.fchmod(sock.fileno(), 0o700)		# security: only allow the user to do anything w. the socket
        sock.bind(spath)
        sock.listen()
        os.rename(spath, socket_path)
#        debug(' done.')

        if readypipe is not None:
#            debug('signaling on readypipe')
            # signal to the reader the server is ready to accept connections
            os.write(readypipe, b"x")
            os.close(readypipe)
#            debug('done')

        # Await for client connections (or server commands)
        while True:
            try:
                conn, _ = sock.accept()
            except socket.timeout:
                debug("Server timeout. Exiting")
                sys.exit(0)
#            debug(f"Connection accepted")

            # make our life easier & create a file-like object
            fp = conn.makefile(mode='rwb', buffering=0)

            cmd = _read_object(fp)
#            debug(f"{cmd=}")
            if cmd == "stop":
                # exit the listen loop
                debug("Server received a command to exit. Exiting")
                sys.exit(0)
            elif cmd == "run":
                # Fork a child process to do the work.
                pid = os.fork()
                if pid == 0:
                    # Child process -- this is where the work gets done
                    atexit.unregister(_unlink_socket)
                    sock.close()
                    return _spawn(conn, fp)
                else:
                    conn.close()

    # This function will continue _only_ in the spawned child process,
    # and execute the main program.

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

def _copy(master_fd, tty_fd, control_fp, termios_attr, remote_pid):
    """Copy and control loop
    Copies
            pty master -> standard output   (master_read)
            standard input -> pty master    (stdin_read)
    and also listens for control messages from the child
    on control_fd/fp.
    """
    control_fd = control_fp.fileno()
    fds = [ control_fd ]
    if master_fd is not None: fds.append(master_fd)
    if tty_fd is not None: fds.append(tty_fd)
    import time
#    debug(f"{fds=} {master_fd=} {tty_fd=}")
    while fds:
        rfds, _wfds, _xfds = select.select(fds, [], [])
#        debug(f"{rfds=} {time.time()=}")

        # received output
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
                os.write(tty_fd, data)

        # received input
        if tty_fd in rfds:
            data = os.read(tty_fd, 1024)
            if not data:
                fds.remove(tty_fd)
            else:
                _writen(master_fd, data)

        # received a control message
        if control_fd in rfds:
            # a control message from the worker. they've
            # paused, exited, etc.
            event, data = _read_object(control_fp)
#            debug(f"CLIENT: received {event=}")
            if event == "stopped":
                if tty_fd is not None:
                    # it's possible we've been backrounded by the time we got here,
                    # so ignore SIGTTOU while mode-setting. This can happen if someone sent
                    # us (the client) an explicit SIGTSTP.
                    signal.signal(signal.SIGTTOU, signal.SIG_IGN)
                    termios.tcsetattr(tty_fd, tty.TCSAFLUSH, termios_attr)	# restore tty
                    signal.signal(signal.SIGTTOU, signal.SIG_DFL)
#                    debug("CLIENT: Putting us to sleep")
#                    os.kill(os.getpid(), signal.SIGSTOP)			# put ourselves to sleep
                os.kill(0, signal.SIGSTOP)			# put ourselves to sleep

                # this is where we sleep....
                # ... and continue when we're awoken by SIGCONT (e.g., 'fg' in the shell)

                if tty_fd is not None:
# 	             debug("CLIENT: Awake again")
                    tty.setraw(tty_fd)					# turn the STDIN raw again

                    # set terminal size (in case it changed while we slept)
                    s = fcntl.ioctl(tty_fd, termios.TIOCGWINSZ, '\0'*8)
                    fcntl.ioctl(master_fd, termios.TIOCSWINSZ, s)

                # FIXME: we should message the nanny to do this (pid race condition!)
                os.killpg(os.getpgid(remote_pid), signal.SIGCONT)	# wake up the worker process
            elif event == "exited":
                return data # data is the exitstatus
            elif event == "signaled":
#                return -1
                signum = data  # data is the signal that terminated the worker
                if tty_fd is not None:
                    termios.tcsetattr(tty_fd, tty.TCSAFLUSH, termios_attr)	# restore tty back from the raw mode
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
#        debug(f"_handle_ISIG: {signum=}")
        os.killpg(os.getpgid(remote_pid), signum)

    # forward all signals that make sense to forward
    fwd_signals = set(signal.Signals) - {signal.SIGKILL, signal.SIGSTOP, signal.SIGCHLD, signal.SIGWINCH}
    for signum in fwd_signals:
        signal.signal(signum, _handle_ISIG)

#
# Really useful explanation of how SIGTSTP SIGSTOP CTRL-Z work:
#   https://news.ycombinator.com/item?id=8773740
#
def _connect(timeout=None):
    # try connecting to the UNIX socket. If successful, pass it our command
    # line (argv).  If connection is not successful, start the server.
#    if not os.path.exists(socket_path):
#        return _server(preload, payload, timeout)

    # try connecting
    try:
        client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        client.connect(socket_path)
    except (FileNotFoundError, ConnectionRefusedError):
        # connection failed; return None
        return None

    fp = client.makefile(mode='rwb', buffering=0)

    cmd = os.environ.get("INSTA_CMD", None)
    if cmd is not None:
        debug(f"Messaging the server {cmd=}")
        _write_object(fp, cmd)
        sys.exit(0)

    # tell the server we want to run a command
    _write_object(fp, "run")

    # send our command line
    _write_object(fp, sys.argv)

    # send cwd
    _write_object(fp, os.getcwd())

    # send environment
    _write_object(fp, os.environ.copy())

    # find which one of our STD* descriptors point to the tty.
    # send non-tty file descriptors directly to the worker. These will
    # be dup2-ed, rather than manually copied to in the _copy loop.
    pipes = filter(lambda fd: not os.isatty(fd), [STDIN, STDOUT, STDERR])
    pipes, tty_fd = [], None
    for fd in [STDIN, STDOUT, STDERR]:
        if not os.isatty(fd):
            pipes.append(fd)
        elif tty_fd is None:
            ttyname = os.ttyname(fd)
#            debug(f"{ttyname=}")
            tty_fd = os.open(ttyname, os.O_RDWR)

#    debug(f"Non-tty {pipes=}")
#    debug(f"{tty_fd=}")
    _write_object(fp, pipes)
    if len(pipes):
        socket.send_fds(client, [ b'm' ], pipes)

    # send our PID (FIXME: is this necessary?)
    _write_object(fp, os.getpid())

    if tty_fd is not None:
        # we'll need a pty. the server will create it for us, and we
        # need to receive and set it up.
        _, (master_fd,), _, _ = socket.recv_fds(client, 10, maxfds=1)
        termios_attr = termios.tcgetattr(tty_fd)
        termios.tcsetattr(master_fd, termios.TCSAFLUSH, termios_attr)
        _setwinsize(master_fd, _getwinsize(tty_fd))
        _write_object(fp, "OK")

        # set up the SIGWINCH handler which copies terminal window changes
        # to the pty
        signal.signal(
            signal.SIGWINCH,
            lambda signum, frame: _setwinsize(master_fd, _getwinsize(tty_fd))
        )
    else:
        # no tty, pipes all the way
        master_fd = termios_attr = None

    # get the child PID
    remote_pid = _read_object(fp)

    # pass any signals we receive back to the worker
    _setup_signal_passthrough(remote_pid)

    # Now enter the control loop
    try:
        # switch our input to raw mode
        # See here for _very_ useful info about raw mode:
        #   https://stackoverflow.com/questions/51509348/python-tty-setraw-ctrl-c-doesnt-work-getch
        if tty_fd is not None:
            tty.setraw(tty_fd)

        # Now enter the communication forwarding loop
        exitcode = _copy(master_fd, tty_fd, fp, termios_attr, remote_pid)
    finally:
        # restore our console
        if tty_fd is not None:
            termios.tcsetattr(tty_fd, tty.TCSAFLUSH, termios_attr)

    return exitcode

#if __name__ == "__main__":
#    if os.environ.get("CLIENT", None):
#        ret = _connect("", "")
#    else:
#        ret = _server("import dask.distributed", "print('Hello World!')")
#
#    exit(ret)

# re-import main as a module, to trigger the preload
#if not hasattr(sys.modules['instastart.auto'], "preloaded"):
#    preloaded = True
#    print("Hello")
#    print(sys.modules['__main__'].__file__)
#    print(dir(sys.modules['__main__']))
#
#    # import the main file as a module
##    from importlib.util import spec_from_loader, module_from_spec
##    from importlib.machinery import SourceFileLoader 
##
##    spec = spec_from_loader("foobar", SourceFileLoader("foobar", "/path/to/foobar"))
##    foobar = module_from_spec(spec)
##    spec.loader.exec_module(foobar)
#
#else:
#    print(f"{preloaded=}")

from contextlib import contextmanager

@contextmanager
def serve():
    start()
    
#    code = 0
#    try:

    yield

#    except SystemExit as e:
#        code = e.code
#        raise
#    except Exception:
#        raise
#        import traceback
#        print(traceback.format_exc(), file=sys.stderr)
#        raise
#    finally:
#        return
#        if code is None:
#            done(0)
#        elif isinstance(code, int):
#            done(code)
#        else:
#            done(1)

def start():
    # run the server
#    print("Spinning up the server... {_w=}")
    timeout = os.environ.get("INSTA_TIMEOUT", 10)
    _server(timeout=timeout, readypipe=_w)

def done(exitcode=0):
    # Flush the output back to the client
    if not sys.stdout.closed: sys.stdout.flush()
    if not sys.stderr.closed: sys.stderr.flush()
    
    # signal the client we've finished, and that it should
    # move on.
    #
    # FIXME: HACK: This is a _HUGE_ hack, introducing a race condition w. the sentry process.
    #              only the sentry should ever be talking to _client_fp; we should
    #              have a pipe back to the sentry instead.
    global _client_fp
    _write_object(_client_fp, ("exited", exitcode))

def _fork_and_wait_for_server():
    global _w
    _r, _w = os.pipe()
    pid = os.fork()
    if pid == 0:
        os.close(_r)
        import setproctitle
        name = sys.argv[0].split('/')[-1]
        setproctitle.setproctitle(f"[instastart: {name}]")

        # child -- this is what will become the server. Just fall through
        # the code, to be caught in start(). This will launch the
        # server, setting up its socket, etc., and signaling we're ready by
        # writing to _r pipe.

        # start a new session, to protect the child from SIGHUPs, etc.
        # we don't double-fork as the daemon never really does anything
        # funny with the tty (TODO: should we still double-fork, just in case?)
        os.setsid()

        # now fall through until we hit start() somewhere in __main__
        pass
    else:
        os.close(_w)
        # parent -- we'll wait for the server to become available, then connect to it.
#        print(f"Awaiting a signal at {socket_path=}")
        while True:
            msg = os.read(_r, 1)
#            debug(msg)
            if len(msg):
                break
        os.close(_r)
#        print(f"Signal received!")
    return pid

def _connect_or_serve():
    # try connecting on our socket; if fail, spawn a new server

    # try connecting
    ret = _connect()
    if ret is not None:
        sys.exit(ret)

    # fork the server. This will return pid or 0, depending on if it's
    # child or parent
    if _fork_and_wait_for_server() != 0:
        # parent (== client)

        # try connecting again
        ret = _connect()
        if ret is not None:
            sys.exit(ret)
        else:
            raise Exception("Uh-oh... Failed to connect to instastart background process!")
    else:
        # this will fall through the code until, running all code that
        # should be prewarmed until it's paused in start()
        pass

_connect_or_serve()
