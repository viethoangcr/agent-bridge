package process

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"time"
)

// Start validates req, reserves concurrency capacity, spawns the command in its
// own process group, and returns a running snapshot. On spawn failure it
// releases the reservation without retaining a record.
func (m *Manager) Start(req StartRequest) (Snapshot, error) {
	command := strings.TrimSpace(req.Command)
	if command == "" {
		return Snapshot{}, fmt.Errorf("%w: command is required", ErrValidation)
	}
	effectiveCwd := m.cwd
	if req.Cwd != "" {
		effectiveCwd = req.Cwd
	}

	cmd := exec.Command(command, req.Args...)
	cmd.Dir = effectiveCwd
	cmd.Env = mergeEnv(m.baseEnv, req.Env)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	stdin, err := cmd.StdinPipe()
	if err != nil {
		return Snapshot{}, fmt.Errorf("%w: stdin pipe: %v", ErrStart, err)
	}
	// Explicit pipes, not cmd.StdoutPipe/cmd.StderrPipe: cmd.Wait closes exec's
	// own parent pipes once the direct child is reaped, which would discard any
	// buffered output the pumps have not read yet. The pumps own these read
	// ends and close them after EOF.
	stdoutR, stdoutW, err := os.Pipe()
	if err != nil {
		closePipe(stdin)
		return Snapshot{}, fmt.Errorf("%w: stdout pipe: %v", ErrStart, err)
	}
	stderrR, stderrW, err := os.Pipe()
	if err != nil {
		closePipe(stdin)
		closePipe(stdoutR)
		closePipe(stdoutW)
		return Snapshot{}, fmt.Errorf("%w: stderr pipe: %v", ErrStart, err)
	}
	cmd.Stdout = stdoutW
	cmd.Stderr = stderrW

	if err := m.reserveProcess(); err != nil {
		closePipe(stdin)
		closePipe(stdoutR)
		closePipe(stdoutW)
		closePipe(stderrR)
		closePipe(stderrW)
		return Snapshot{}, err
	}
	if err := cmd.Start(); err != nil {
		closePipe(stdin)
		closePipe(stdoutR)
		closePipe(stdoutW)
		closePipe(stderrR)
		closePipe(stderrW)
		m.releaseProcess()
		return Snapshot{}, fmt.Errorf("%w: %v", ErrStart, err)
	}
	// The parent must drop its write ends so the pumps observe EOF once the
	// direct child and every descendant have closed theirs.
	closePipe(stdoutW)
	closePipe(stderrW)

	pid := cmd.Process.Pid
	p := &managedProcess{
		id:             m.newID(),
		command:        command,
		args:           append([]string(nil), req.Args...),
		cwd:            effectiveCwd,
		status:         StatusRunning,
		pid:            pid,
		signals:        signalGate{pid: pid, proc: cmd.Process},
		createdAtMs:    time.Now().UnixMilli(),
		manager:        m,
		cmd:            cmd,
		stdin:          stdin,
		inputAdmission: make(chan struct{}, 1),
		ring:           newLogRing(m.config.load().MaxLogBytesPerProcess),
		done:           make(chan struct{}),
	}

	m.mu.Lock()
	m.processes[p.id] = p
	snapshot := p.snapshot()
	m.mu.Unlock()

	// A concurrent Shutdown marks the manager closing after this start passed
	// its reservation check but before the group was registered. Self-kill the
	// freshly spawned group so it cannot outlive the shutdown snapshot.
	if m.isClosing() {
		_ = p.signalGroup(syscall.SIGKILL)
	}

	p.pumps.Add(2)
	go p.pump("stdout", stdoutR)
	go p.pump("stderr", stderrR)
	go m.watch(p)

	return snapshot, nil
}
