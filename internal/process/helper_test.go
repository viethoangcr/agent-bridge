package process

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"testing"
	"time"
)

// The test binary doubles as the managed-process helper. TestMain re-execs
// os.Args[0] with processHelperEnv set to one of the modes below; no testdata
// helper program is needed. Helper modes report the direct/grandchild PIDs to
// processHelperPIDEnv and the environment/cwd report to processHelperReportEnv.
const (
	processHelperEnv       = "AGENT_BRIDGE_TEST_PROCESS_HELPER"
	processHelperPIDEnv    = "AGENT_BRIDGE_TEST_PROCESS_PIDFILE"
	processHelperReportEnv = "AGENT_BRIDGE_TEST_PROCESS_REPORT"
	processHelperExitEnv   = "AGENT_BRIDGE_TEST_PROCESS_EXIT"
)

const (
	helperOutput     = "output"
	helperBlocking   = "blocking"
	helperEnvCwd     = "env-cwd"
	helperExitCode   = "exit-code"
	helperSignal     = "signal"
	helperChild      = "child"
	helperChildExits = "child-exits"
	helperGrandchild = "grandchild"
	helperStdinEcho  = "stdin-echo"
	helperBadUTF8    = "bad-utf8"
	helperBurst      = "burst"
)

// helperBurstBytes is deliberately larger than a Linux pipe buffer so the
// managed pump must drain the kernel pipe while the child is still writing.
const helperBurstBytes = 1<<20 + 1<<16

// burstPayload returns the deterministic byte pattern the burst helper writes,
// shared by the child and its asserting test.
func burstPayload() []byte {
	data := make([]byte, helperBurstBytes)
	for i := range data {
		data[i] = byte(i*7 + 3)
	}
	return data
}

// helperPIDs is the JSON line a spawning helper reports: the direct child PID
// and the grandchild PID that shares its process group.
type helperPIDs struct {
	Direct     int `json:"direct"`
	Grandchild int `json:"grandchild"`
}

// helperReport is the environment/cwd report a helper writes.
type helperReport struct {
	Cwd string   `json:"cwd"`
	Env []string `json:"env"`
}

func TestMain(m *testing.M) {
	if mode := os.Getenv(processHelperEnv); mode != "" {
		runProcessHelper(mode)
	}
	os.Exit(m.Run())
}

// runProcessHelper runs one helper mode and terminates the process. It never
// returns for blocking modes.
func runProcessHelper(mode string) {
	switch mode {
	case helperOutput:
		_, _ = os.Stdout.WriteString("stdout-payload")
		_, _ = os.Stderr.WriteString("stderr-payload")
	case helperBlocking, helperSignal:
		blockProcessHelper()
	case helperGrandchild:
		blockProcessHelper()
	case helperEnvCwd:
		runEnvCwdHelper()
	case helperExitCode:
		code, err := strconv.Atoi(os.Getenv(processHelperExitEnv))
		if err != nil {
			fmt.Fprintln(os.Stderr, "exit helper:", err)
			os.Exit(2)
		}
		os.Exit(code)
	case helperChild:
		writeHelperPIDs(startHelperGrandchild())
		blockProcessHelper()
	case helperChildExits:
		writeHelperPIDs(startHelperGrandchild())
	case helperStdinEcho:
		_, _ = io.Copy(os.Stdout, os.Stdin)
	case helperBadUTF8:
		_, _ = os.Stdout.Write([]byte{0xff, 0xfe, 'h', 'i'})
	case helperBurst:
		_, _ = os.Stdout.Write(burstPayload())
	default:
		fmt.Fprintln(os.Stderr, "unknown process helper mode:", mode)
		os.Exit(2)
	}
	os.Exit(0)
}

// runEnvCwdHelper writes the effective environment and cwd as JSON to the path
// named by processHelperReportEnv.
func runEnvCwdHelper() {
	cwd, err := os.Getwd()
	if err != nil {
		fmt.Fprintln(os.Stderr, "getwd:", err)
		os.Exit(2)
	}
	data, err := json.Marshal(helperReport{Cwd: cwd, Env: os.Environ()})
	if err != nil {
		fmt.Fprintln(os.Stderr, "marshal report:", err)
		os.Exit(2)
	}
	if err := os.WriteFile(os.Getenv(processHelperReportEnv), data, 0o600); err != nil {
		fmt.Fprintln(os.Stderr, "write report:", err)
		os.Exit(2)
	}
}

// startHelperGrandchild launches a grandchild that inherits this process's
// stdout/stderr (and therefore its process group) and blocks.
func startHelperGrandchild() int {
	cmd := exec.Command(os.Args[0])
	cmd.Env = append(os.Environ(), processHelperEnv+"="+helperGrandchild)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		fmt.Fprintln(os.Stderr, "start grandchild:", err)
		os.Exit(2)
	}
	return cmd.Process.Pid
}

func writeHelperPIDs(grandchild int) {
	path := os.Getenv(processHelperPIDEnv)
	if path == "" {
		return
	}
	line, err := json.Marshal(helperPIDs{Direct: os.Getpid(), Grandchild: grandchild})
	if err != nil {
		fmt.Fprintln(os.Stderr, "marshal pids:", err)
		os.Exit(2)
	}
	line = append(line, '\n')
	if err := os.WriteFile(path, line, 0o600); err != nil {
		fmt.Fprintln(os.Stderr, "write pid file:", err)
		os.Exit(2)
	}
}

// blockProcessHelper blocks until the process group is killed. It never
// returns and must not be used in test-process code paths.
func blockProcessHelper() {
	for {
		time.Sleep(time.Hour)
	}
}
