package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"sync"
	"time"
	"net"

	"golang.org/x/sync/errgroup"

	abi "github.com/lynoferraz/abigo"
	"github.com/prototyp3-dev/go-rollups/handler"
	"github.com/prototyp3-dev/go-rollups/handler/abi"
	"github.com/prototyp3-dev/go-rollups/rollups"
)

type FlashDriveConfig struct {
	ImageFilename string `json:"image_filename"`
	Length        uint   `json:"length"`
	Shared        bool   `json:"shared"`
	Start         uint   `json:"start"`
}

type MachineConfig struct {
	FlashDriveConfig []FlashDriveConfig `json:"flash_drive"`
}

type CartesiMachineConfig struct {
	Config MachineConfig `json:"config"`
}

var infolog = log.New(os.Stderr, "[ info ]  ", log.Lshortfile)
var warnlog = log.New(os.Stderr, "[ warn ]  ", log.Lshortfile)

var dirMode fs.FileMode = 0755
var fileMode fs.FileMode = 0644
var waitDelay time.Duration = 10*time.Second
var remoteCmInitDelayTimeout time.Duration = 1*time.Second

var cmCommand string = "cartesi-machine"
var remoteCmCommand string = "jsonrpc-remote-cartesi-machine"

var remoteCMPort uint64 = 10000 
var remoteCMHost string = "localhost"
// var remoteCMAddress string = "localhost:8090"
var cmOutput string = "cartesi_machine.out"
var latestLinkPath string = "latest"
var baseImagePath string = "local_image"
var latestBlockPath string = "latest_block"
var workingSnapshotDir string = "working_image"
var lastSnapshotTs uint64 = 0
var lastAdvanceWithSnapshot uint64 = 0
var noSnapshotsYet uint64 = 1

var inputFile string = "epoch-%d-input-%d.bin"
var inputMetadataFile string = "epoch-%d-input-metadata-%d.bin"
var queryFile string = "query.bin"

// var queryResponseFile = "query-report-0.bin"
var metadataTyp = abi.MustNewType("tuple(address,uint256,uint256,uint256,uint256)")
var bytesTyp = abi.MustNewType("tuple(bytes)")
var voucherTyp = abi.MustNewType("tuple(address,bytes)")

var imagePath, fromFlashdrivePath, storeFlashdrivePath, storePath string
var delayRemoteTest float64
var remoteCmInitTimeout float64

// var remoteCmCmd *exec.Cmd
var ctx context.Context
var cancel context.CancelFunc
var errorWaitGroup *errgroup.Group

var dataFlashdriveConfig FlashDriveConfig
var disableConsistencyChecks bool
var disableWorkdir bool
var disableRemoteCm bool
var saveSnapshotNAdvances uint64
var saveSnapshotTimeout uint64


func SetupImagePaths(resetLatestLink bool) error {

	// create store path if it doesn't exist
	_, err := os.Stat(storePath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			if err = os.Mkdir(storePath, dirMode); err != nil {
				return fmt.Errorf("error creating store path: %s", err)
			}
		} else {
			return fmt.Errorf("error reading store path: %s", err)
		}
	}

	// starting image path
	startingImagePath := fmt.Sprintf("%s/%s_start", storePath, baseImagePath)

	// set latest link image
	_, errLink := os.Lstat(filepath.Join(storePath,latestLinkPath))
	if errLink != nil {
		if errors.Is(errLink, os.ErrNotExist) {
			resetLatestLink = true
		} else {
			return fmt.Errorf("error reading link")
		}
	}

	if resetLatestLink {

		// remove old link
		if errLink == nil {
			// read link
			fileInfo, err := os.Lstat(filepath.Join(storePath,latestLinkPath))
			if err != nil {
				return fmt.Errorf("error reading latest link: %s", err)
			}

			removeOldTarget := true
			// remove old link target
			if fileInfo.Mode()&os.ModeSymlink != 0 {
				target, err := os.Readlink(fileInfo.Name())

				if err != nil { // no target
					if errors.Is(err, os.ErrNotExist) {
						removeOldTarget = false
					} else {
						return fmt.Errorf("error getting latest link target: %s",
						err)
					}
				}
				if removeOldTarget {
					if err := os.RemoveAll(target); err != nil {
						return fmt.Errorf("error removing old link target: %s", err)
					}
				}
			}

			if err := os.Remove(filepath.Join(storePath,latestLinkPath)); err != nil {
				return fmt.Errorf("error removing link: %s", err)
			}
		}

		// remove old latest block file
		if _, err := os.Stat(filepath.Join(storePath,latestBlockPath)); err != nil {
			if !errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("latest block file error: %s", err)
			}
		} else {
			if err := os.Remove(filepath.Join(storePath,latestBlockPath)); err != nil {
				return fmt.Errorf("error removing latest block file: %s", err)
			}
		}

		// remove any old starting dir
		if _, err := os.Stat(startingImagePath); err == nil {
			if err := os.RemoveAll(startingImagePath); err != nil {
				return fmt.Errorf("error removing starting image path: %s", err)
			}
		} else if !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("image error: %s", err)
		}

		// copy image path to starting path
		err := filepath.Walk(imagePath,
			func(path string, info os.FileInfo, err error) error {
				var relPath string = strings.TrimPrefix(path, imagePath)
				if err != nil {
					return err
				}
				if info.IsDir() {
					err = os.Mkdir(startingImagePath, info.Mode())
					return err
				} else {
					source, err := os.Open(filepath.Join(imagePath, relPath))
					if err != nil {
						return err
					}
					defer source.Close()

					destination, err := os.Create(
						filepath.Join(startingImagePath, relPath))
					if err != nil {
						return err
					}
					defer destination.Close()

					err = destination.Chmod(info.Mode())
					if err != nil {
						return err
					}

					_, err = io.Copy(destination, source)
					return err
				}
			})
		if err != nil {
			return fmt.Errorf("error copying image path: %s", err)
		}

		// reset cm output
		if _, err := os.Stat(cmOutput); err != nil {
			if !errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("cm log error: %s", err)
			}
		} else {
			if err := os.Remove(cmOutput); err != nil {
				return fmt.Errorf("error removing cm log: %s", err)
			}
		}

		err = os.Symlink(strings.TrimPrefix(startingImagePath, fmt.Sprintf("%s/",filepath.Join(storePath,""))), filepath.Join(storePath,latestLinkPath))
		if err != nil {
			return fmt.Errorf("error creating latest link: %s", err)
		}
	}

	// check image path
	if _, err := os.Stat(filepath.Join(storePath,latestLinkPath)); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("image directory not found")
		}
		return fmt.Errorf("image error: %s", err)
	}

	if fromFlashdrivePath != "" || storeFlashdrivePath != "" {
		// open config file
		configFile, err := os.Open(filepath.Join(storePath, latestLinkPath, "config.json"))
		if err != nil {
			return fmt.Errorf("read config error: %s", err)
		}
		defer configFile.Close()

		configByteValue, err := io.ReadAll(configFile)
		if err != nil {
			return fmt.Errorf("general config byte conversion error: %s", err)
		}

		var machineConfig CartesiMachineConfig
		err = json.Unmarshal(configByteValue, &machineConfig)
		if err != nil {
			return fmt.Errorf("flash drives config unmarshall error: %s", err)
		}

		flasdriveConfigs := machineConfig.Config.FlashDriveConfig
		dataFlashdriveConfig = flasdriveConfigs[len(flasdriveConfigs)-1]

		flashdriveFile := fmt.Sprintf("%016s-%s.bin",
			strconv.FormatInt(int64(dataFlashdriveConfig.Start), 16),
			strconv.FormatInt(int64(dataFlashdriveConfig.Length), 16))
		if _, err := os.Stat(
			filepath.Join(
				storePath, latestLinkPath, flashdriveFile)); err != nil {
			if !errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("drive error: %s", err)
			}
		}

	}

	// remove working snapshot
	_, err = os.Stat(filepath.Join(storePath,workingSnapshotDir))
	if err == nil {
		if err := os.RemoveAll(filepath.Join(storePath,workingSnapshotDir)); err != nil {
			return fmt.Errorf("error removing working snapshot dir: %s", err)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("working snapshot error: %s", err)
	}

	return nil
}

func InitializeRemoteCartesi(
	currCtx context.Context, ready chan<- error) error {
	infolog.Println("remote cm: intializing")

	log, err := os.OpenFile(cmOutput, os.O_APPEND|os.O_CREATE|os.O_WRONLY, fileMode)
	if err != nil {
		return err
	}


	var wg sync.WaitGroup
	command := remoteCmCommand

	args := make([]string, 0)
	args = append(args, fmt.Sprintf("--server-address=%s:%d", remoteCMHost,remoteCMPort))
	// args = append(args, fmt.Sprintf("--log-level=%s", logLevel))

	cmd := exec.CommandContext(currCtx, command, args...)
	cmd.Stdout = log
	cmd.Stderr = log
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true, Pgid: 0}
	cmd.WaitDelay = time.Second * waitDelay
	cmd.Cancel = func() error {
		log.Close()
		
		if ctx.Err() == nil {

			// Send the terminate signal to the process group by passing the negative pid.
			infolog.Println("remote cm: sent SIGTERM command", command)
			err := syscall.Kill(-cmd.Process.Pid, syscall.SIGTERM)
			if err != nil {
				warnlog.Println("remote cm: failed to send SIGTERM ",
					"command", command, "error", err)
			}
			// cancelCommand := cmCommand

			// cancelArgs := make([]string, 0)
			// cancelArgs = append(cancelArgs, fmt.Sprintf("--remote-address=%s", remoteCMHost))
			// cancelArgs = append(args, fmt.Sprintf("--remote-address=%s:%d", remoteCMHost, remoteCMPort))
			// cancelArgs = append(cancelArgs, "--remote-protocol=jsonrpc")
			// cancelArgs = append(cancelArgs, "--no-remote-create")
			// cancelArgs = append(cancelArgs, "--remote-shutdown")
			// cancelArgs = append(cancelArgs, "--skip-root-hash-check")
			// infolog.Println("running: ", command, strings.Join(cancelArgs, " "))
			// cancelCmd := exec.Command(cancelCommand, cancelArgs...)
			// out, err := cancelCmd.CombinedOutput()
			// infolog.Printf("\n====\n%s====", string(out))
			// if err != nil {
			// 	warnlog.Println("remote cm: failed to send remote shutdown ",
			// 		"command", cancelCommand, "error", err)
			// }

			wg.Wait()

			// infolog.Println("remote proc",cmd.ProcessState,cmd.Process.Pid)
			// if cmd.ProcessState != nil {
			// 	infolog.Println("remote exited",cmd.ProcessState.Exited())
			// }

			// for (cmd.ProcessState == nil || !cmd.ProcessState.Exited()) {
			// 	infolog.Println("remote still up")
			// 	time.Sleep(time.Duration(delayRemoteTest * float64(time.Second)))
			// }
			
		}
		
		return nil
	}
	// remoteCmCmd = cmd
	infolog.Println("running: ", command, strings.Join(args, " "))
	wg.Add(1)
	err = cmd.Start()
	if err != nil {
		warnlog.Println("remote cm: failed to initialize:", err)
		ready <- err
		return err
	}
	now := time.Now()
	var conn net.Conn
	for (time.Since(now) < time.Duration(remoteCmInitTimeout) * time.Second) {
		time.Sleep(time.Duration(delayRemoteTest * float64(time.Second)))
		conn, err = net.DialTimeout("tcp", fmt.Sprintf("%s:%d",remoteCMHost,remoteCMPort), remoteCmInitDelayTimeout)
		if err == nil {
			break
		}
	}
	if err != nil {
		warnlog.Println("remote cm: failed to connect remote cm:", err)
		ready <- err
		return err
	}
	if conn == nil {
		warnlog.Println("remote cm: failed to initialize remote cm:", err)
		ready <- err
		return err
	}
	conn.Close()
	ready <- nil
	infolog.Println("remote cm: ready")
	err = cmd.Wait()
	// finishedProcess <- struct{}{}
	if ctx.Err() != nil {
		warnlog.Println("init context error")
		return ctx.Err()
	}
	wg.Done()
	infolog.Println("remote cm: done")
	return err
}

func prepareSnapshot() (string,error) {

	target, err := os.Readlink(filepath.Join(storePath,latestLinkPath))
	var workdirPath string
	if !disableWorkdir {
		// Copy target to work dir
		workdirPath = filepath.Join(storePath,workingSnapshotDir)

		if err != nil { // no target
			return "",fmt.Errorf("error getting latest link target: %s",
			err)
		}
		latestPath := filepath.Join(storePath,target)


		err = filepath.Walk(latestPath,
			func(path string, info os.FileInfo, err error) error {
				var relPath string = strings.TrimPrefix(path, latestPath)
				if err != nil {
					return err
				}
				if info.IsDir() {
					err = os.Mkdir(workdirPath, info.Mode())
					return err
				} else {
					source, err := os.Open(filepath.Join(latestPath, relPath))
					if err != nil {
						return err
					}
					defer source.Close()

					destination, err := os.Create(
						filepath.Join(workdirPath, relPath))
					if err != nil {
						return err
					}
					defer destination.Close()

					err = destination.Chmod(info.Mode())
					if err != nil {
						return err
					}

					_, err = io.Copy(destination, source)
					return err
				}
			})
	} else {
		workdirPath = filepath.Join(storePath,target)
	}
	return workdirPath,err
}

func PreloadCM() error {
	infolog.Println("preloading cm: intializing")

	workdirPath,err := prepareSnapshot()
	if err != nil {
		return fmt.Errorf("error copying workdir path: %s", err)
	}

	command := cmCommand

	args := make([]string, 0)
	args = append(args, fmt.Sprintf("--load=%s", workdirPath))
	args = append(args, fmt.Sprintf("--remote-address=%s:%d", remoteCMHost, remoteCMPort))
	args = append(args, "--remote-protocol=jsonrpc")
	args = append(args, "--no-remote-destroy")
	// args = append(args, "--max-mcycle=0")
	if disableConsistencyChecks {
		args = append(args, "--skip-root-hash-check")
		args = append(args, "--skip-root-hash-store")
	}
	args = append(args, "--assert-rolling-template")

	if fromFlashdrivePath != "" {
		if _, err := os.Stat(fromFlashdrivePath); err == nil {
			args = append(args,
				fmt.Sprintf(
					"--replace-flash-drive=start:%d,length:%d,filename:%s",
					dataFlashdriveConfig.Start,
					dataFlashdriveConfig.Length,
					fromFlashdrivePath))
		}
	}

	infolog.Println("running: ", command, strings.Join(args, " "))
	cmd := exec.Command(command, args...)
	out, err := cmd.CombinedOutput()
	infolog.Printf("\n====\n%s====", string(out))
	if err != nil {
		return err
	}
	infolog.Println("preloading cm: finished")
	return nil
}

func ReadCMOutput(file string) ([]byte, error) {
	var dataOut []byte
	fileBytes, err := os.ReadFile(file)
	if err != nil {
		return dataOut, fmt.Errorf("error reading query output file: %s", err)
	}
	err = os.Remove(file)
	if err != nil {
		return dataOut, fmt.Errorf("error removing input/output file %s: %s", file, err)
	}

	decoded, err := abi.Decode(bytesTyp, fileBytes)
	if err != nil {
		return dataOut, fmt.Errorf("error decoding query output: %s", err)
	}

	mapResult, ok := decoded.(map[string]interface{})
	if !ok {
		return dataOut, fmt.Errorf("convert decoded payload to map error")
	}

	dataOut, ok = mapResult["0"].([]byte)
	if !ok {
		message := "convert decoded payload map to bytes error"
		return dataOut, fmt.Errorf(message)
	}
	return dataOut, nil
}

func ReadCMVoucher(file string) (abihandler.Address, []byte, error) {
	var dataOut []byte
	var address abihandler.Address
	fileBytes, err := os.ReadFile(file)
	if err != nil {
		return address, dataOut, fmt.Errorf("error reading query output file: %s", err)
	}
	err = os.Remove(file)
	if err != nil {
		return address, dataOut, fmt.Errorf("error removing input/output file %s: %s",
			file, err)
	}

	decoded, err := abi.Decode(voucherTyp, fileBytes)
	if err != nil {
		return address, dataOut, fmt.Errorf("error decoding query output: %s", err)
	}

	mapResult, ok := decoded.(map[string]interface{})
	if !ok {
		return address, dataOut, fmt.Errorf("convert decoded payload to map error")
	}

	address, ok = mapResult["0"].(abihandler.Address)
	if !ok {
		message := "convert decoded payload map to addrss error"
		return address, dataOut, fmt.Errorf(message)
	}
	dataOut, ok = mapResult["1"].([]byte)
	if !ok {
		message := "convert decoded payload map to bytes error"
		return address, dataOut, fmt.Errorf(message)
	}
	return address, dataOut, nil
}

func HandleInspect(payloadHex string) error {
	infolog.Println("inspect: received")
	// encode query
	payloadMap := make(map[string]interface{})
	data, err := rollups.Hex2Bin(payloadHex)
	if err != nil {
		return fmt.Errorf("error converting payload to bin: %s", err)
	}

	payloadMap["0"] = data

	queryPayload, err := abi.Encode(payloadMap, bytesTyp)
	if err != nil {
		return fmt.Errorf("error encoding payload: %s", err)
	}

	// save payload in query file
	err = os.WriteFile(queryFile, queryPayload, os.ModePerm)
	if err != nil {
		return fmt.Errorf("error writing file: %s", err)
	}
	defer os.Remove(queryFile)

	command := cmCommand

	args := make([]string, 0)
	args = append(args, fmt.Sprintf("--remote-address=%s:%d", remoteCMHost, remoteCMPort))
	args = append(args, "--remote-protocol=jsonrpc")
	args = append(args, "--no-remote-create")
	args = append(args, "--no-remote-destroy")
	if disableConsistencyChecks {
		args = append(args, "--skip-root-hash-check")
		args = append(args, "--skip-root-hash-store")
	}
	args = append(args, "--assert-rolling-template")
	args = append(args, "--rollup-inspect-state")
	// args = append(args, "--quiet")

	infolog.Println("running: ", command, strings.Join(args, " "))
	cmd := exec.Command(command, args...)
	out, err := cmd.CombinedOutput()
	infolog.Printf("\n====\n%s====", string(out))
	if err != nil {
		return err
	}

	// read output and send report
	files, err := filepath.Glob("query-report-*.bin")
	slices.Sort(files)
	if err != nil {
		return fmt.Errorf("error getting output files: %s", err)
	}
	for _, f := range files {
		// infolog.Println("sending report from", f)
		dataOut, err := ReadCMOutput(f)
		if err != nil {
			return fmt.Errorf("error reading query output file: %s", err)
		}

		_, err = rollups.SendReport(&rollups.Report{Payload: rollups.Bin2Hex(dataOut)})
		if err != nil {
			return fmt.Errorf("error making http request: %s", err)
		}
	}

	infolog.Println("inspect: finished")
	return nil
}

func HandleAdvance(metadata *rollups.Metadata, payloadHex string) error {
	infolog.Println("advance: received")
	payloadMap := make(map[string]interface{})
	data, err := rollups.Hex2Bin(payloadHex)
	if err != nil {
		return fmt.Errorf("error converting payload to bin: %s", err)
	}

	if _, err := os.Stat(filepath.Join(storePath,latestBlockPath)); err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("latest block file error: %s", err)
		}
	} else {
		latestBlockBytes, err := os.ReadFile(filepath.Join(storePath,latestBlockPath))
		if err != nil {
			return fmt.Errorf("latest block reding file error: %s", err)
		}

		latestBlock, err := strconv.Atoi(string(latestBlockBytes))
		if err != nil {
			return fmt.Errorf("latest block converting value error: %s", err)
		}

		if latestBlock >= int(metadata.BlockNumber) {
			warnlog.Println("skipping input from block",metadata.BlockNumber,"(latest",latestBlock,")")
			return nil
		}
	
	}

	payloadMap["0"] = data

	advancePayload, err := abi.Encode(payloadMap, bytesTyp)
	if err != nil {
		return fmt.Errorf("error encoding payload: %s", err)
	}

	// save payloadin advance file
	iFile := fmt.Sprintf(inputFile, metadata.EpochIndex, metadata.InputIndex)
	// iFile := fmt.Sprintf(inputFile, 0, 0)
	err = os.WriteFile(iFile, advancePayload, os.ModePerm)
	if err != nil {
		return fmt.Errorf("error writing file: %s", err)
	}

	// save metadata in advance file
	payloadMetadateMap := make(map[string]interface{})
	payloadMetadateMap["0"] = metadata.MsgSender
	payloadMetadateMap["1"] = metadata.BlockNumber
	payloadMetadateMap["2"] = metadata.Timestamp
	payloadMetadateMap["3"] = metadata.EpochIndex
	payloadMetadateMap["4"] = metadata.InputIndex

	advanceMetadata, err := abi.Encode(payloadMetadateMap, metadataTyp)
	if err != nil {
		return fmt.Errorf("error encoding payload: %s", err)
	}

	// save payloadin advance file
	imFile := fmt.Sprintf(inputMetadataFile, metadata.EpochIndex, metadata.InputIndex)
	// imFile := fmt.Sprintf(inputMetadataFile, 0, 0)
	err = os.WriteFile(imFile, advanceMetadata, os.ModePerm)
	if err != nil {
		return fmt.Errorf("error writing file: %s", err)
	}

	newImagePath := fmt.Sprintf("%s/%s_%d_%d_%d",
		storePath, baseImagePath, metadata.EpochIndex,
		metadata.InputIndex, metadata.BlockNumber)
	
	storeCurrentAdvance := 
		(saveSnapshotNAdvances > 0 && metadata.InputIndex >= lastAdvanceWithSnapshot + saveSnapshotNAdvances - noSnapshotsYet) || 
		(saveSnapshotTimeout > 0 && metadata.Timestamp >= lastSnapshotTs + saveSnapshotTimeout)

	command := cmCommand

	args := make([]string, 0)
	if !disableRemoteCm {
		args = append(args, fmt.Sprintf("--remote-address=%s:%d", remoteCMHost, remoteCMPort))
		args = append(args, "--remote-protocol=jsonrpc")
		args = append(args, "--no-remote-create")
		// args = append(args, "--no-remote-destroy")
	} else {
		workdirPath,err := prepareSnapshot()
		if err != nil {
			return fmt.Errorf("error copying workdir path: %s", err)
		}
		args = append(args, fmt.Sprintf("--load=%s", workdirPath))
		args = append(args, "--no-rollback")
	}

	if disableConsistencyChecks {
		args = append(args, "--skip-root-hash-check")
		args = append(args, "--skip-root-hash-store")
	}
	args = append(args, "--assert-rolling-template")
	args = append(args, fmt.Sprintf(
		"--rollup-advance-state=epoch_index:%d,input_index_begin:%d,input_index_end:%d",
		metadata.EpochIndex, metadata.InputIndex, metadata.InputIndex+1))

	if storeCurrentAdvance {
		args = append(args, fmt.Sprintf("--store=%s", newImagePath))
		if !disableRemoteCm {
			args = append(args, "--remote-shutdown")
		}
	}
	// args = append(args, "--quiet")

	infolog.Println("running: ", command, strings.Join(args, " "))
	cmd := exec.Command(command, args...)
	out, cmdErr := cmd.CombinedOutput()
	infolog.Printf("\n====\n%s====", string(out))

	// Redirect reports
	files, err := filepath.Glob(fmt.Sprintf(
		"epoch-%d-input-%d-report-[0-9]*.bin", metadata.EpochIndex, metadata.InputIndex))
	slices.Sort(files)
	if err != nil {
		return fmt.Errorf("error getting output files: %s", err)
	}
	for _, f := range files {
		// infolog.Println("sending report from", f)
		dataOut, err := ReadCMOutput(f)
		if err != nil {
			return fmt.Errorf("error reading query output file: %s", err)
		}

		_, err = rollups.SendReport(&rollups.Report{Payload: rollups.Bin2Hex(dataOut)})
		if err != nil {
			return fmt.Errorf("error making http request: %s", err)
		}
	}

	// Redirect notices
	files, err = filepath.Glob(fmt.Sprintf(
		"epoch-%d-input-%d-notice-[0-9]*.bin", metadata.EpochIndex, metadata.InputIndex))
	slices.Sort(files)
	if err != nil {
		return fmt.Errorf("error getting output files: %s", err)
	}
	for _, f := range files {
		// infolog.Println("sending notice from", f)
		dataOut, err := ReadCMOutput(f)
		if err != nil {
			return fmt.Errorf("error reading query output file: %s", err)
		}

		_, err = rollups.SendNotice(&rollups.Notice{Payload: rollups.Bin2Hex(dataOut)})
		if err != nil {
			return fmt.Errorf("error making http request: %s", err)
		}
	}

	// Redirect vouchers
	files, err = filepath.Glob(fmt.Sprintf(
		"epoch-%d-input-%d-voucher-[0-9]*.bin", metadata.EpochIndex, metadata.InputIndex))
	slices.Sort(files)
	if err != nil {
		return fmt.Errorf("error getting output files: %s", err)
	}
	for _, f := range files {
		// infolog.Println("sending voucher from", f)
		addr, dataOut, err := ReadCMVoucher(f)
		if err != nil {
			return fmt.Errorf("error reading query output file: %s", err)
		}

		_, err = rollups.SendVoucher(
			&rollups.Voucher{Destination: addr.String(),
				Payload: rollups.Bin2Hex(dataOut)})
		if err != nil {
			return fmt.Errorf("error making http request: %s", err)
		}
	}

	// remove all output files
	files, err = filepath.Glob(fmt.Sprintf("epoch-%d-input*.bin", metadata.EpochIndex))
	if err != nil {
		return fmt.Errorf("error getting output files: %s", err)
	}
	for _, f := range files {
		err = os.Remove(f)
		if err != nil {
			return fmt.Errorf("error removing input/output file %s: %s", f, err)
		}
	}

	files, err = filepath.Glob("query-report-*.bin")
	slices.Sort(files)
	if err != nil {
		return fmt.Errorf("error getting output files: %s", err)
	}
	for _, f := range files {
		dataOut, err := ReadCMOutput(f)
		if err != nil {
			return fmt.Errorf("error reading query output file: %s", err)
		}

		_, err = rollups.SendReport(&rollups.Report{Payload: rollups.Bin2Hex(dataOut)})
		if err != nil {
			return fmt.Errorf("error making http request: %s", err)
		}

		err = os.Remove(f)
		if err != nil {
			return fmt.Errorf("error removing input/output file %s: %s", f, err)
		}
	}

	// copying flash drive
	if cmdErr == nil {
		if storeFlashdrivePath != "" {

			flashdriveFile := fmt.Sprintf("%016s-%s.bin",
				strconv.FormatInt(int64(dataFlashdriveConfig.Start), 16),
				strconv.FormatInt(int64(dataFlashdriveConfig.Length), 16))

			source, err := os.Open(filepath.Join(newImagePath, flashdriveFile))
			if err != nil {
				return err
			}
			defer source.Close()

			destination, err := os.Create(storeFlashdrivePath)
			if err != nil {
				return err
			}
			defer destination.Close()
			_, err = io.Copy(destination, source)
			if err != nil {
				return err
			}
		}

		if storeCurrentAdvance {
			// read link
			fileInfo, err := os.Lstat(filepath.Join(storePath,latestLinkPath))
			if err != nil {
				return fmt.Errorf("error reading latest link: %s", err)
			}

			// remove old link target
			if fileInfo.Mode() & os.ModeSymlink != 0 {
				target, err := os.Readlink(filepath.Join(storePath,fileInfo.Name()))

				if err != nil {
					return fmt.Errorf("error getting latest link target: %s", err)
				}

				if err := os.RemoveAll(filepath.Join(storePath,target)); err != nil {
					return fmt.Errorf("error removing old link target: %s", err)
				}
			}

			// remove link and create link with new image
			if err := os.Remove(filepath.Join(storePath,latestLinkPath)); err != nil {
				return fmt.Errorf("error removing link: %s", err)
			}
			err = os.Symlink(strings.TrimPrefix(newImagePath, fmt.Sprintf("%s/",filepath.Join(storePath,""))), filepath.Join(storePath,latestLinkPath))
			if err != nil {
				return fmt.Errorf("error creating latest link: %s", err)
			}

			// remove old latest block
			if _, err := os.Stat(filepath.Join(storePath,latestBlockPath)); err != nil {
				if !errors.Is(err, os.ErrNotExist) {
					return fmt.Errorf("latest block file error: %s", err)
				}
			} else {
				if err := os.Remove(filepath.Join(storePath,latestBlockPath)); err != nil {
					return fmt.Errorf("error removing old latest block file: %s", err)
				}
			}

			lastSnapshotTs = metadata.Timestamp
			lastAdvanceWithSnapshot = metadata.InputIndex
			noSnapshotsYet = 0
		}
		err = os.WriteFile(filepath.Join(storePath,latestBlockPath), []byte(fmt.Sprintf("%d",metadata.BlockNumber)), os.ModePerm)
		if err != nil {
			return fmt.Errorf("error creating latest block file: %s", err)
		}
	} else {
		warnlog.Println("error advancing cartesi machine", cmdErr)

		if storeCurrentAdvance {
			if err := os.RemoveAll(newImagePath); err != nil {
				return fmt.Errorf("error removing new image: %s", err)
			}
			// return cmdErr
		}
	}

	if storeCurrentAdvance {
		if !disableWorkdir {
			if _, err := os.Stat(filepath.Join(storePath,workingSnapshotDir)); err == nil {
				if err := os.RemoveAll(filepath.Join(storePath,workingSnapshotDir)); err != nil {
					return fmt.Errorf("error removing working snapshot dir: %s", err)
				}
			} else if !errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("image error: %s", err)
			}
		}

		if !disableRemoteCm {
			// infolog.Println("canceling remote")
			// err = remoteCmCmd.Cancel()
			// if err != nil {
			// 	defer cancel()
			// 	return fmt.Errorf("remote cm: failed to shutdown: %s", err)
			// }
			
			remoteCMPort = 10000 + metadata.InputIndex
			err = StartRemoteCartesiRoutine()
			if err != nil {
				return fmt.Errorf("remote cm error: %s", err)
			}

			err = PreloadCM()
			if err != nil {
				errorWaitGroup.Go(func() error {
					return fmt.Errorf("preload cm error: %s", err)
				})
				return fmt.Errorf("preload cm error: %s", err)
			}

		}
	}
	if cmdErr != nil {
		return cmdErr
	}

	infolog.Println("advance: finished")
	return err
}

func Handle(payload string) error {
	warnlog.Println("shouldn't be here, wrong resquest type")
	return nil
}

func StartRemoteCartesiRoutine() error {
	innerReadyError := make(chan error)

	errorWaitGroup.Go(func() error {
		return InitializeRemoteCartesi(ctx, innerReadyError)
	})

	err := <-innerReadyError

	return err
}

func main() {
	var help, disableInspect, disableAdvance, resetLatestLink bool

	flag.StringVar(&storePath, "store-path", ".", "Path where data and images are stored")
	flag.StringVar(&imagePath, "image", "image", "Path to the cartesi machine image")
	flag.StringVar(&fromFlashdrivePath, "flash-data-read", "",
		"Path to the flashdrive to insert in the cartesi machine when present")
	flag.StringVar(&storeFlashdrivePath, "flash-data-store", "",
		"Path to the flashdrive to save from the cartesi machine when present")
	flag.BoolVar(&disableAdvance, "disable-advance", false, "Disable advance requests")
	flag.BoolVar(&disableInspect, "disable-inspect", false, "Disable inspect requests")
	flag.BoolVar(&resetLatestLink, "reset-latest", false,
		"Reset latest link (otherwise use latest link target as base image)")
	flag.Float64Var(&delayRemoteTest, "remote-delay", 0.1, 
		"Delay between remote cartesi machine tests")
	flag.Float64Var(&remoteCmInitTimeout, "remote-timeout", 10.0, 
		"Timeout for testing the remote cartesi machine")
	flag.BoolVar(&disableConsistencyChecks, "disable-consistency-checks", false, 
		"Disable assert rollups and root hash checks when starting cm")
	flag.BoolVar(&disableWorkdir, "disable-workdir", false, 
		"Disable copying snapshot to a workdir before starting cm (not recommeded with disable-remote)")
	flag.BoolVar(&disableRemoteCm, "disable-remote", false, 
		"Disable remote cm and do advances without no rollback option (not compatible with inspects)")
	flag.Uint64Var(&saveSnapshotTimeout, "save-snapshot-timeout", 0, 
		"Timeout to do a snapshot after an advance")
	flag.Uint64Var(&saveSnapshotNAdvances, "save-snapshot-batch", 1, 
		"Number of advances to batch before saving snapshots")
	flag.BoolVar(&help, "help", false, "Show this help")

	flag.Parse()

	if help {
		flag.PrintDefaults()
		os.Exit(0)
	}

	if disableRemoteCm && !disableInspect {
		log.Panicln(fmt.Errorf("Can't disable remote CM without disabling inspects"))
	}

	// Setup context
	ctx = context.Background()
	errorWaitGroup, ctx = errgroup.WithContext(ctx)
	ctx, cancel = signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	// Setup cm service paths
	infolog.Println("setting up starting image")
	err := SetupImagePaths(resetLatestLink)
	if err != nil {
		log.Panicln(fmt.Errorf("error setting up paths: %s", err))
	}

	// Start remote cm services
	if !disableRemoteCm {
		err = StartRemoteCartesiRoutine()
		if err != nil {
			if err := errorWaitGroup.Wait(); err != nil {
				warnlog.Println("error in errgroup:", err)
				os.Exit(1)
			}
		}

		// preload cm
		err = PreloadCM()
		if err != nil {
			errorWaitGroup.Go(func() error {
				return fmt.Errorf("preload cm error: %s", err)
			})
			if err := errorWaitGroup.Wait(); err != nil {
				warnlog.Println("error in errgroup:", err)
				os.Exit(1)
			}
			infolog.Println("exiting")
			os.Exit(0)
		}
	}

	// Add handlers and start rollup service
	handler.HandleDefault(Handle)
	if !disableInspect {
		handler.HandleInspect(HandleInspect)
	}
	if !disableAdvance {
		handler.HandleAdvance(HandleAdvance)
	}

	// start rollup service
	infolog.Println("starting rollups")
	errorWaitGroup.Go(func() error {
		return handler.RunContext(ctx)
	})

	// main services processing
	if err := errorWaitGroup.Wait(); err != nil {
		warnlog.Println("error in errgroup:", err)
		os.Exit(1)
	}
	infolog.Println("exiting")

}
