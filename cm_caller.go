package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"log"
	"math"
	"math/big"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/radovskyb/watcher"
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
var waitDelay time.Duration = 10 * time.Second
var remoteCmInitDelayTimeout time.Duration = 1 * time.Second

var cmCommand string = "cartesi-machine"
var remoteCmCommand string = "jsonrpc-remote-cartesi-machine"

var baseRemoteCMPort uint64 = 10000
var remoteCMPort uint64
var remoteCMHost string = "127.0.0.1"
var cmOutput string = "cartesi_machine.out"
var latestLinkPath string = "latest"
var baseImagePath string = "local_image"
var latestBlockPath string = "latest_block"
var latestIndexPath string = "latest_index"
var workingSnapshotDir string = "working_image"
var lastSnapshotTs uint64 = 0
var lastAdvanceWithSnapshot uint64 = 0
var noSnapshotsYet uint64 = 1

var inputFile string = "input-%d.bin"
var queryFile string = "query.bin"

// var queryResponseFile = "query-report-0.bin"
var inputPayloadTyp = abi.MustNewType(
	"tuple(uint64,address,address,uint64,uint64,bytes32,uint64,bytes)")
var voucherTyp = abi.MustNewType("tuple(address,uint256,bytes)")
var bytesTyp = abi.MustNewType("tuple(bytes)")

var imagePath, fromFlashdrivePath, storeFlashdrivePath, storePath, watcherPath string
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

// Notice header bytes 0x415bf363
var evmAdvanceHeader []byte = []byte{65, 91, 243, 99}

// Notice header bytes 0xc258d6e5
var noticeHeader []byte = []byte{194, 88, 214, 229}

// voucher header 0x237a816f
var voucherHeader []byte = []byte{35, 122, 129, 111}

// delegated voucher header 0x10321e8b
var delegatedVoucherHeader []byte = []byte{16, 50, 30, 139}

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
	_, errLink := os.Lstat(filepath.Join(storePath, latestLinkPath))
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
			fileInfo, err := os.Lstat(filepath.Join(storePath, latestLinkPath))
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
						return fmt.Errorf(
							"error getting latest link target: %s", err)
					}
				}
				if removeOldTarget {
					if err := os.RemoveAll(target); err != nil {
						return fmt.Errorf(
							"error removing old link target: %s", err)
					}
				}
			}

			if err := os.Remove(filepath.Join(storePath, latestLinkPath)); err != nil {
				return fmt.Errorf("error removing link: %s", err)
			}
		}

		// remove old latest block file
		if _, err := os.Stat(filepath.Join(storePath, latestBlockPath)); err != nil {
			if !errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("latest block file error: %s", err)
			}
		} else {
			if err := os.Remove(filepath.Join(storePath, latestBlockPath)); err != nil {
				return fmt.Errorf("error removing latest block file: %s", err)
			}
		}

		// remove old latest index file
		if _, err := os.Stat(filepath.Join(storePath, latestIndexPath)); err != nil {
			if !errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("latest index file error: %s", err)
			}
		} else {
			if err := os.Remove(filepath.Join(storePath, latestIndexPath)); err != nil {
				return fmt.Errorf("error removing latest index file: %s", err)
			}
		}

		// copy image path to starting path
		err := copyDir(imagePath, startingImagePath)
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

		err = os.Symlink(strings.TrimPrefix(startingImagePath,
			fmt.Sprintf("%s/", filepath.Join(storePath, ""))),
			filepath.Join(storePath, latestLinkPath))
		if err != nil {
			return fmt.Errorf("error creating latest link: %s", err)
		}
	}

	// check image path
	if _, err := os.Stat(filepath.Join(storePath, latestLinkPath)); err != nil {
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
			return fmt.Errorf("flash drives config unmarshal error: %s", err)
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
	_, err = os.Stat(filepath.Join(storePath, workingSnapshotDir))
	if err == nil {
		if err := os.RemoveAll(filepath.Join(storePath, workingSnapshotDir)); err != nil {
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
	args = append(args, fmt.Sprintf("--server-address=%s:%d", remoteCMHost, remoteCMPort))
	// args = append(args, fmt.Sprintf("--log-level=%s", logLevel))

	cmd := exec.CommandContext(currCtx, command, args...)
	cmd.Stdout = log
	cmd.Stderr = log
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true, Pgid: 0}
	cmd.WaitDelay = time.Second * waitDelay
	cmd.Cancel = func() error {
		log.Close()
		
		if ctx.Err() == nil {
			// Send the terminate signal to the process group 
			//	 by passing the negative pid.
			infolog.Println("remote cm: sent SIGTERM command", command)
			err := syscall.Kill(-cmd.Process.Pid, syscall.SIGTERM)
			if err != nil {
				warnlog.Println("remote cm: failed to send SIGTERM ",
					"command", command, "error", err)
			}

			wg.Wait()
			
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
		conn, err = net.DialTimeout("tcp", 
			fmt.Sprintf("%s:%d", remoteCMHost, remoteCMPort), remoteCmInitDelayTimeout)
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

	target, err := os.Readlink(filepath.Join(storePath, latestLinkPath))
	var workdirPath string
	if !disableWorkdir {
		// Copy target to work dir
		workdirPath = filepath.Join(storePath, workingSnapshotDir)

		if err != nil { // no target
			return "",fmt.Errorf("error getting latest link target: %s",
			err)
		}
		latestPath := filepath.Join(storePath, target)

		err = copyDir(latestPath, workdirPath)
	} else {
		workdirPath = filepath.Join(storePath, target)
	}
	return workdirPath,err
}

func copyDir(pathFrom string, pathTo string) error {
	// remove path to
	if _, err := os.Stat(pathTo); err == nil {
		if err := os.RemoveAll(pathTo); err != nil {
			return fmt.Errorf("error removing pathTo: %s", err)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("image error: %s", err)
	}

	return filepath.Walk(pathFrom,
		func(path string, info os.FileInfo, err error) error {
			var relPath string = strings.TrimPrefix(path, pathFrom)
			if err != nil {
				return err
			}
			if info.IsDir() {
				err = os.Mkdir(pathTo, info.Mode()&os.ModePerm)
				return err
			} else if (info.Mode()&os.ModeSymlink) == os.ModeSymlink {
				target, err := os.Readlink(path)
				if err != nil {
					return err
				}
				if filepath.IsAbs(target) {
					return copyDir(target, pathTo)
				}
				return copyDir(filepath.Join(filepath.Dir(path), target), pathTo)
			} else {
				source, err := os.Open(filepath.Join(pathFrom, relPath))
				if err != nil {
					return err
				}
				defer source.Close()

				destination, err := os.Create(
					filepath.Join(pathTo, relPath))
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

	return fileBytes, nil
}

func HandleInspect(payloadHex string) error {
	infolog.Println("inspect: received")
	// encode query
	data, err := rollups.Hex2Bin(payloadHex)
	if err != nil {
		return fmt.Errorf("error converting hex: %s", err)
	}
	
	err = os.WriteFile(queryFile, data, os.ModePerm)
	if err != nil {
		return fmt.Errorf("error writing file: %s", err)
	}
	defer os.Remove(queryFile)

	command := cmCommand

	args := make([]string, 0)
	if !disableRemoteCm {
		args = append(args, fmt.Sprintf("--remote-address=%s:%d", 
			remoteCMHost, remoteCMPort))
		args = append(args, "--no-remote-create")
		args = append(args, "--no-remote-destroy")
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
	args = append(args, "--cmio-inspect-state")
	// args = append(args, "--quiet")

	infolog.Println("running: ", command, strings.Join(args, " "))
	cmd := exec.Command(command, args...)
	out, err := cmd.CombinedOutput()
	infolog.Printf("\n====\n%s\n====", string(out))
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

	data, err := rollups.Hex2Bin(payloadHex)
	if err != nil {
		return fmt.Errorf("error converting payload to bin: %s", err)
	}
	
	if _, err := os.Stat(filepath.Join(storePath,latestIndexPath)); err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("latest index file error: %s", err)
		}
	} else {
		latestIndexBytes, err := os.ReadFile(filepath.Join(storePath,latestIndexPath))
		if err != nil {
			return fmt.Errorf("latest index reding file error: %s", err)
		}

		latestIndex, err := strconv.Atoi(string(latestIndexBytes))
		if err != nil {
			return fmt.Errorf("latest index converting value error: %s", err)
		}

		if latestIndex >= int(metadata.InputIndex) {
			warnlog.Println("skipping input from index",metadata.InputIndex,"(latest",latestIndex,")")
			return nil
		}
	
	}

	payloadMap := make(map[string]interface{})
	payloadMap["0"] = metadata.ChainId
	payloadMap["1"] = metadata.AppContract
	payloadMap["2"] = metadata.MsgSender
	payloadMap["3"] = metadata.BlockNumber
	payloadMap["4"] = metadata.BlockTimestamp
	payloadMap["5"] = metadata.PrevRandao
	payloadMap["6"] = metadata.InputIndex
	payloadMap["7"] = data

	advancePayload, err := abi.Encode(payloadMap, inputPayloadTyp)
	if err != nil {
		return fmt.Errorf("error encoding payload: %s", err)
	}

	inputPayload := append(evmAdvanceHeader,advancePayload...)

	// save payloadin advance file
	iFile := fmt.Sprintf(inputFile, metadata.InputIndex)
	err = os.WriteFile(iFile, inputPayload, os.ModePerm)
	if err != nil {
		return fmt.Errorf("error writing file: %s", err)
	}
	defer os.Remove(iFile)


	newImagePath := fmt.Sprintf("%s/%s_%d_%d",
		storePath, baseImagePath,
		metadata.InputIndex, metadata.BlockNumber)
	
	storeCurrentAdvance := 
		(saveSnapshotNAdvances > 0 && metadata.InputIndex >= 
				lastAdvanceWithSnapshot + saveSnapshotNAdvances - noSnapshotsYet) ||
		(saveSnapshotTimeout > 0 && metadata.BlockTimestamp >= 
			lastSnapshotTs + saveSnapshotTimeout)

	command := cmCommand

	args := make([]string, 0)
	if !disableRemoteCm {
		args = append(args, fmt.Sprintf("--remote-address=%s:%d", 
			remoteCMHost, remoteCMPort))
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
		"--cmio-advance-state=input_index_begin:%d,input_index_end:%d",
		metadata.InputIndex, metadata.InputIndex+1))

	if storeCurrentAdvance {
		args = append(args, fmt.Sprintf("--store=%s", newImagePath))
		if !disableRemoteCm {
			args = append(args, "--remote-shutdown")
		}
	} else {
		args = append(args, "--no-remote-destroy")
	}
	// args = append(args, "--quiet")

	infolog.Println("running: ", command, strings.Join(args, " "))
	cmd := exec.Command(command, args...)
	out, cmdErr := cmd.CombinedOutput()
	infolog.Printf("\n====\n%s\n====", string(out))

	// Redirect reports
	files, err := filepath.Glob(fmt.Sprintf(
		"input-%d-report-[0-9]*.bin", metadata.InputIndex))
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

	// Redirect outputs
	files, err = filepath.Glob(fmt.Sprintf(
		"input-%d-output-[0-9]*.bin", metadata.InputIndex))
	slices.Sort(files)
	if err != nil {
		return fmt.Errorf("error getting output files: %s", err)
	}
	for _, f := range files {
		// infolog.Println("sending voucher from", f)
		dataOut, err := ReadCMOutput(f)
		if err != nil {
			return fmt.Errorf("error reading query output file: %s", err)
		}

		dataHeader := dataOut[:4]

		switch {
		case bytes.Equal(noticeHeader,dataHeader):

			decoded, err := abi.Decode(bytesTyp, dataOut[4:])
			if err != nil {
				return fmt.Errorf("error decoding voucher: %s", err)
			}

			mapResult, ok := decoded.(map[string]interface{})
			if !ok {
				return fmt.Errorf("convert decoded payload to map error")
			}
			dataBytes, ok1 := mapResult["0"].([]byte)

			if !ok1 {
				return fmt.Errorf("convert decoded to fields")
			}

			_, err = rollups.SendNotice(&rollups.Notice{
				Payload: rollups.Bin2Hex(dataBytes)})
			if err != nil {
				return fmt.Errorf("error making http request: %s", err)
			}

		case bytes.Equal(voucherHeader,dataHeader):

			decoded, err := abi.Decode(voucherTyp, dataOut[4:])
			if err != nil {
				return fmt.Errorf("error decoding voucher: %s", err)
			}

			mapResult, ok := decoded.(map[string]interface{})
			if !ok {
				return fmt.Errorf("convert decoded payload to map error")
			}
			addr, ok1 := mapResult["0"].(abihandler.Address)
			value, ok2 := mapResult["1"].(*big.Int)
			dataBytes, ok3 := mapResult["2"].([]byte)

			if !ok1 || !ok2 || !ok3 {
				return fmt.Errorf("convert decoded to fields")
			}

			_, err = rollups.SendVoucher(
				&rollups.Voucher{Destination: addr.String(),
					Value: value,
					Payload: rollups.Bin2Hex(dataBytes)})
			if err != nil {
				return fmt.Errorf("error making http request: %s", err)
			}
		case bytes.Equal(delegatedVoucherHeader,dataHeader):
			warnlog.Println("not implemented")
		default:
			warnlog.Println("couldn't get corresponding output type")
		}
	}

	// remove extra output files
	files, err = filepath.Glob(fmt.Sprintf("input-%d-output-hahes-root-hash.bin",metadata.InputIndex))
	if err != nil {
		return fmt.Errorf("error getting output hash files: %s", err)
	}
	for _, f := range files {
		err = os.Remove(f)
		if err != nil {
			return fmt.Errorf("error removing output hash file %s: %s", f, err)
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
					return fmt.Errorf(
						"error getting latest link target: %s", err)
				}

				if err := os.RemoveAll(filepath.Join(storePath,target)); err != nil {
					return fmt.Errorf("error removing old link target: %s", err)
				}
			}

			// remove link and create link with new image
			if err := os.Remove(filepath.Join(storePath,latestLinkPath)); err != nil {
				return fmt.Errorf("error removing link: %s", err)
			}
			err = os.Symlink(
				strings.TrimPrefix(newImagePath, 
					fmt.Sprintf("%s/",filepath.Join(storePath,""))), 
					filepath.Join(storePath,latestLinkPath))
			if err != nil {
				return fmt.Errorf("error creating latest link: %s", err)
			}

			lastSnapshotTs = metadata.BlockTimestamp
			lastAdvanceWithSnapshot = metadata.InputIndex
			noSnapshotsYet = 0
		}
		err = os.WriteFile(filepath.Join(storePath,latestBlockPath), 
			[]byte(fmt.Sprintf("%d",metadata.BlockNumber)), os.ModePerm)
		if err != nil {
			return fmt.Errorf("error creating latest block file: %s", err)
		}
		err = os.WriteFile(filepath.Join(storePath,latestIndexPath), 
			[]byte(fmt.Sprintf("%d",metadata.InputIndex)), os.ModePerm)
		if err != nil {
			return fmt.Errorf("error creating latest index file: %s", err)
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
		err = StartRemoteCM(NewRemotePort())
		if err != nil {
			return fmt.Errorf("error starting remote cm: %s", err)
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

func NewRemotePort() uint64 {
	newPort := remoteCMPort + 1
	if newPort >= math.MaxUint16 {
		return baseRemoteCMPort
	}
	return newPort
}
func StartRemoteCM(newPort uint64) error {

	if !disableRemoteCm {
		remoteCMPort = newPort
		err := StartRemoteCartesiRoutine()
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
	return nil
}

func RestartRemoteCM() error {

	if !disableRemoteCm {
		// cancel remote cm
		command := cmCommand

		args := make([]string, 0)
		args = append(args, 
			fmt.Sprintf("--remote-address=%s:%d", remoteCMHost, remoteCMPort))
		args = append(args, "--no-remote-create")
		if disableConsistencyChecks {
			args = append(args, "--skip-root-hash-check")
			args = append(args, "--skip-root-hash-store")
		}
		args = append(args, "--remote-shutdown")
		// args = append(args, "--assert-rolling-template")

		infolog.Println("running: ", command, strings.Join(args, " "))
		cmd := exec.Command(command, args...)
		out, err := cmd.CombinedOutput()
		infolog.Printf("\n====\n%s====", string(out))
		if err != nil {
			return fmt.Errorf("shutdown remote cm error: %s", err)
		}
	}

	infolog.Println("setting up starting image")
	err := SetupImagePaths(true)
	if err != nil {
		log.Panicln(fmt.Errorf("error setting up paths: %s", err))
	}

	if !disableRemoteCm {
		err = StartRemoteCM(NewRemotePort())
		if err != nil {
			return fmt.Errorf("start remote cm error: %s", err)
		}
	}
	return nil
}

func StartWatcher(w *watcher.Watcher, watcherInterval uint64, pathToWatch string) error {

	parent := filepath.Dir(pathToWatch)
	pattern := regexp.MustCompile(filepath.Base(pathToWatch))
	if err := w.Add(parent); err != nil {
		errMsg := fmt.Errorf("watcher add path: %s", err)
		warnlog.Println(errMsg)
		return errMsg
	}
	w.AddFilterHook(watcher.RegexFilterHook(pattern, false))

	errorWaitGroup.Go(func() error {
		for {
			select {
			// Context done
			case <-ctx.Done():
				errMsg := fmt.Errorf("watcher context done: %s", ctx.Err())
				warnlog.Println(errMsg)
				return errMsg
			// Read from Errors.
			case err := <-w.Error:
				warnlog.Println("error in watcher loop", err)
				return err
			// Read from Events.
			case e := <-w.Event:
				
				if pattern.MatchString(e.FileInfo.Name()) && 
					((e.FileInfo.IsDir() && e.Op == watcher.Create) || 
					((e.FileInfo.Mode()&os.ModeSymlink) == os.ModeSymlink &&
					e.Op == watcher.Write)) {
					infolog.Printf("watcher: image changed")

					err := RestartRemoteCM()
					if err != nil {
						return fmt.Errorf("restart remote cm error: %s",err)
					}
				}
			case <-w.Closed:
				infolog.Println("watcher closed")
				return nil
			}
		}
	})

	errorWaitGroup.Go(func() error {
		errCh := make(chan error)
		infolog.Println("starting watcher")

		go func() {
			errCh <- w.Start(time.Millisecond * time.Duration(watcherInterval))
		}()

		select {
			// Context done
			case <-ctx.Done():
				errMsg := fmt.Errorf("watcher starter context done: %s", ctx.Err())
				warnlog.Println(errMsg)
				return errMsg
			case err := <- errCh:
				if err != nil {
					warnlog.Println("error in watcher",err)
				}
			return err
		}
	})
	return nil
}

func main() {
	var help, disableInspect, disableAdvance, resetLatestLink, enableWatcher bool
	var watcherInterval uint64 = 1000

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
		"Disable root hash checks when starting cm and storing")
	flag.BoolVar(&disableWorkdir, "disable-workdir", false, 
		"Disable copying snapshot to a workdir before starting cm " +
		"(not recommended with disable-remote)")
	flag.BoolVar(&disableRemoteCm, "disable-remote", false, 
		"Disable remote cm and do advances without no rollback option " +
		"(not compatible with inspects)")
	flag.Uint64Var(&saveSnapshotTimeout, "save-snapshot-timeout", 0, 
		"Timeout to do a snapshot after an advance")
	flag.Uint64Var(&saveSnapshotNAdvances, "save-snapshot-batch", 1, 
		"Number of advances to batch before saving snapshots")
	flag.BoolVar(&enableWatcher, "enable-watcher", false, 
		"Enables latest link watcher to reload remote cartesi machine")
	flag.StringVar(&watcherPath, "watcher-path", "", 
		"Path where for the watcher watch new images (deafault: image path)")
	flag.Uint64Var(&watcherInterval, "watcher-interval", watcherInterval, 
		"Watcher polling interval")
	flag.Uint64Var(&baseRemoteCMPort, "base-remote-port", baseRemoteCMPort, 
		"Starting remote port")
	flag.StringVar(&cmOutput, "remote-output", cmOutput, 
		"Path to the rmote cartesi machine output")
	flag.BoolVar(&help, "help", false, "Show this help")

	flag.Parse()

	if help {
		flag.PrintDefaults()
		os.Exit(0)
	}

	if enableWatcher && !disableAdvance {
		log.Panicln(fmt.Errorf("Can't enable watcher without disabling advances"))
	}
	
	remoteCMPort = baseRemoteCMPort

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

	if enableWatcher {
		w := watcher.New()
		defer w.Close()

		// w.SetMaxEvents(1)
		w.FilterOps(watcher.Create,watcher.Write)
		
		pathToWatch := imagePath
		if watcherPath != "" {
			pathToWatch = watcherPath
		}

		err = StartWatcher(w,watcherInterval,pathToWatch)
		if err != nil {
			errorWaitGroup.Go(func() error {
				return fmt.Errorf("start watcher error: %s", err)
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
