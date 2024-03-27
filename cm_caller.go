package main

import (
	"context"
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
	"strings"
	"syscall"
	"time"

	"golang.org/x/sync/errgroup"

	abi "github.com/lynoferraz/abigo"
	"github.com/prototyp3-dev/go-rollups/handler"
	"github.com/prototyp3-dev/go-rollups/handler/abi"
	"github.com/prototyp3-dev/go-rollups/rollups"
)

var infolog = log.New(os.Stderr, "[ info ]  ", log.Lshortfile)
var warnlog = log.New(os.Stderr, "[ warn ]  ", log.Lshortfile)

var dirMode fs.FileMode = 0755
var fileMode fs.FileMode = 0644
var memSplittedFormatSize int = 2
var waitDelay time.Duration = 10

var cmCommand string = "cartesi-machine"
var remoteCmCommand string = "jsonrpc-remote-cartesi-machine"

var remoteCMAddress string = "localhost:8090"
var cmOutput string = "cartesi_machine.out"
var latestLinkPath string = "latest"
var baseImagePath string = "local_image"

var inputFile string = "epoch-%d-input-%d.bin"
var inputMetadataFile string = "epoch-%d-input-metadata-%d.bin"
var queryFile string = "query.bin"

// var queryResponseFile = "query-report-0.bin"
var metadataTyp = abi.MustNewType("tuple(address,uint256,uint256,uint256,uint256)")
var bytesTyp = abi.MustNewType("tuple(bytes)")
var voucherTyp = abi.MustNewType("tuple(address,bytes)")

var imagePath, flashdriveMemDef, flashdrivePath, storePath string

var remoteCmCmd *exec.Cmd
var ctx context.Context
var cancel context.CancelFunc
var errorWaitGroup *errgroup.Group

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

	// set latest link flash drive
	_, errLink := os.Lstat(latestLinkPath)
	if errLink != nil {
		fmt.Println("error", errLink)
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
			fileInfo, err := os.Lstat(latestLinkPath)
			if err != nil {
				return fmt.Errorf("error reading latest link: %s", err)
			}

			// remove old link target
			if fileInfo.Mode()&os.ModeSymlink != 0 {
				target, err := os.Readlink(fileInfo.Name())

				if err != nil {
					return fmt.Errorf("error getting latest link target: %s",
						err)
				}

				if err := os.RemoveAll(target); err != nil {
					return fmt.Errorf("error removing old link target: %s", err)
				}
			}

			if err := os.Remove(latestLinkPath); err != nil {
				return fmt.Errorf("error removing link: %s", err)
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

		err = os.Symlink(startingImagePath, latestLinkPath)
		if err != nil {
			return fmt.Errorf("error creating latest link: %s", err)
		}
	}

	// check image path
	if _, err := os.Stat(latestLinkPath); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("image directory not found")
		}
		return fmt.Errorf("image error: %s", err)
	}

	// check flash drive
	if _, err := os.Stat(
		filepath.Join(
			latestLinkPath, fmt.Sprintf("%s.bin", flashdriveMemDef))); err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("drive error: %s", err)
		}
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

	command := remoteCmCommand

	args := make([]string, 0)
	args = append(args, fmt.Sprintf("--server-address=%s", remoteCMAddress))
	// args = append(args, fmt.Sprintf("--log-level=%s", logLevel))

	cmd := exec.CommandContext(currCtx, command, args...)
	cmd.Stdout = log
	cmd.Stderr = log
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.WaitDelay = time.Second * waitDelay
	cmd.Cancel = func() error {
		log.Close()
		// Send the terminate signal to the process group by passing the negative pid.
		infolog.Println("remote cm: sent SIGTERM command", command)
		err := syscall.Kill(-cmd.Process.Pid, syscall.SIGTERM)
		if err != nil {
			warnlog.Println("remote cm: failed to send SIGTERM ",
				"command", command, "error", err)
		}

		return err
	}
	remoteCmCmd = cmd
	err = remoteCmCmd.Start()
	if err != nil {
		warnlog.Println("remote cm: failed to initialize:", err)
		ready <- err
		return err
	}
	ready <- nil
	infolog.Println("remote cm: ready")
	err = remoteCmCmd.Wait()
	// finishedProcess <- struct{}{}
	if ctx.Err() != nil {
		warnlog.Println("init context error")
		return ctx.Err()
	}
	return err
}

func PreloadCM() error {
	infolog.Println("preloading cm: intializing")

	command := cmCommand

	args := make([]string, 0)
	args = append(args, fmt.Sprintf("--load=%s", latestLinkPath))
	args = append(args, fmt.Sprintf("--remote-address=%s", remoteCMAddress))
	args = append(args, "--remote-protocol=jsonrpc")
	args = append(args, "--no-remote-destroy")
	// args = append(args, "--max-mcycle=0")
	args = append(args, "--rollup")
	args = append(args, "--assert-rolling-template")

	if flashdrivePath != "" {
		if _, err := os.Stat(flashdrivePath); err == nil {
			// TODO; Get the start address and size
			fileInfo, err := os.Stat(fmt.Sprintf("%s/%s.bin",
				latestLinkPath, flashdriveMemDef))
			if err != nil {
				return fmt.Errorf("error getting drive infos: %s", err)
			}

			splittedMem := strings.Split(flashdriveMemDef, "-")
			if len(splittedMem) != memSplittedFormatSize {
				return fmt.Errorf("wrong flasdrive mem format")
			}

			args = append(args,
				fmt.Sprintf(
					"--replace-flash-drive=start:0x%s,length:%d,filename:%s",
					splittedMem[0], fileInfo.Size(), flashdrivePath))
		}
	}

	infolog.Println("running: ", command, strings.Join(args, " "))
	cmd := exec.Command(command, args...)
	out, err := cmd.CombinedOutput()
	infolog.Printf("\n%s", string(out))
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
	args = append(args, fmt.Sprintf("--remote-address=%s", remoteCMAddress))
	args = append(args, "--remote-protocol=jsonrpc")
	args = append(args, "--no-remote-create")
	args = append(args, "--no-remote-destroy")
	args = append(args, "--rollup")
	args = append(args, "--assert-rolling-template")
	args = append(args, "--rollup-inspect-state")
	// args = append(args, "--quiet")

	infolog.Println("running: ", command, strings.Join(args, " "))
	cmd := exec.Command(command, args...)
	out, err := cmd.CombinedOutput()
	infolog.Printf("\n%s", string(out))
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
	infolog.Println("advance: received with metadata: ", metadata)
	payloadMap := make(map[string]interface{})
	data, err := rollups.Hex2Bin(payloadHex)
	if err != nil {
		return fmt.Errorf("error converting payload to bin: %s", err)
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

	command := cmCommand

	args := make([]string, 0)
	args = append(args, fmt.Sprintf("--remote-address=%s", remoteCMAddress))
	args = append(args, "--remote-protocol=jsonrpc")
	args = append(args, "--no-remote-create")
	// args = append(args, "--no-remote-destroy")
	args = append(args, "--rollup")
	args = append(args, "--assert-rolling-template")
	args = append(args, fmt.Sprintf(
		"--rollup-advance-state=epoch_index:%d,input_index_begin:%d,input_index_end:%d",
		metadata.EpochIndex, metadata.InputIndex, metadata.InputIndex+1))
	args = append(args, fmt.Sprintf("--store=%s", newImagePath))
	// args = append(args, "--quiet")

	infolog.Println("running: ", command, strings.Join(args, " "))
	cmd := exec.Command(command, args...)
	out, cmdErr := cmd.CombinedOutput()
	infolog.Printf("\n%s", string(out))

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

	if cmdErr != nil {
		warnlog.Println("error advancing", cmdErr)
		return err
	}

	// copying flash drive
	if flashdrivePath != "" {
		source, err := os.Open(filepath.Join(
			newImagePath, fmt.Sprintf("%s.bin", flashdriveMemDef)))
		if err != nil {
			return err
		}
		defer source.Close()

		destination, err := os.Create(flashdrivePath)
		if err != nil {
			return err
		}
		defer destination.Close()
		_, err = io.Copy(destination, source)
		if err != nil {
			return err
		}
	}

	// read link
	fileInfo, err := os.Lstat(latestLinkPath)
	if err != nil {
		return fmt.Errorf("error reading latest link: %s", err)
	}

	// remove old link target
	if fileInfo.Mode()&os.ModeSymlink != 0 {
		target, err := os.Readlink(fileInfo.Name())

		if err != nil {
			return fmt.Errorf("error getting latest link target: %s", err)
		}

		if err := os.RemoveAll(target); err != nil {
			return fmt.Errorf("error removing old link target: %s", err)
		}
	}

	// remove link and create link with new image
	if err := os.Remove(latestLinkPath); err != nil {
		return fmt.Errorf("error removing link: %s", err)
	}
	err = os.Symlink(newImagePath, latestLinkPath)
	if err != nil {
		return fmt.Errorf("error creating latest link: %s", err)
	}

	err = remoteCmCmd.Cancel()
	if err != nil {
		defer cancel()
		return fmt.Errorf("remote cm: failed to cancel: %s", err)
	}

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
	flag.StringVar(&flashdriveMemDef, "flash-mem", "0090000000000000-8000000",
		"Flashdrive mememory definition to identify the cartesi machine drive")
	flag.StringVar(&flashdrivePath, "flash-data", "",
		"Path to the flashdrive to save and insert in the cartesi machine when present")
	flag.BoolVar(&disableAdvance, "no-advance", false, "Disable advance requests")
	flag.BoolVar(&disableInspect, "no-inspect", false, "Disable inspect requests")
	flag.BoolVar(&resetLatestLink, "reset-latest", false,
		"Reset latest link (otherwise use latest link target as base image)")
	flag.BoolVar(&help, "help", false, "Show this help")

	flag.Parse()

	if help {
		flag.PrintDefaults()
		os.Exit(0)
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
		log.Panicln(fmt.Errorf("error setuping paths: %s", err))
	}

	// Start cm services
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
		return handler.RunDebugContext(ctx)
	})

	// main services processing
	if err := errorWaitGroup.Wait(); err != nil {
		warnlog.Println("error in errgroup:", err)
		os.Exit(1)
	}
	infolog.Println("exiting")

}
