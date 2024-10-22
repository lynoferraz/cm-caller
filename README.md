# Cm Caller

```
Nonodo version: v2.10.x and Cartesi Machine version 0.19.x
```

A cartesi rollups app that runs cartesi machine containing a rollup app. It stores the image after each advance and allow for selectively disabling the advances or inspects. It is useful to create reader nodes for cartesi rollups.

## Requirements

To run you'll need the [Cartesi Machine](https://github.com/cartesi/machine-emulator) as well as [Nonodo](https://github.com/Calindra/nonodo/tags). Optionally, you could use the provided Dockerfile to build a container and run it.

## Create a Cartesi Rollups image

You could generate an image with the [Cartesi cli](https://github.com/cartesi/cli) or you can generate an image direcly with cartesi machine.

The following commands assumes you have cartesi machine on your system. Alternatively, you might want to build a docker container with all required packeges and run in interactive mode:

```shell
docker build --target base -t cm-caller-base .
docker run -it --rm -v $PWD:/workdir -w /workdir cm-caller-base bash
```

First, you should start off from a base rootfs, either the one installed with cartesi machine (`/share/cartesi-machine/images/rootfs.ext2`) or anyone generated with cartesi cli (`/path/to/app/.cartesi/image.ext2`). Copy the base image to a working dir so you can start making changes. 

```shell
cp /path/to/rootfs.ext2 rootfs.ext2
```

### Start from a rootfs.ext2 and prepare image

Before you install you app in the image, you should prepare and install any dependencies. Start the cartesi machine in interactive mode with network and volumes virtio:

```shell
cp /path/to/rootfs.ext2 rootfs.ext2
```

This rootfs.ext2` is your working image. Then, you should resize as you see necessary

```shell
resize2fs -f rootfs.ext2 128M
```

Start the cartesi machine in interactive mode with network and volumes virtio:

```shell
cartesi-machine --network --volume=.:/mnt --workdir=/mnt --flash-drive=label:root,filename:rootfs.ext2,shared -u=root -it -- bash
```

Now that you are inside the cartesi machine, install any packages required ti run your application. Also, copy your projects files to its final dir (we'll consider it is a single `app` binary)

```shell
root@localhost:/mnt# mkdir -p /opt/cartesi/app     
root@localhost:/mnt# cp app /opt/cartesi/app/.
```

### Run in rollups mode and generate the starting snapshot

With the rootfs in place, you can start the cartesi machine in rollups mode and generate the starting snapshot

```shell
cartesi-machine --env=ROLLUP_HTTP_SERVER_URL=http://127.0.0.1:5004 --workdir=/opt/cartesi/app --flash-drive=label:root,filename:rootfs.ext2 --store=image --assert-rolling-template -- rollup-init /opt/cartesi/app/app
```

The starting snapshot was saved to `image` directory. This snapshot is used by cm caller to run you application

## Usage

To test it you could use [nonodo](https://github.com/Calindra/nonodo),which you can control the active components.

Note: you should have a generated cartesi rollup image.

Display help

```shell
./cm-caller -help
```

Start nonono and cm-caller with custom store path and no inspects

```shell
nonodo -- ./cm-caller --store-path=data --flash-data=data/data.ext2 --disable-inspect
```

Start nonono and cm-caller without advances (inspect-only node)

```shell
nonodo --disable-advance --disable-devnet -- ./cm-caller --disable-advance
```

## version Campatibility

The cm-caller version depends on the [cartesi machine emulator](https://github.com/cartesi/machine-emulator) version

| Version  | Emulator Version | Nonodo Version |
| -------- | ------- | ------- |
| v0.0.1 | v0.15.x | v1.x.x |
| v0.1.0 | v0.16.x | v1.x.x |
| v0.2.0 | v0.19.x | v2.10.x+ |
