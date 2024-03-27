# Cm Caller

```
Cartesi Rollups Node version: 1.2.x (sunodo version 0.10.x)
```

A cartesi rollups app that runs cartesi machine containing a rollup app. It stores the image after each advance and allow for selectively disabling the advances or inspects. It is useful to create reader nodes for cartesi rollups.

## Requirements

To run locally you'll need the [cartesi machine](https://github.com/cartesi/machine-emulator), so it is recommended to use it with a docker image which already contains it such as [sunodo/sdk](https://hub.docker.com/r/sunodo/sdk/tags).

## Usage

To test it you could use [nonodo fork](https://github.com/lynoferraz/nonodo) (fork from [nonodo](https://github.com/gligneul/nonodo)), which you can control the active components.

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
