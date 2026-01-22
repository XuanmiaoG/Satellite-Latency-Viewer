# Satellite Latency Pipeline (Project Overview)

This repo groups three related components used in the satellite latency pipeline. The main logic and web tooling live in `latency-viewer`, with `rt_latency` providing the runtime wrapper/daemon packaging and `quickmq` supplying the AMQP publish layer (optimized here for the pipeline).

## What's inside

- `latency-viewer` — Core latency tooling and the Satellite Latency Viewer utilities (relationship generation, CGI scripts, and helper modules).
- `rt_latency` — Runtime wrapper for the real-time pipeline (daemon, deployment, and packaging details around `sat_latency`).
- `quickmq` — AMQP/RabbitMQ publisher library used by the pipeline; optimized here for reliability and delivery guarantees.
- `demo.jpeg` — Visual reference for the demo.

## How these fit together

1. `quickmq` publishes satellite events reliably to RabbitMQ.
2. `rt_latency` consumes events and updates the latency data store.
3. `latency-viewer` provides the web-facing utilities and relationship generation to visualize and inspect latency data.

## Start here

Each component is self-contained and has its own README:

- `latency-viewer/README.md`
- `rt_latency/README.md`
- `quickmq/README.md`

## Ownership

- `latency-viewer` is authored and maintained by Yang Gao.
- `rt_latency` is the pipeline wrapper/integration maintained by Yang Gao.
- `quickmq` is optimized here for this pipeline and is maintained as part of this project.

## Notes

- All components expect Python 3.9+ unless otherwise noted in their individual READMEs.
- For system setup, permissions, and CGI configuration, follow the instructions in `latency-viewer/README.md`.

## Demo

<img src="demo.jpeg" alt="Satellite latency demo" width="900" />
