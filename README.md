<a id="readme-top"></a>

[![Contributors][contributors-shield]][contributors-url]
[![Forks][forks-shield]][forks-url]
[![Stargazers][stars-shield]][stars-url]
[![Issues][issues-shield]][issues-url]
[![Apache 2.0 License][license-shield]][license-url]

<br />
<div align="center">
  <h1>pubsubfx</h1>
  <p>
    Uber Fx module for publish/subscribe messaging with in-process and Redis backends selected by URL.
  </p>
  <a href="https://github.com/go-core-fx/pubsubfx/issues/new">Report Bug</a>
  &middot;
  <a href="https://github.com/go-core-fx/pubsubfx/issues/new">Request Feature</a>
</div>

## Table of Contents
- [Table of Contents](#table-of-contents)
- [About The Project](#about-the-project)
  - [Built With](#built-with)
- [Getting Started](#getting-started)
  - [Prerequisites](#prerequisites)
  - [Installation](#installation)
- [Usage](#usage)
  - [Module Setup](#module-setup)
  - [Configuration Reference](#configuration-reference)
  - [Direct Construction](#direct-construction)
  - [Backends](#backends)
  - [Errors](#errors)
- [Development](#development)
  - [Make Targets](#make-targets)
  - [Continuous Integration](#continuous-integration)
- [Contributing](#contributing)
- [License](#license)
- [Acknowledgments](#acknowledgments)

---

## About The Project

`pubsubfx` is an [Uber Fx](https://uber-go.github.io/fx/) module that provides a `pubsub.PubSub` for fan-out messaging. It supports:

- **URL-selected backends** — `memory://` for in-process messaging, `redis://` for cross-process messaging through a Redis server
- **Fan-out delivery** — every subscriber of a topic receives every published message
- **Per-subscription buffering** — `BufferSize` caps how far a subscriber may lag before `Publish` blocks (memory backend)
- **Topic namespacing** — `Prefix` isolates applications sharing one Redis server
- **Fail-fast startup** — an invalid URL or scheme fails app startup, not first publish
- **Close on stop** — the Fx module closes the instance during graceful shutdown

### Built With

- [![Go](https://img.shields.io/badge/Go-00ADD8?style=for-the-badge&logo=go&logoColor=white)](https://go.dev/)
- [![Uber Fx](https://img.shields.io/badge/Uber%20Fx-000000?style=for-the-badge)](https://uber-go.github.io/fx/)
- [![go-redis](https://img.shields.io/badge/github.com%2Fredis%2Fgo--redis-DC382D?style=for-the-badge&logo=redis&logoColor=white)](https://redis.uptrace.dev/)

<p align="right">(<a href="#readme-top">back to top</a>)</p>

---

## Getting Started

### Prerequisites

- Go 1.26+ (`go.mod` requires `go 1.26.0`)
- An application using [Uber Fx](https://uber-go.github.io/fx/) for dependency injection (for `Module()`; the package also works without Fx)

### Installation

```sh
go get github.com/go-core-fx/pubsubfx@latest
```

Until the first tag is cut, pin a commit instead:

```sh
go get github.com/go-core-fx/pubsubfx@<commit-sha>
```

<p align="right">(<a href="#readme-top">back to top</a>)</p>

---

## Usage

### Module Setup

```go
import (
    "github.com/go-core-fx/logger"
    "github.com/go-core-fx/pubsubfx"
    "go.uber.org/fx"
)

func main() {
    fx.New(
        logger.Module(),
        fx.Provide(func() pubsubfx.Config {
            return pubsubfx.Config{
                URL:        "redis://localhost:6379/0",
                BufferSize: 64,
                Prefix:     "myapp:",
            }
        }),
        pubsubfx.Module(),
        // ... other modules
    ).Run()
}
```

`Module()` provides `pubsub.PubSub` for the `Config` in the container, decorates the `*zap.Logger` with `pubsubfx`, and closes the instance when the app stops. An invalid `URL` or scheme makes startup fail with `pubsubfx.ErrInvalidConfig`. The container must already contain a `*zap.Logger` — `logger.Module()` from [`go-core-fx/logger`](https://github.com/go-core-fx/logger) provides one.

Consumers inject the interface:

```go
func Handler(ps pubsub.PubSub) error {
    sub, err := ps.Subscribe(context.Background(), "events")
    if err != nil {
        return err
    }
    defer sub.Close()

    go func() {
        for msg := range sub.Receive() {
            log.Printf("%s: %s", msg.Topic, msg.Data)
        }
    }()

    return ps.Publish(context.Background(), "events", []byte("hello"))
}
```

### Configuration Reference

| Field        | Type     | Default     | Description                                                                                                                             |
| ------------ | -------- | ----------- | --------------------------------------------------------------------------------------------------------------------------------------- |
| `URL`        | `string` | `""`        | Backend URL. Empty defaults to `memory://`. `redis://` connects to Redis; any other scheme fails with `ErrInvalidConfig`.                |
| `BufferSize` | `uint`   | `0`         | Capacity of the channel behind each subscription. `0` makes the memory backend's `Publish` block until every subscriber has accepted.    |
| `Prefix`     | `string` | `""`        | Prepended to every topic name (a trailing `:` is added automatically). Isolates apps sharing one Redis server. Ignored by `memory://`.   |

### Direct Construction

The package works without Fx:

```go
ps, err := pubsubfx.New(pubsubfx.Config{
    URL:    "memory://",
    Prefix: "myapp:",
})
if err != nil {
    log.Fatal(err)
}
defer ps.Close()
```

Individual backends from the [`pubsub`](./pubsub) package can be built directly — `pubsub.NewMemory(opts...)` and `pubsub.NewRedis(pubsub.RedisConfig{...}, opts...)` — with `pubsub.WithBufferSize(n)` for buffering.

### Backends

**`memory://`** — in-process fan-out. `Publish` blocks until every subscriber has received the message or a context is cancelled; `BufferSize` gives subscribers slack so a slow consumer does not stall publishers. Topics are scoped per instance; `Prefix` is ignored.

**`redis://`** — publishes through a Redis server; subscribers receive via Redis pub/sub, so messaging works across processes. `Prefix` namespaces the topics. The Fx module and `New` create and own the Redis client; a client passed directly to `pubsub.NewRedis` stays owned by the caller and is never closed by the pubsub.

### Errors

| Error                          | Meaning                                                            |
| ------------------------------ | ------------------------------------------------------------------ |
| `pubsubfx.ErrInvalidConfig`    | `Config.URL` is unparseable or selects an unknown scheme           |
| `pubsub.ErrInvalidConfig`      | `RedisConfig` has neither a client nor a URL                       |
| `pubsub.ErrPubSubClosed`       | Operation on a closed pubsub instance                              |
| `pubsub.ErrInvalidTopic`       | Empty topic name                                                   |

<p align="right">(<a href="#readme-top">back to top</a>)</p>

---

## Development

### Make Targets

| Target           | Description                                                            |
| ---------------- | ---------------------------------------------------------------------- |
| `make all`       | Run all checks: `fmt`, `lint`, `coverage`                              |
| `make version`   | Display the current version (from `git describe`, defaults to `0.0.0`) |
| `make fmt`       | Run `go generate ./...`, then `golangci-lint fmt`                      |
| `make lint`      | Run `golangci-lint run --timeout=5m`                                   |
| `make test`      | Run tests with race detector, shuffle and atomic coverage              |
| `make coverage`  | Produce `coverage.out` and an HTML report (`coverage.html`)            |
| `make benchmark` | Run benchmarks into `benchmark.txt`                                    |
| `make deps`      | Download module dependencies                                           |
| `make clean`     | Remove coverage output, benchmarks and build artifacts                 |
| `make help`      | List all available targets                                             |

### Continuous Integration

The [Go workflow](.github/workflows/go.yml) runs on pushes to `master` and on pull requests (path-filtered to Go sources, `go.mod`/`go.sum`, the lint config and the workflows themselves):

- **Lint** — golangci-lint with the repository config
- **Test** — race-enabled, shuffled tests with coverage uploaded to [Codecov](https://codecov.io/); the Redis tests run against a `redis:8-alpine` testcontainer and fail the job if Docker is unavailable in CI
- **Benchmark** — runs `go test -bench`, keeps history in the Actions cache and only alerts when benchmarks exist

<p align="right">(<a href="#readme-top">back to top</a>)</p>

---

## Contributing

Contributions are what make the open source community such an amazing place to learn, inspire, and create. Any contributions you make are **greatly appreciated**.

1. Fork the Project
2. Create your Feature Branch (`git checkout -b feature/AmazingFeature`)
3. Commit your Changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the Branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

<p align="right">(<a href="#readme-top">back to top</a>)</p>

---

## License

Distributed under the Apache License 2.0. See `LICENSE` for more information.

<p align="right">(<a href="#readme-top">back to top</a>)</p>

---

## Acknowledgments

- [Uber Fx](https://uber-go.github.io/fx/) — dependency injection framework
- [go-redis](https://github.com/redis/go-redis) — Redis client used by the `redis://` backend
- [Best-README-Template](https://github.com/othneildrew/Best-README-Template) — README structure

<p align="right">(<a href="#readme-top">back to top</a>)</p>

<!-- MARKDOWN LINKS & IMAGES -->
[contributors-shield]: https://img.shields.io/github/contributors/go-core-fx/pubsubfx.svg?style=for-the-badge
[contributors-url]: https://github.com/go-core-fx/pubsubfx/graphs/contributors
[forks-shield]: https://img.shields.io/github/forks/go-core-fx/pubsubfx.svg?style=for-the-badge
[forks-url]: https://github.com/go-core-fx/pubsubfx/network/members
[stars-shield]: https://img.shields.io/github/stars/go-core-fx/pubsubfx.svg?style=for-the-badge
[stars-url]: https://github.com/go-core-fx/pubsubfx/stargazers
[issues-shield]: https://img.shields.io/github/issues/go-core-fx/pubsubfx.svg?style=for-the-badge
[issues-url]: https://github.com/go-core-fx/pubsubfx/issues
[license-shield]: https://img.shields.io/github/license/go-core-fx/pubsubfx.svg?style=for-the-badge
[license-url]: https://github.com/go-core-fx/pubsubfx/blob/master/LICENSE
