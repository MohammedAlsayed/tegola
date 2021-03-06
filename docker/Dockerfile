# To build, run in root of tegola source tree:
#  1. git clone git@github.com:go-spatial/tegola.git or git clone https://github.com/go-spatial/tegola.git
#  2. cd tegola
#  3. docker build -f docker/Dockerfile -t gospatial/tegola:<version> .
#
# To use with local files, add file data sources (i.e. Geopackages) and config as config.toml to a
#	local directory and mount that directory as a volume at /opt/tegola_config/.  Examples:
#
# To display command-line options available:
#  1. `docker run --rm tegola /opt/tegola -h`
#
# Example PostGIS use w/ http-based config:
#  1. `docker run -p 8080 tegola /opt/tegola --config http://my-domain.com/config serve`
#
# Example PostGIS use w/ local config:
#  1. `mkdir docker-config`
#  2. `cp my-config-file docker-config/config.toml`
#  3. `docker run -v /path/to/docker-config:/opt/tegola_config -p 8080 tegola serve`
#
# Example gpkg use:
#  1. `mkdir docker-config`
#  2. `cp my-config-file docker-config/config.toml`
#  3. `cp my-db.gpkg docker-config/`
#  4. update docker-config/config.toml with my-db.gpkg located at /opt/tegola_config/my-db.gpkg
#  5. `docker run -v /path/to/docker-config:/opt/tegola_config -p 8080 tegola serve`


# --- Build the binary
FROM golang:1.9.2-alpine3.7 AS build

# Only needed for CGO support at time of build, results in no noticable change in binary size
#  incurs approximately 1:30 extra build time (1:54 vs 0:27) to install packages.  Doesn't impact
#  development as these layers are drawn from cache after the first build.
RUN apk update
RUN apk add musl-dev=1.1.18-r2
RUN apk add gcc=6.4.0-r5

# Set up source for compilation
RUN mkdir -p /opt/tegola_src
COPY . /opt/tegola_src
RUN rm -rf /opt/tegola_src/src
RUN mkdir -p /opt/tegola_src/src/github.com/go-spatial
RUN ln -s /opt/tegola_src /opt/tegola_src/src/github.com/go-spatial/tegola

# Build binary
WORKDIR /opt/tegola_src/cmd/tegola
ENV GOPATH=/opt/tegola_src
RUN go build -v -gcflags "-N -l" -o /opt/tegola
RUN chmod a+x /opt/tegola

# --- Create minimal deployment image, just alpine & the binary
FROM alpine:3.7
LABEL maintainer="Development@JivanAmara.net"
LABEL io.gospatial.version="0.6.0_beta"
COPY --from=build /opt/tegola /opt/
CMD ["/opt/tegola", "--config", "/opt/tegola_config/config.toml", "serve"]
