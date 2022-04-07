# syntax=docker/dockerfile:1

##
## Build
##
FROM golang:1.18-bullseye AS build

WORKDIR /app

COPY go.mod ./
COPY go.sum ./

RUN go mod download

COPY . ./

RUN go build -o /s3_downloader

##
## Deploy
##
FROM gcr.io/distroless/base-debian10

WORKDIR /

USER nonroot:nonroot

COPY --from=build /s3_downloader /s3_downloader

ENTRYPOINT ["/s3_downloader"]