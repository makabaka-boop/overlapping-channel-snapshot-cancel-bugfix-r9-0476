FROM golang:1.23-alpine AS build
WORKDIR /src
COPY go.mod ./
COPY *.go ./
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags='-s -w' -o /snapshot-lab .

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /snapshot-lab /usr/local/bin/snapshot-lab
EXPOSE 8080
USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/snapshot-lab"]
