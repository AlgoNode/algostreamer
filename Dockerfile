FROM golang:1.18 as build-env

RUN apt update && apt install libsqlite3-dev 
WORKDIR /go/src/app
COPY . /go/src/app

WORKDIR /go/src/app/cmd/algostream
RUN go get
RUN CGO_ENABLED=1 GOOS=linux GOARCH=amd64 go build -o /go/bin/algostream
RUN strip /go/bin/algostream

FROM gcr.io/distroless/static

COPY --from=build-env /go/bin/algostream /app/
CMD ["/app/algostream"]
