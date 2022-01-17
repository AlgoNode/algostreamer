FROM golang:1.17 as build-env

WORKDIR /go/src/app
COPY . /go/src/app

WORKDIR /go/src/app/cmd/algostream
RUN go get
RUN CGO_ENABLED=0 go build -o /go/bin/algostream
RUN strip /go/bin/algostream

FROM gcr.io/distroless/static

COPY --from=build-env /go/bin/algostream /app
CMD ["/app/algostream"]
