cmd/algostream/algostream: go-algorand
	cd cmd/algostream && go build && strip algostream

go-algorand:
	git submodule update --init && cd third_party/go-algorand && \
		make crypto/libs/linux/amd64/lib/libsodium.a

update-submodule:
	git submodule update --remote

.PHONY: cmd/algostream/algostream go-algorand
