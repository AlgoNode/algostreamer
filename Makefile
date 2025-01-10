cmd/algostream/algostream: 
	cd cmd/algostream && go build && strip algostream

update-submodule:
	git submodule update --remote

.PHONY: cmd/algostream/algostream go-algorand
