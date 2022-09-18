// Copyright (C) 2022 AlgoNode Org.
//
// algostreamer is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// algostreamer is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with algostreamer.  If not, see <https://www.gnu.org/licenses/>.

package utils

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/algorand/go-codec/codec"

	"github.com/tidwall/jsonc"
)

type eternalFn func(ctx context.Context) error

func Backoff(ctx context.Context, fn eternalFn, timeout time.Duration, wait time.Duration, maxwait time.Duration) error {
	//Loop until Algoverse gets cancelled
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		cctx, cancel := context.WithTimeout(ctx, timeout)
		err := fn(cctx)
		if err == nil { // Success
			cancel()
			return nil
		}
		cancel()
		fmt.Fprintf(os.Stderr, err.Error())

		//keep an eye on cancellation while backing off
		if wait > 0 {
			select {
			case <-ctx.Done():
			case <-time.After(wait):
			}
			if maxwait > 0 {
				wait *= 2
				if wait > maxwait {
					wait = maxwait
				}
			}
		}
	}
}

func LoadJSONCFromFile(filename string, object interface{}) (err error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return err
	}
	return json.Unmarshal(jsonc.ToJSON(data), &object)
}

func EncodeJson(obj interface{}) ([]byte, error) {
	//	enc := codec.NewEncoderBytes(&output, protocol.JSONHandle)
	output, err := json.Marshal(obj)
	if err != nil {
		return nil, fmt.Errorf("failed to encode object: %v", err)
	}
	return output, nil
}

// PrintableUTF8OrEmpty checks to see if the entire string is a UTF8 printable string.
// If this is the case, the string is returned as is. Otherwise, the empty string is returned.
func PrintableUTF8OrEmpty(in string) string {
	// iterate throughout all the characters in the string to see if they are all printable.
	// when range iterating on go strings, go decode each element as a utf8 rune.
	for _, c := range in {
		// is this a printable character, or invalid rune ?
		if c == utf8.RuneError || !unicode.IsPrint(c) {
			return ""
		}
	}
	return in
}

// KeysStringBool returns all of the keys in the map joined by a comma.
func KeysStringBool(m map[string]bool) string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return strings.Join(keys, ", ")
}

// MaybeFail exits if there was an error.
func MaybeFail(err error, errfmt string, params ...interface{}) {
	if err == nil {
		return
	}
	fmt.Fprintf(os.Stderr, errfmt, params...)
	fmt.Fprintf(os.Stderr, "\nError: %v\n", err)
	os.Exit(1)
}

var oneLineJSONCodecHandle *codec.JsonHandle

// JSONOneLine converts an object into JSON
func JSONOneLine(obj interface{}) string {
	var b []byte
	enc := codec.NewEncoderBytes(&b, oneLineJSONCodecHandle)
	enc.MustEncode(obj)
	return string(b)
}

func init() {
	oneLineJSONCodecHandle = new(codec.JsonHandle)
	oneLineJSONCodecHandle.ErrorIfNoField = true
	oneLineJSONCodecHandle.ErrorIfNoArrayExpand = true
	oneLineJSONCodecHandle.Canonical = true
	oneLineJSONCodecHandle.RecursiveEmptyCheck = true
	oneLineJSONCodecHandle.HTMLCharsAsIs = true
	oneLineJSONCodecHandle.Indent = 0
	oneLineJSONCodecHandle.MapKeyAsString = true
}
