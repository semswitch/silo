package expose

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"
)

const dispatchTestTimeout = 2 * time.Second

type dispatchTestConnection struct {
	input          []byte
	readSizes      []int
	position       int
	readIndex      int
	zeroLengthRead bool
	output         bytes.Buffer
}

func (connection *dispatchTestConnection) Read(destination []byte) (int, error) {
	if len(destination) == 0 {
		connection.zeroLengthRead = true
		return 0, errors.New("zero-length read")
	}
	if connection.position == len(connection.input) {
		return 0, io.EOF
	}

	readLength := len(destination)
	if connection.readIndex < len(connection.readSizes) {
		readLength = min(readLength, connection.readSizes[connection.readIndex])
		connection.readIndex++
	}
	readLength = min(readLength, len(connection.input)-connection.position)
	copy(destination, connection.input[connection.position:connection.position+readLength])
	connection.position += readLength
	return readLength, nil
}

func (connection *dispatchTestConnection) Write(data []byte) (int, error) {
	return connection.output.Write(data)
}

func (connection *dispatchTestConnection) Close() error { return nil }

type dispatchTestWrite struct {
	offset  int64
	payload []byte
}

type dispatchTestProvider struct {
	mutex  sync.Mutex
	writes []dispatchTestWrite
}

func (provider *dispatchTestProvider) ReadAt([]byte, int64) (int, error) {
	return 0, errors.New("unexpected read")
}

func (provider *dispatchTestProvider) WriteAt(payload []byte, offset int64) (int, error) {
	provider.mutex.Lock()
	defer provider.mutex.Unlock()
	provider.writes = append(provider.writes, dispatchTestWrite{
		offset:  offset,
		payload: bytes.Clone(payload),
	})
	return len(payload), nil
}

func (provider *dispatchTestProvider) Size() uint64              { return 1 << 30 }
func (provider *dispatchTestProvider) Flush() error              { return nil }
func (provider *dispatchTestProvider) Close() error              { return nil }
func (provider *dispatchTestProvider) CancelWrites(int64, int64) {}

func nbdWriteRequest(handle, offset uint64, payload []byte) []byte {
	request := make([]byte, 28+len(payload))
	binary.BigEndian.PutUint32(request, NBDRequestMagic)
	binary.BigEndian.PutUint32(request[4:8], NBDCmdWrite)
	binary.BigEndian.PutUint64(request[8:16], handle)
	binary.BigEndian.PutUint64(request[16:24], offset)
	binary.BigEndian.PutUint32(request[24:28], uint32(len(payload)))
	copy(request[28:], payload)
	return request
}

func nbdDisconnectRequest() []byte {
	request := make([]byte, 28)
	binary.BigEndian.PutUint32(request, NBDRequestMagic)
	binary.BigEndian.PutUint32(request[4:8], NBDCmdDisconnect)
	return request
}

func runDispatchTest(t *testing.T, input []byte, readSizes []int) (*dispatchTestConnection, *dispatchTestProvider, error) {
	t.Helper()
	connection := &dispatchTestConnection{input: input, readSizes: readSizes}
	provider := &dispatchTestProvider{}
	dispatch := NewDispatch(context.Background(), "test", nil, connection, provider)
	dispatch.asyncWrites = false

	result := make(chan error, 1)
	go func() { result <- dispatch.Handle() }()
	select {
	case err := <-result:
		return connection, provider, err
	case <-time.After(dispatchTestTimeout):
		t.Fatal("Dispatch.Handle stalled")
		return nil, nil, nil
	}
}

func assertDispatchWrites(t *testing.T, provider *dispatchTestProvider, expected []dispatchTestWrite) {
	t.Helper()
	if len(provider.writes) != len(expected) {
		t.Fatalf("provider received %d writes, want %d", len(provider.writes), len(expected))
	}
	for index := range expected {
		if provider.writes[index].offset != expected[index].offset {
			t.Errorf("write %d offset = %d, want %d", index, provider.writes[index].offset, expected[index].offset)
		}
		if !bytes.Equal(provider.writes[index].payload, expected[index].payload) {
			t.Errorf("write %d payload differs", index)
		}
	}
}

func assertNBDResponses(t *testing.T, output []byte, handles ...uint64) {
	t.Helper()
	if len(output) != 16*len(handles) {
		t.Fatalf("response length = %d, want %d", len(output), 16*len(handles))
	}
	for index, handle := range handles {
		response := output[index*16 : (index+1)*16]
		if binary.BigEndian.Uint32(response) != NBDResponseMagic {
			t.Errorf("response %d has invalid magic", index)
		}
		if binary.BigEndian.Uint32(response[4:8]) != 0 {
			t.Errorf("response %d returned an error", index)
		}
		if binary.BigEndian.Uint64(response[8:16]) != handle {
			t.Errorf("response %d handle = %d, want %d", index, binary.BigEndian.Uint64(response[8:16]), handle)
		}
	}
}

func TestDispatchHandleWrites(t *testing.T) {
	largePayload := bytes.Repeat([]byte{0x5a}, dispatchBufferSize)
	tests := []struct {
		name      string
		requests  []dispatchTestWrite
		readSizes []int
	}{
		{name: "normal request", requests: []dispatchTestWrite{{offset: 17, payload: []byte("small payload")}}},
		{name: "four MiB payload", requests: []dispatchTestWrite{{offset: 4096, payload: largePayload}}},
		{
			name:      "fragmented large request",
			requests:  []dispatchTestWrite{{offset: 8192, payload: largePayload}},
			readSizes: []int{11, 17, 1024, dispatchBufferSize - 1052, 28},
		},
		{
			name: "multiple requests",
			requests: []dispatchTestWrite{
				{offset: 32, payload: []byte("first")},
				{offset: 64, payload: []byte("second")},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			input := make([]byte, 0)
			handles := make([]uint64, 0, len(test.requests))
			for index, request := range test.requests {
				handle := uint64(index + 1)
				input = append(input, nbdWriteRequest(handle, uint64(request.offset), request.payload)...)
				handles = append(handles, handle)
			}
			input = append(input, nbdDisconnectRequest()...)

			connection, provider, err := runDispatchTest(t, input, test.readSizes)
			if err != nil {
				t.Fatalf("Dispatch.Handle returned %v", err)
			}
			if connection.zeroLengthRead {
				t.Fatal("Dispatch.Handle called Read with a zero-length slice")
			}
			assertDispatchWrites(t, provider, test.requests)
			assertNBDResponses(t, connection.output.Bytes(), handles...)
		})
	}
}

func TestDispatchHandleRejectsOversizedRequest(t *testing.T) {
	header := nbdWriteRequest(1, 0, nil)
	binary.BigEndian.PutUint32(header[24:28], uint32(maxDispatchBufferSize))
	input := bytes.Clone(header)
	input = append(input, make([]byte, dispatchBufferSize-len(header))...)

	connection, provider, err := runDispatchTest(t, input, nil)
	if err == nil || !strings.Contains(err.Error(), "exceeds maximum supported size") {
		t.Fatalf("Dispatch.Handle error = %v, want maximum-size error", err)
	}
	if connection.zeroLengthRead || len(provider.writes) != 0 {
		t.Fatal("oversized request reached a zero-length read or provider write")
	}
}

func TestDispatchHandleRejectsInvalidMagicAtGrowthBoundary(t *testing.T) {
	input := nbdWriteRequest(1, 0, bytes.Repeat([]byte{0x2a}, dispatchBufferSize))
	binary.BigEndian.PutUint32(input, 0xdeadbeef)

	connection, provider, err := runDispatchTest(t, input, nil)
	if err == nil || !strings.Contains(strings.ToLower(err.Error()), "invalid magic") {
		t.Fatalf("Dispatch.Handle error = %v, want invalid-magic error", err)
	}
	if connection.zeroLengthRead || len(provider.writes) != 0 {
		t.Fatal("invalid request reached a zero-length read or provider write")
	}
}
