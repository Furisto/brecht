package log

import (
	"bufio"
	"encoding/binary"
	"os"
	"sync"
)

var encoding = binary.BigEndian

const lenWidth = 8

type store struct {
	*os.File
	mu   sync.Mutex
	buf  *bufio.Writer
	size uint64
}

func newStore(f *os.File) (*store, error) {
	stat, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}

	size := uint64(stat.Size())
	return &store{
		File: f,
		size: size,
		buf:  bufio.NewWriter(f),
	}, nil
}

func (s *store) Append(data []byte) (entryLength uint64, entryPosition uint64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	position := s.size
	if err := binary.Write(s.buf, encoding, uint64(len(data))); err != nil {
		return 0, 0, err
	}

	w, err := s.buf.Write(data)
	if err != nil {
		return 0, 0, err
	}

	w += lenWidth
	s.size += uint64(w)
	return uint64(w), position, nil
}

func (s *store) Read(position uint64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.buf.Flush(); err != nil {
		return nil, err
	}

	entryLength := make([]byte, lenWidth)
	if _, err := s.File.ReadAt(entryLength, int64(position)); err != nil {
		return nil, err
	}

	entryBuf := make([]byte, encoding.Uint64(entryLength))
	if _, err := s.File.ReadAt(entryBuf, int64(position+lenWidth)); err != nil {
		return nil, err
	}

	return entryBuf, nil
}

func (s *store) ReadAt(data []byte, offset int64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.buf.Flush(); err != nil {
		return 0, err
	}

	return s.File.ReadAt(data, offset)
}

func (s *store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	err := s.buf.Flush()
	if err != nil {
		return err
	}

	return s.File.Close()
}


