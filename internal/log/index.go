package log

import (
	"io"
	"os"

	"github.com/tysonmote/gommap"
)

var (
	offsetWidth   uint64 = 4
	positionWidth uint64 = 8
	entWidth             = offsetWidth + positionWidth
)

type Config struct {
	MaxIndexBytes uint64
}

type Index struct {
	file *os.File
	mmap gommap.MMap
	size uint64
}

func newIndex(f *os.File, c Config) (*Index, error) {
	index := &Index{
		file: f,
	}

	stat, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}

	index.size = uint64(stat.Size())
	if err = os.Truncate(f.Name(), int64(c.MaxIndexBytes)); err != nil {
		return nil, err
	}

	if index.mmap, err = gommap.Map(index.file.Fd(), gommap.PROT_READ|gommap.PROT_WRITE, gommap.MAP_SHARED); err != nil {
		return nil, err
	}

	return index, nil
}

func (index *Index) Close() error {
	if err := index.mmap.Sync(gommap.MS_SYNC); err != nil {
		return err
	}

	if err := index.file.Sync(); err != nil {
		return err
	}

	if err := index.file.Truncate(int64(index.size)); err != nil {
		return err
	}

	return index.file.Close()
}

func (index *Index) Read(in int64) (out uint32, pos uint64, err error) {
	if index.size == 0 {
		return 0, 0, io.EOF
	}

	if in == -1 {
		out = uint32((index.size / entWidth) - 1)
	} else {
		out = uint32(in)
	}
	pos = uint64(out) * entWidth
	if index.size < pos+entWidth {
		return 0, 0, io.EOF
	}
	out = encoding.Uint32(index.mmap[pos : pos+offsetWidth])
	pos = encoding.Uint64(index.mmap[pos+offsetWidth : pos+entWidth])
	return out, pos, nil
}

func (index *Index) Write(offset uint32, position uint64) error {
	if uint64(len(index.mmap)) < index.size+entWidth {
		return io.EOF
	}

	encoding.PutUint32(index.mmap[index.size: index.size + offsetWidth], offset)
	encoding.PutUint64(index.mmap[index.size + offsetWidth : index.size + entWidth], position)
	index.size+= entWidth
	return nil
}

func (index *Index) Name() string {
	return index.file.Name()
}
