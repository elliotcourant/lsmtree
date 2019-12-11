package lsmtree

import (
	"encoding/binary"
	"github.com/stretchr/testify/assert"
	"math/rand"
	"sync"
	"testing"
)

func TestOpenValueFile(t *testing.T) {
	t.Run("directory doesnt exist", func(t *testing.T) {
		file, err := openValueFile("tmp", 1)
		assert.Error(t, err)
		assert.Nil(t, file)
	})

	t.Run("create file", func(t *testing.T) {
		dir, cleanup := NewTempDirectory(t)
		defer cleanup()

		file, err := openValueFile(dir, 1)
		assert.NoError(t, err)
		assert.NotNil(t, file)
	})
}

func TestValueFile_Write(t *testing.T) {
	t.Run("synchronous", func(t *testing.T) {
		dir, cleanup := NewTempDirectory(t)
		defer cleanup()

		file, err := openValueFile(dir, 1)
		assert.NoError(t, err)
		assert.NotNil(t, file)

		originalValue1, originalValue2 := []byte("value one"), []byte("another value")

		offset1, err := file.Write(originalValue1)
		assert.NoError(t, err)
		assert.Equal(t, uint64(0), offset1)

		offset2, err := file.Write(originalValue2)
		assert.NoError(t, err)
		// Make sure the offset of the second value is the length of the first value appended plus the
		// size of the checksum for the first value.
		assert.Equal(t, uint64(len(originalValue1)+4), offset2)
	})

	t.Run("asynchronous", func(t *testing.T) {
		doAsyncTest := func(t *testing.T, file *valueFile) {
			numberOfValues := 1000
			numberOfRoutines := 8

			// Make sure that the number of values divides evenly into the
			// number of routines.
			assert.Equal(t, 0, numberOfValues%numberOfRoutines)

			values := make([][]byte, numberOfValues)
			for i := 0; i < numberOfValues; i++ {
				v := make([]byte, 8)
				binary.BigEndian.PutUint64(v, rand.Uint64())
				values[i] = v
			}

			wg := sync.WaitGroup{}
			wg.Add(numberOfRoutines)
			numberOfValuesPerRoutine := len(values) / numberOfRoutines
			for i := 0; i < numberOfRoutines; i++ {
				go func(i int) {
					defer wg.Done()
					routineValues := values[i*numberOfValuesPerRoutine : i*numberOfValuesPerRoutine+numberOfValuesPerRoutine]
					for _, value := range routineValues {
						_, err := file.Write(value)
						assert.NoError(t, err)
					}
				}(i)
			}
			wg.Wait()

			// Make sure the new offset matches the expected.
			assert.Equal(t, uint64(numberOfValues*(8+4)), file.Offset)
		}

		t.Run("os.File", func(t *testing.T) {
			dir, cleanup := NewTempDirectory(t)
			defer cleanup()

			file, err := openValueFile(dir, 1)
			assert.NoError(t, err)
			assert.NotNil(t, file)

			doAsyncTest(t, file)
		})
	})
}

func BenchmarkValueFile_Write(b *testing.B) {
	dir, cleanup := NewTempDirectory(b)
	defer cleanup()

	file, err := openValueFile(dir, 1)
	assert.NoError(b, err)
	assert.NotNil(b, file)

	value := []byte("test benchmark value for write")

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = file.Write(value)
	}
}
