package iouring

import (
	"math"
	"unsafe"

	"github.com/sirkon/blog"
	"github.com/sirkon/blog/beer"
	"golang.org/x/sys/unix"
)

// Close корректно освобождает разделяемую с ядром память и закрывает ринг
func (r *IOUring) Close() error {
	// 1. Проверяем, не был ли ринг уже закрыт (защита от double close)
	if r.FD == 0 || r.FD == math.MaxInt32 {
		return nil
	}

	var failed bool

	// 2. Раззамапливаем массив SQEs (физический склад 64-байтных задач)
	// Размер вычисляем строго так же, как при создании: количество элементов * 64 байта
	if len(r.SQ) > 0 {
		sqesSize := len(r.SQ) * 64 // или unsafe.Sizeof(SQEntry{})
		// Извлекаем сырой указатель на начало слайса в памяти
		sqesPtr := unsafe.SliceData(r.SQ)
		err := unix.Munmap(unsafe.Slice((*byte)(unsafe.Pointer(sqesPtr)), sqesSize))
		if err != nil {
			r.logger.Error(nil, "failed to unmap SQes buffer", blog.Err(err))
		}
		failed = true
	}

	// 3. Раззамапливаем CQ Ring и SQ Ring
	// В режиме IORING_FEAT_SINGLE_MMAP (который стандарт для современных ядер Linux)
	// CQ и SQ делят одну область памяти, поэтому адрес начала у них одинаковый.
	// Если ты мапил их одним вызовом по размеру максимального из колец, размапливаем его.

	// Берем базовый указатель на SQ Ring (память управления очередью), который мы сохраняли
	if r.SQRingPtr != 0 {
		// Вычисляем размер SQ кольца управления (индексы Array + смещение)
		sqRingSize := r.Params.SQOff.Array + r.Params.SQEntries*4

		// Если был SINGLE_MMAP, cqRingSize мог быть больше, и мы выравнивали sqRingSize по нему:
		cqRingSize := r.Params.CQOff.Cqes + r.Params.CQEntries*16 // 16 байт на CQEntry
		if (r.Params.Features & featSingleMMap) != 0 {
			if cqRingSize > sqRingSize {
				sqRingSize = cqRingSize
			}
		}

		// Освобождаем SQ область управления
		err := unix.Munmap(unsafe.Slice((*byte)(unsafe.Pointer(r.SQRingPtr)), int(sqRingSize)))
		if err != nil {
			r.logger.Error(nil, "failed to unmap SQRing buffer", blog.Err(err))
			failed = true
		}

		// Если SINGLE_MMAP не было (старое ядро), то CQ мапился отдельно — освобождаем его
		if (r.Params.Features&featSingleMMap) == 0 && r.CQRingPtr != 0 {
			err = unix.Munmap(unsafe.Slice((*byte)(unsafe.Pointer(r.CQRingPtr)), int(cqRingSize)))
			if err != nil {
				r.logger.Error(nil, "failed to unmap CQRing buffer", blog.Err(err))
				failed = true
			}
		}
	}

	// 4. Закрываем файловый дескриптор самого ринга.
	// Это уничтожает контекст io_uring в ядре и останавливает поток SQPOLL.
	if err := unix.Close(r.FD); err != nil {
		r.logger.Error(nil, "failed to close io_uring descriptor", blog.Err(err))
		failed = true
	}

	// Помечаем ринг как невалидный
	r.FD = math.MaxInt32
	r.SQRingPtr = 0
	r.CQRingPtr = 0

	if failed {
		return beer.New("there were errors on ring deconstruction")
	}

	return nil
}
