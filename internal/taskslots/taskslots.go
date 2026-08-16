package taskslots

import (
	"math/bits"
	"unsafe"

	"github.com/kelindar/bitmap"
	"github.com/sirkon/blog/beer"
)

type Slots[T any] struct {
	cap           int
	tasks         []T
	bitmap        bitmap.Bitmap
	bitmapLenMask uint64
	wave          uint64
	free          int
	fallback      map[uint64]T
	fullCount     uint64
}

func New[T any](capacity int) (*Slots[T], error) {
	if bits.OnesCount(uint(capacity)) != 1 {
		return nil, beer.New("capacity must be power of 2")
	}
	if capacity <= 4096 {
		return nil, beer.New("capacity must be at least 4096")
	}

	// Количество слов uint64. Для 131072 это 2048 элементов
	wordsCount := capacity >> 6
	bm := allocAlign(wordsCount)

	return &Slots[T]{
		cap:           capacity,
		bitmap:        bm,
		bitmapLenMask: uint64(wordsCount - 1),
		free:          capacity,
		tasks:         make([]T, capacity),
		fallback:      make(map[uint64]T),
	}, nil
}

func (s *Slots[T]) Add(v T) uint64 {
	// Жесткий контроль: если free == 0, места на горячем пути нет физически
	if s.free == 0 {
		idx := uint64(s.cap) + s.fullCount
		s.fallback[idx] = v
		s.fullCount++
		return idx
	}

	// 1. Привязываем волну к маске кольца слов uint64
	wave := s.wave & s.bitmapLenMask

	// Отрезаем срез от текущего слова до конца битмапы
	bitmp := s.bitmap[wave:]

	localBitIdx, found := bitmp.MinZero()
	var globalSlotIdx uint64

	if found {
		globalSlotIdx = (wave << 6) + uint64(localBitIdx)

		// 🚀 УЛЬТРА-ХАК: Прямая побитовая запись по адресу памяти БЕЗ Bounds Check!
		// Получаем базовый указатель на начало s.bitmap
		basePtr := unsafe.Pointer(unsafe.SliceData(s.bitmap))
		wordIdx := globalSlotIdx >> 6
		bitAt := globalSlotIdx & 63

		// Находим точный адрес нужного uint64 слова: basePtr + wordIdx * 8 байт
		wordPtr := (*uint64)(unsafe.Add(basePtr, wordIdx<<3))
		// Атомарно для этого потока накатываем маску за 1 такт процессора
		*wordPtr |= (uint64(1) << bitAt)

		s.wave += uint64(localBitIdx >> 6)
	} else {
		// 2. НЕ НАШЛИ в хвосте: сбрасываем поиск и ищем с самого начала битмапы.
		// Раз s.free > 0, свободный бит там гарантированно есть!
		globalZeroIdx, _ := s.bitmap.MinZero()
		globalSlotIdx = uint64(globalZeroIdx)
		s.bitmap.Set(globalZeroIdx)

		// Сбрасываем волну на то слово, где только что нашли дырку в начале
		s.wave = globalSlotIdx >> 6
	}

	// Уменьшаем честный счетчик свободных мест
	s.free--

	// Записываем задачу на горячий путь
	s.tasks[globalSlotIdx] = v

	return globalSlotIdx
}

func (s *Slots[T]) Get(idx uint64) (T, bool) {
	// Если индекс укладывается в капу — это горячий путь
	if idx < uint64(s.cap) {
		basePtr := unsafe.Pointer(unsafe.SliceData(s.bitmap))

		// 1. Делим на 64, чтобы найти индекс uint64-слова в слайсе битмапы
		wordIdx := idx >> 6
		// 2. Остаток от деления на 64, чтобы найти позицию бита внутри этого слова
		bitAt := idx & 63

		// 3. Умножаем wordIdx на 8 (сдвиг << 3), так как uint64 весит 8 байт
		wordPtr := (*uint64)(unsafe.Add(basePtr, wordIdx<<3))
		blk := *wordPtr

		// Проверяем наличие бита
		exists := (blk & (uint64(1) << bitAt)) != 0

		return s.tasks[idx], exists
	}

	// Иначе — это фолбек мапа
	res, exists := s.fallback[idx]
	return res, exists
}

func (s *Slots[T]) Del(idx uint64) {
	if idx >= uint64(s.cap) {
		delete(s.fallback, idx)
		return
	}

	s.free++

	// 🚀 УЛЬТРА-ХАК: Сброс бита в 1 такт вообще без проверок длины массива
	basePtr := unsafe.Pointer(unsafe.SliceData(s.bitmap))
	wordIdx := idx >> 6
	bitAt := idx & 63

	wordPtr := (*uint64)(unsafe.Add(basePtr, wordIdx<<3))
	*wordPtr &^= (uint64(1) << bitAt)
}

// Reset полностью очищает состояние SlotTable для повторного использования без аллокаций.
func (s *Slots[T]) Reset() {
	s.free = s.cap
	s.wave = 0
	s.fullCount = 0

	// 1. Быстро зануляем битмапу.
	// Go оптимизирует этот цикл в эффективную ассемблерную команду memclr / vzeroupper.
	clear(s.bitmap)

	// 3. Сбрасываем фолбек-мапу.
	// Если она разрослась, проще пересоздать её, но если она была пустой, аллокации не будет.
	clear(s.fallback)
}

func allocAlign(wordsCount int) []uint64 {
	// Выделяем память с запасом под выравнивание (64 байта = 8 штук uint64)
	buf := make([]uint64, wordsCount+8)
	ptr := uintptr(unsafe.Pointer(&buf[0]))
	aptr := (ptr + 63) &^ 63
	gap := (aptr - ptr) >> 3 // Смещение в элементах uint64 (деление на 8 байт)

	return buf[int(gap) : int(gap)+wordsCount]
}
