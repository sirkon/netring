package iouring

import (
	"fmt"
	"unsafe"
)

// SQEntry задача для SQ.
type SQEntry struct {
	Opcode   uint8  // Код операции (например, OpAccept)
	Flags    uint8  // Флаги (например, IOSQE_IO_LINK)
	Priority uint16 // Приоритет ввода-вывода
	FD       int32  // Твой сокет / файловый дескриптор

	// В Си тут union: off (смещение) или addr2
	Off uint64

	// В Си тут union: addr (указатель на буфер/структуру) или splice_off_in
	Addr uint64

	Len uint32 // Длина буфера / количество iovec

	// В Си тут union: flags распаковывается в op_flags
	OpFlags uint32

	UserData uint64 // Твой контекст, который вернется в CQE

	// В Си тут union: buf_index или buf_group
	BufIndex uint16

	Personality uint16 // ID пользователя для выполнения команды
	SpliceFdIn  int32  // Для операции splice

	// Паддинг/резерв на будущее, чтобы добить структуру ровно до 64 байт
	Pad2 [2]uint64
}

type CQEntry struct {
	UserData uint64 // Тот самый твой контекст из SQE
	Res      int32  // Результат сискола (например, количество прочитанных байт или -ERRNO)
	Flags    uint32 // Флаги ядра
}

func init() {
	fmt.Println(unsafe.Sizeof(SQEntry{}))
}
