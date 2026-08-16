package iouring

import (
	"runtime"
	"sync/atomic"
)

func Poll(
	r *IOUring,
	stop *atomic.Bool,
	finish chan struct{},
	wp chan<- CQEntry,
	fallback func(CQEntry),
) {
	runtime.LockOSThread()
	defer close(finish)

	for {
		if stop.Load() {
			break
		}

		read := *r.CQHead // Или наоборот???
		if read == atomic.LoadUint32(r.CQTail) {
			// TODO считаем количество холостых ходов.
			//      Если накопилось слишком много, то входим в режим висения SQE.
			//      Нужно висение с таймаутом. Возможно достаточно просто накидывать
			//      периодически таймеров в SQ, чтобы будить. Ну или опция в Enter
			//      может какая есть (сомнительно, задача на таймер отлично это решает).
			runtime.Gosched()
			continue
		}

		idx := read & r.CQLengthMask
		resp := r.CQ[idx]
		atomic.AddUint32(r.CQHead, 1)

		select {
		case wp <- resp:
		default:
			go fallback(resp)
		}
	}
}
