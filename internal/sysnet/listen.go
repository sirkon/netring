package sysnet

import (
	"net"

	"github.com/sirkon/blog/beer"
	"golang.org/x/sys/unix"
)

func Listen(ip string, port int) (int, error) {
	// 1. Создаем сокет (сразу неблокирующий или блокирующий — под io_uring обычно берут стандартный)
	fd, err := unix.Socket(unix.AF_INET, unix.SOCK_STREAM, 0)
	if err != nil {
		return 0, beer.Wrap(err, "create socket")
	}

	// 2. Включаем SO_REUSEADDR, чтобы не ждать освобождения порта
	if err := unix.SetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_REUSEADDR, 1); err != nil {
		return 0, beer.Wrap(err, "set socket flags")
	}

	// 3. Биндим порт
	sockAddr := &unix.SockaddrInet4{Port: port}
	copy(sockAddr.Addr[:], net.ParseIP(ip).To4())
	if err := unix.Bind(fd, sockAddr); err != nil {
		return 0, beer.Wrap(err, "bind socket to given addr:port")
	}

	// 4. Начинаем слушать
	if err := unix.Listen(fd, unix.SOMAXCONN); err != nil {
		return 0, beer.Wrap(err, "listen to connections")
	}

	return fd, nil
}
