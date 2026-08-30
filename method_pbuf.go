package netring

import (
	"github.com/sirkon/blog/beer"
)

// RegisterBufferRing provisions a kernel-provided buffer ring for sizeClass
// with the given capacity and publishes it under bgid == uint16(sizeClass).
// Recv(fd, sizeClass) becomes usable for this ring afterwards.
//
// All five size classes (128..16384 bytes) are multiples of 64, so the
// internal multiple-of-64 buffer size guard always passes for valid classes;
// capacity must still be a power of two (and no greater than 32768).
func (nr *NetRing) RegisterBufferRing(sizeClass SizeClass, capacity uint32) error {
	if sizeClass.Size() == 0 {
		return beer.Newf("invalid size class %s", sizeClass)
	}
	if nr.pbrs[sizeClass] != nil {
		return beer.Newf("size class %s is already provisioned", sizeClass)
	}

	pbr, err := nr.r.RegisterBufferRing(uint16(sizeClass), capacity, sizeClass.Size())
	if err != nil {
		return beer.Wrapf(err, "register %s buffer ring", sizeClass)
	}

	nr.pbrs[sizeClass] = pbr
	return nil
}

// UnregisterBufferRing detaches and frees the ring of sizeClass.
func (nr *NetRing) UnregisterBufferRing(sizeClass SizeClass) error {
	pbr := nr.pbrs[sizeClass]
	if pbr == nil {
		return beer.Newf("size class %s is not provisioned", sizeClass)
	}
	if err := pbr.Unregister(); err != nil {
		return beer.Wrapf(err, "unregister %s buffer ring", sizeClass)
	}

	nr.pbrs[sizeClass] = nil
	return nil
}