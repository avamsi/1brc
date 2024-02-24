package main

import (
	"cmp"
	"math/rand/v2"
)

type skipListNode[K cmp.Ordered, V any] struct {
	k    K
	v    V
	next []*skipListNode[K, V]
}

type SkipList[K cmp.Ordered, V any] struct {
	head skipListNode[K, V]
}

func randHeight() int {
	h := 1
	for h < 42 && rand.IntN(2) == 0 {
		h++
	}
	return h
}

func (s *SkipList[K, V]) Put(k K, v V) {
	var (
		h = randHeight()
		p = &skipListNode[K, V]{k, v, make([]*skipListNode[K, V], h)}
		n = &s.head
	)
	if h > len(n.next) {
		n.next = append(n.next,
			make([]*skipListNode[K, V], h-len(n.next))...)
	}
	for i := len(n.next) - 1; i >= 0; {
		if n.next[i] == nil || n.next[i].k >= k {
			if i < h {
				p.next[i] = n.next[i]
				n.next[i] = p
			}
			i--
		} else {
			n = n.next[i]
		}
	}
}

func (s *SkipList[K, V]) Get(k K) (V, bool) {
	n := &s.head
	for i := len(n.next) - 1; i >= 0; {
		if n.next[i] == nil || n.next[i].k > k {
			i--
		} else if n.next[i].k == k {
			return n.next[i].v, true
		} else {
			n = n.next[i]
		}
	}
	return *new(V), false
}

func (s *SkipList[K, V]) Items() func(yield func(K, V) bool) {
	return func(yield func(K, V) bool) {
		for n := &s.head; n.next[0] != nil; {
			n = n.next[0]
			if !yield(n.k, n.v) {
				return
			}
		}
	}
}
