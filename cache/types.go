package cache

import (
  "sync"
)

type Value struct {
  Data []byte
  ContentType string
}

// Regarding concurrency / safety see:
//  https://eli.thegreenplace.net/2019/on-concurrency-in-go-http-servers
//  https://stackoverflow.com/questions/45585589/golang-fatal-error-concurrent-map-read-and-map-write/45585833
//
// FUTURE: Consider using a syncmap to improve performance with many cores
type Sequence struct {
  Data map[string]int
  Lock sync.RWMutex
}

func NewSequence() *Sequence {
  m := make(map[string]int)
  l := sync.RWMutex{}
  return &Sequence{m, l}
}

type Map struct {
  Data map[string]Value
  Lock sync.RWMutex
}

func NewMap() *Map {
  m := make(map[string]Value)
  l := sync.RWMutex{}
  return &Map{m, l}
}

func (m *Sequence) Increment(k string) int{
  (*m).Lock.Lock()
  if val, ok := (*m).Data[k]; ok {
    (*m).Data[k] = val + 1
  } else {
    (*m).Data[k] = 0
  }
  result := (*m).Data[k]
  (*m).Lock.Unlock()

  return result
}

func (m *Sequence) Delete(k string) {
  (*m).Lock.Lock()
  delete((*m).Data, k)
  (*m).Lock.Unlock()
}

func (m *Map) Get(k string) (Value, bool) {
  (*m).Lock.RLock()
  val, ok := (*m).Data[k]
  (*m).Lock.RUnlock()
  return val, ok
}

func (m *Map) Set(k string, v Value) {
  (*m).Lock.Lock()
  (*m).Data[k] = v
  (*m).Lock.Unlock()
}

func (m *Map) Delete(k string) {
  (*m).Lock.Lock()
  delete((*m).Data, k)
  (*m).Lock.Unlock()
}
