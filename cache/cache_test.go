package cache

import (
  "math/rand"
  "strconv"
  "testing"
)

func BenchmarkSeq(b *testing.B) {
  s := NewSequence()

  for i := 0; i < b.N; i++ {
    s.Increment("bench")
    s.Increment("bench 1")
    s.Increment("bench 2")
  }
}

func BenchmarkKeyValue(b *testing.B) {
  m := NewMap()

  for i := 0; i < b.N; i++ {
    token := make([]byte, 8)
    rand.Read(token)
    m.Set(strconv.Itoa(i), Value{Data: token, ContentType: "test content"})
  }

  for i := 0; i < b.N; i++ {
    m.Get(strconv.Itoa(i))
  }

  for i := 0; i < b.N; i++ {
    m.Delete(strconv.Itoa(i))
  }
}

func TestSeq(t *testing.T) {
  if false {
    t.Error("Should never fail")
  }
}
