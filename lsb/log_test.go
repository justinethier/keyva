package lsb

import (
  "bytes"
  "math/rand"
  "strconv"
  "testing"
)

//func BenchmarkSeq(b *testing.B) {
//  for i := 0; i < b.N; i++ {
//    Increment("bench")
//    Increment("bench 1")
//    Increment("bench 2")
//  }
//}

func BenchmarkKeyValueSet(b *testing.B) {
  ResetDB()

  for i := 0; i < b.N; i++ {
    token := make([]byte, 8)
    rand.Read(token)
    j := rand.Intn(b.N)
    Set(strconv.Itoa(j), Value{Data: token, ContentType: "test content"})
  }
}

func BenchmarkKeyValueGet(b *testing.B) {
  for i := 0; i < b.N; i++ {
    j := rand.Intn(b.N)
    Get(strconv.Itoa(j))
  }
}

func BenchmarkKeyValueDelete(b *testing.B) {
  for i := 0; i < b.N; i++ {
    j := rand.Intn(b.N)
    Delete(strconv.Itoa(j))
  }
}

func TestSeq(t *testing.T) {
  if false {
    t.Error("Should never fail")
  }
}

func TestKeyValue(t *testing.T) {
  var N = 100
  ResetDB()

  for i := 0; i < N; i++ {
    // TODO: encode predictable value for i
    //token := make([]byte, 8)
    //rand.Read(token)
    Set(strconv.Itoa(i), Value{Data: []byte(strconv.Itoa(i)), ContentType: "test content"})
  }

  // verify i contains expected value
  for i := 0; i < N; i++ {
    if val, found := Get(strconv.Itoa(i)); found {
      if bytes.Compare(val.Data, []byte(strconv.Itoa(i))) != 0 {
        t.Error("Unexpected value", val.Data, "for key", i)
      }
    } else {
      t.Error("Value not found for key", i)
    }
  }

  for i := 0; i < N; i++ {
    Delete(strconv.Itoa(i))
  }

  // verify key does not exist for i
  for i := 0; i < N; i++ {
    if val, found := Get(strconv.Itoa(i)); found {
      t.Error("Unexpected value", val.Data, "for key", i)
    }
  }


  // TODO: add a key back
  Set("abcd", Value{[]byte("test"), "text"})

  // TODO verify that key exists now
  if val, found := Get("abcd"); found {
    if string(val.Data) != "test" {
      t.Error("Unexpected value", val.Data, "for key", "abcd")
    }
  } else {
    t.Error("Value not found for key", "abcd")
  }
}

