package main

import (
	"github.com/jiubafangxing/baecon/internal/store"
	"reflect"
)

func main() {
	record := store.RecordBatch{}
	print(reflect.TypeOf(record.ProducerId).Size())
}
