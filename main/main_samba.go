package main

import "github.com/arizvisa/go-fetch"
import "github.com/arizvisa/go-fetch/source"
import "fmt"
import "context"
import "sync"
import "os"
//import "encoding/hex"

func main() {
	ctx := context.Background()
	ctx = context.WithValue(ctx, fetch.CKSourcePath{}, os.Args[1])
	//ctx = context.WithValue(ctx, fetch.CKSourceOffset{}, int64(0))
	//ctx = context.WithValue(ctx, fetch.CKSourceSegment{}, int64(1476))
	err_s := make(chan error)
	ctx = context.WithValue(ctx, fetch.CKSourceError{}, err_s)

	fmt.Printf("new\n")
	src := source.NewSamba()
	if src == nil {
		panic("fuck")
	}

	fmt.Printf("setup\n")
	ctx, err := src.Setup(ctx)
	if err != nil {
		panic(err.Error())
	}
	ctx, _ = context.WithCancel(ctx)

	fmt.Printf("read\n")
	wg := sync.WaitGroup{}
	out, err := src.Read(ctx)
	if err != nil {
		panic(err.Error())
	}
	wg.Add(1)

	go func() {
		var total int64
		total = 0
	Transfer:
		for {
			select {
			case err := <-err_s:
				fmt.Printf("Error: %#v\n", err)
				break Transfer
			case <-ctx.Done():
				fmt.Printf("Done!\n")
				break Transfer
			case data := <-out:
				total += int64(len(data))
				fmt.Printf("Received +%d bytes (%d)\n", len(data), total)
				//fmt.Println(hex.Dump(data))
			}
		}
	Errors:
		for {
			select {
			case err := <-err_s:
				fmt.Printf("Leftover error: %#v\n", err)
			default:
				break Errors
			}
		}
		fmt.Printf("Filename: %#v\nFilepath: %#v\nTimestamp: %#v\n", src.LocalName(), src.LocalPath(), src.LocalTime().String())
		wg.Done()
	}()
	wg.Wait()
}
