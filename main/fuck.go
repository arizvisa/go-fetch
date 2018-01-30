package main
import "strings"
import "net/url"
import "fmt"

func main() {
	u, _ := url.Parse("http://fuck/fuck/you")
	a := strings.SplitN(u.Host, ":", 2)
	fmt.Println(a)
	fmt.Println(len(a))
}
