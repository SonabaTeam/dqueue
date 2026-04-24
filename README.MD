USAGE FOR DQUEUE

```go
import (
	"fmt"
	"time"
	
	"github.com/SonabaTeam/dqueue"
)

// START DQUEUE
dqueue.Start()

// Push the queue to the execution queue list
dqueue.Push(func(){
	fmt.Print("Push")
}, 2 * time.Second /* Delay time e.g(you can use second, hours, minute for delay time) */)

// Push the queue to the front of the execution queue list
dqueue.PushFront(func(){
	fmt.Print("PushFront")
}, 2 * time.Second /* Delay time e.g(you can use second, hours, minute for delay time) */)

// STOP DQUEUE
dqueue.Stop()
```