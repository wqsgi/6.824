package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "os"
import (
	"container/list"
	"sync/atomic"
	"fmt"
)

type ViewServer struct {
	mu       sync.Mutex
	l        net.Listener
	dead     int32 // for testing
	rpccount int32 // for testing
	me       string

	// Your declarations here.
	track       map[string]time.Time
	currentView uint // 当前的view

	members list.List
	// 超时时间
	timeout time.Duration
	primaryAck bool
}

type ack struct {
	viewNum uint
	ackCount uint
}
//
// server Ping RPC handler.
// todo ack确认机制缺失，需要判断client ack确认后才能升级为主
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {
	vs.mu.Lock()
	defer vs.mu.Unlock()
	// Your code here.
	vs.track[args.Me] = time.Now()
	if args.Me == vs.master() && args.Viewnum == vs.currentView{
		vs.primaryAck = true
	}

	if args.Viewnum == vs.currentView && vs.currentView != 0 {
		reply.View = View{Viewnum: vs.currentView, Primary: vs.master(), Backup: vs.salve()}
		return nil
	}
	if args.Viewnum == 0 {
		// 对于已经是链表最后的服务，第一次请求的num==0 的情况，不需要重新删除添加
		if vs.appendNotExist(args.Me) && vs.members.Back().Value.(string) != args.Me {
			vs.removeExist(args.Me)
		}
	}
	vs.appendNotExist(args.Me)
	reply.View = View{Viewnum: vs.currentView, Primary: vs.master(), Backup: vs.salve()}
	return nil

}

// 需要确认ack是否正确。
func (vs *ViewServer) master() string {
	for e := vs.members.Front(); e != nil; e = e.Next() {
		return e.Value.(string)
	}
	return ""
}

func (vs *ViewServer) salve() string {
	index := 0
	for e := vs.members.Front(); e != nil; e = e.Next() {
		if index == 1 {
			return e.Value.(string)
		}
		index++
	}
	return ""
}

// true 存在，false 不存在
func (vs *ViewServer) appendNotExist(name string) bool {
	index := 0
	ret := false
	for e := vs.members.Front(); e != nil; e = e.Next() {
		index++
		if value, ok := e.Value.(string); ok {
			if value == name {
				ret = true
				break
			}
		}
	}
	if index < 2 && !ret {
		vs.currentView += 1
	}
	if !ret {
		// 对于第一次获取的主，需要ack后置为true
		if index == 1{
			vs.primaryAck = false
		}
		vs.members.PushBack(name)

	}
	return ret
}

// true 存在，false 不存在
func (vs *ViewServer) removeExist(name string) bool {
	index := 0
	for e := vs.members.Front(); e != nil; e = e.Next() {
		index++
		if value, ok := e.Value.(string); ok {
			if value == name {
				// 删除的是master情况
				if index == 1 {
					vs.primaryAck = false
				}
				if index < 2 {
					vs.currentView += 1
				}
				vs.members.Remove(e)

				return true
			}
		}
	}
	return false
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {
	vs.mu.Lock()
	defer vs.mu.Unlock()
	// Your code here.

	reply.View = View{Viewnum: vs.currentView, Primary: vs.master(), Backup: vs.salve()}
	return nil
}

//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {
	vs.mu.Lock()
	defer vs.mu.Unlock()
	// Your code here.

	for key, value := range vs.track {
		if time.Now().Sub(value) > vs.timeout {
			if  !vs.primaryAck && vs.master() == key{
				continue
			}
			vs.removeExist(key)
			delete(vs.track, key)
		}

	}
}

//
// tell the server to shut itself down.
// for testing.
// please don't change these two functions.
//
func (vs *ViewServer) Kill() {
	atomic.StoreInt32(&vs.dead, 1)
	vs.l.Close()
}

//
// has this server been asked to shut down?
//
func (vs *ViewServer) isdead() bool {
	return atomic.LoadInt32(&vs.dead) != 0
}

// please don't change this function.
func (vs *ViewServer) GetRPCCount() int32 {
	return atomic.LoadInt32(&vs.rpccount)
}

func StartServer(me string) *ViewServer {
	vs := new(ViewServer)
	vs.currentView = 0
	vs.track = make(map[string]time.Time)
	vs.me = me
	vs.timeout = PingInterval * DeadPings
	// Your vs.* initializations here.

	// tell net/rpc about our RPC server and handlers.
	rpcs := rpc.NewServer()
	rpcs.Register(vs)

	// prepare to receive connections from clients.
	// change "unix" to "tcp" to use over a network.
	os.Remove(vs.me) // only needed for "unix"
	l, e := net.Listen("tcp", vs.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	vs.l = l

	// please don't change any of the following code,
	// or do anything to subvert it.

	// create a thread to accept RPC connections from clients.
	go func() {
		for vs.isdead() == false {
			conn, err := vs.l.Accept()
			if err == nil && vs.isdead() == false {
				atomic.AddInt32(&vs.rpccount, 1)
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
			if err != nil && vs.isdead() == false {
				fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
				vs.Kill()
			}
		}
	}()

	// create a thread to call tick() periodically.
	go func() {
		for vs.isdead() == false {
			vs.tick()
			time.Sleep(PingInterval)
		}
	}()

	return vs
}
