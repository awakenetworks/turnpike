package turnpike

import (
	"strconv"
	"sync"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

func newTestRouter() *defaultRouter {
	router := NewDefaultRouter()
	router.RegisterRealm(URI("turnpike.test"), Realm{})
	return router.(*defaultRouter)
}

func connectedTestClients() (*Client, *Client) {
	router := newTestRouter()
	peer1 := router.getTestPeer()
	peer2 := router.getTestPeer()
	return newTestClient(peer1), newTestClient(peer2)
}

func connectedNTestClients(n int) (callee *Client, callers []*Client) {
	router := newTestRouter()
	callee = newTestClient(router.getTestPeer())
	for i := 0; i < n; i++ {
		callers = append(callers, newTestClient(router.getTestPeer()))
	}
	return callee, callers
}

func newTestClient(p Peer) *Client {
	client := NewClient(p)
	client.ReceiveTimeout = time.Second
	_, err := client.JoinRealm("turnpike.test", nil)
	So(err, ShouldBeNil)
	return client
}

func TestJoinRealm(t *testing.T) {
	Convey("Given a server accepting client connections", t, func() {
		peer := newTestRouter().getTestPeer()

		Convey("A client should be able to succesfully join a realm", func() {
			client := NewClient(peer)
			_, err := client.JoinRealm("turnpike.test", nil)
			So(err, ShouldBeNil)
		})
	})
}

func testAuthFunc(d map[string]interface{}, c map[string]interface{}) (string, map[string]interface{}, error) {
	return testCRSign(c), map[string]interface{}{}, nil
}

func TestJoinRealmWithAuth(t *testing.T) {
	Convey("Given a server accepting client connections", t, func() {
		router := newTestRouter()
		router.RegisterRealm(URI("turnpike.test.auth"), Realm{
			CRAuthenticators: map[string]CRAuthenticator{"testauth": &testCRAuthenticator{}},
		})

		peer := router.getTestPeer()

		Convey("A client should be able to successfully authenticate and join a realm", func() {
			client := NewClient(peer)
			client.Auth = map[string]AuthFunc{"testauth": testAuthFunc}
			details := map[string]interface{}{"username": "tester"}
			_, err := client.JoinRealm("turnpike.test.auth", details)
			So(err, ShouldBeNil)
		})
	})
}

func TestRemoteCall(t *testing.T) {
	Convey("Given two clients connected to the same server", t, func() {
		callee, caller := connectedTestClients()

		Convey("The callee unregisters an invalid method", func() {
			err := callee.Unregister("invalidmethod")
			Convey("And expects an error", func() {
				So(err, ShouldNotBeNil)
			})
		})

		Convey("The callee registers a valid method", func() {
			handler := func(args []interface{}, kwargs map[string]interface{}) *CallResult {
				return &CallResult{Args: []interface{}{args[0].(int) * 2}}
			}
			methodName := "mymethod"
			err := callee.BasicRegister(methodName, handler)

			Convey("And expects no error", func() {
				So(err, ShouldBeNil)

				Convey("The caller calls the callee's remote method", func() {
					callArgs := []interface{}{5100}
					result, err := caller.Call(methodName, callArgs, make(map[string]interface{}))

					Convey("And succeeds at multiplying the number by 2", func() {
						So(err, ShouldBeNil)
						So(result.Arguments[0], ShouldEqual, 10200)
					})
				})
			})

			Convey("And unregisters the method", func() {
				err := callee.Unregister(methodName)
				Convey("And expects no error", func() {
					So(err, ShouldBeNil)
				})

				Convey("Calling the unregistered procedure", func() {
					callArgs := []interface{}{5100}
					result, err := caller.Call(methodName, callArgs, make(map[string]interface{}))

					Convey("Should result in an error", func() {
						So(err, ShouldNotBeNil)
						So(result, ShouldNotBeNil)
					})
				})
			})
		})
	})
}

// on OSX this test may not run without raising file limits
// sudo launchctl limit maxfiles 2000000 2000000
// go test -v -race -cpu 8
func TestNWayParallelRemoteCall(t *testing.T) {
	clients := 50
	Convey("Given "+strconv.Itoa(clients)+" clients connected to the same server", t, func() {
		callee, callers := connectedNTestClients(clients)
		defer callee.Close()

		Convey("The callee registers a valid method", func() {
			handler := func(args []interface{}, kwargs map[string]interface{}) *CallResult {
				return &CallResult{Args: []interface{}{args[0].(int) * 2}}
			}
			methodName := "mymethod"
			err := callee.BasicRegister(methodName, handler)

			Convey("And expects no error", func() {
				So(err, ShouldBeNil)

				Convey("The callers calls the callee's remote method", func() {
					wg := sync.WaitGroup{}
					wg.Add(clients)

					for i := 0; i < clients; i++ {
						go func(i int) {
							defer callers[i].Close()
							callArgs := []interface{}{5100}
							result, err := callers[i].Call(methodName, callArgs, make(map[string]interface{}))
							if err != nil {
								t.Log("Error ", err)
								t.Fail()
							}
							if result.Arguments[0] != 10200 {
								t.Log("Expected 10200 got ", result.Arguments[0])
								t.Fail()
							}
							wg.Done()
						}(i)
					}
					wg.Wait()
				})
			})

			Convey("And unregisters the method", func() {
				err := callee.Unregister(methodName)
				Convey("And expects no error", func() {
					So(err, ShouldBeNil)
				})
			})
		})
	})
}
