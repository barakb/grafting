Grafting 
==========

[![Build Status](https://travis-ci.org/barakb/grafting.svg?branch=master)](https://travis-ci.org/barakb/grafting)

My golang raft implementation.

Grafting is after [queen rearing grafting](http://www.nsqba.com/graftingtipsinfo.html)

For now state machine is `map[string]interface{}`

TCP transport is not written yet.

See sample usage in the file [servers_test.go](https://github.com/barakb/grafting/blob/master/servers_test.go)

There is a new client that works with the server !

```Go
	res := <-client.Execute(SetValue{"foo", "bar"})
	if res != nil {
		t.Errorf("result of first command should be nil, instead %q\n", res)
	}
	res = <-client.Execute(SetValue{"foo", "bar1"})
	if res != "bar" {
		t.Errorf("result of second command should be %q, instead %q\n", "bar", res)
	}
```

- [x] client allow up to 5 pending request after that it block.
- [x] client send retries to servers in the right order.
- [x] log save for each client the 5 last request and (response)
- [x] server ignore retry if response not ready otherwise send old response.
- [ ] TestBecomeLeader some time fails: 
 === RUN   TestBecomeLeader
 INFO[0000] server1 is LEADER                            
 --- FAIL: TestBecomeLeader (0.50s)
 	server_test.go:173: server should have bean CANDIDATE (2) instead 3
