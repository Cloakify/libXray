package xray

import "testing"

// Instance is read by the failover and the probe while the tunnel starts and
// stops. Run with -race (validate.yml does): an unlocked read of the core
// fails here.
func TestInstanceRacesRunAndStop(t *testing.T) {
	const config = `{"log": {"loglevel": "none"}, "outbounds": [{"protocol": "freedom"}]}`
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 20; i++ {
			if err := RunXray(config); err != nil {
				t.Error(err)
				return
			}
			if err := StopXray(); err != nil {
				t.Error(err)
				return
			}
		}
	}()
	for {
		select {
		case <-done:
			return
		default:
			_ = Instance()
		}
	}
}
