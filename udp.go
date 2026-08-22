package invoke

import (
	"encoding/json"
	"fmt"
	"net"
)

const udpBufSize = 65535 // max UDP datagram — self-delimiting, no length prefix needed

func (e *Engine) serveUDP() {
	addr, err := net.ResolveUDPAddr("udp", e.cfg.UDPAddr)
	if err != nil {
		e.emit(LogLifecycle, "invoke: UDP resolve failed on %s: %v", e.cfg.UDPAddr, err)
		return
	}
	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		e.emit(LogLifecycle, "invoke: UDP listen failed on %s: %v", e.cfg.UDPAddr, err)
		return
	}
	e.udpConn = conn
	e.emit(LogLifecycle, "invoke: UDP listening on %s", e.cfg.UDPAddr)

	buf := make([]byte, udpBufSize)
	for {
		n, remote, err := conn.ReadFromUDP(buf)
		if err != nil {
			select {
			case <-e.stopCh:
				return // clean shutdown — udpConn.Close() unblocked ReadFromUDP
			default:
				e.emit(LogDropped, "invoke: UDP read error: %v", err)
				continue
			}
		}
		// copy before goroutine — buf is reused on next iteration
		data := make([]byte, n)
		copy(data, buf[:n])
		src := remote.String()
		e.iPool.submitR(rTask{fn: func() {
			var pkt Packet
			if err := json.Unmarshal(data, &pkt); err != nil {
				e.emit(LogDropped, "invoke: UDP unmarshal from %s: %v", src, err)
				return
			}
			e.handleCommand(src, pkt) // same dispatch path as TCP
		}})
	}
}

// PushUDP sends a packet to any UDP address without expecting a response.
// Unlike Push, there is no connection registry, no ACK, and no retry —
// delivery is best-effort by design.
func (f *Factory) PushUDP(addr string, pkt Packet) error {
	if f.engine.udpConn == nil {
		return fmt.Errorf("invoke: UDP not enabled")
	}
	pkt.Ts = f.engine.timer.ReadUs()
	data, err := json.Marshal(pkt)
	if err != nil {
		return fmt.Errorf("invoke: UDP marshal: %w", err)
	}
	remote, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return fmt.Errorf("invoke: UDP resolve %q: %w", addr, err)
	}
	return f.AssignP(func() {
		if _, err := f.engine.udpConn.WriteToUDP(data, remote); err != nil {
			f.engine.emit(LogDropped, "invoke: UDP write to %s: %v", addr, err)
		}
	})
}
