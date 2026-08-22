package invoke

import (
	"encoding/binary"
	"encoding/json"
	"io"
	"net"
)

// type prefix — first byte of every client message
const (
	typeAck     byte = 0x00 // [0x00][ack: 0|1]
	typeCommand byte = 0x01 // [0x01][uint32 length][JSON Packet]
)

const maxPacketSize uint32 = 1 << 20 // 1MB sanity guard — stops malicious allocs

func (e *Engine) serveTCP() {
	ln, err := net.Listen("tcp", e.cfg.TCPAddr)
	if err != nil {
		e.emit(LogLifecycle, "invoke: TCP listen failed on %s: %v", e.cfg.TCPAddr, err)
		return
	}
	e.tcpLn = ln
	e.emit(LogLifecycle, "invoke: TCP listening on %s", e.cfg.TCPAddr)

	for {
		conn, err := ln.Accept()
		if err != nil {
			select {
			case <-e.stopCh:
				return // clean shutdown — tcpLn.Close() unblocked Accept
			default:
				e.emit(LogDropped, "invoke: TCP accept error: %v", err)
				continue
			}
		}
		clientID := conn.RemoteAddr().String()
		entry := e.register(clientID, conn)
		go e.readLoop(clientID, entry) // one goroutine owns one connection
	}
}

func (e *Engine) readLoop(clientID string, entry *connEntry) {
	defer e.unregister(clientID)

	header := make([]byte, 1)
	for {
		if _, err := io.ReadFull(entry.conn, header); err != nil {
			e.emit(LogDropped, "invoke: read header from %q: %v", clientID, err)
			return
		}

		switch header[0] {

		case typeAck:
			ack := make([]byte, 1)
			if _, err := io.ReadFull(entry.conn, ack); err != nil {
				e.emit(LogDropped, "invoke: read ack from %q: %v", clientID, err)
				return
			}
			select {
			case entry.ackCh <- ack[0]:
			default:
				// Push already timed out and moved on — drop silently
			}

		case typeCommand:
			lenBuf := make([]byte, 4)
			if _, err := io.ReadFull(entry.conn, lenBuf); err != nil {
				e.emit(LogDropped, "invoke: read length from %q: %v", clientID, err)
				return
			}
			size := binary.BigEndian.Uint32(lenBuf)
			if size > maxPacketSize {
				e.emit(LogDropped, "invoke: packet from %q too large (%d bytes) — closing", clientID, size)
				return
			}
			data := make([]byte, size)
			if _, err := io.ReadFull(entry.conn, data); err != nil {
				e.emit(LogDropped, "invoke: read payload from %q: %v", clientID, err)
				return
			}
			var pkt Packet
			if err := json.Unmarshal(data, &pkt); err != nil {
				e.emit(LogDropped, "invoke: unmarshal from %q: %v", clientID, err)
				continue // bad packet — keep connection alive, keep reading
			}
			captured, cID := pkt, clientID
			e.iPool.submitR(rTask{fn: func() {
				e.handleCommand(cID, captured)
			}})

		default:
			e.emit(LogDropped, "invoke: unknown type 0x%02x from %q — closing", header[0], clientID)
			return
		}
	}
}

// handleCommand is the wiring point between the TCP layer and the CommandTable.
// The CommandTable attaches here at setup — TCP never touches command logic directly.
func (e *Engine) handleCommand(clientID string, pkt Packet) {
	if e.table == nil {
		e.emit(LogDropped, "invoke: handleCommand — no table attached, packet from %q dropped", clientID)
		return
	}
	e.table.Dispatch(clientID, pkt)
}
