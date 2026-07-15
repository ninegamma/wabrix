import { useEffect, useRef } from 'react';
import { Terminal } from 'xterm';
import { FitAddon } from 'xterm-addon-fit';
import 'xterm/css/xterm.css'; // CRITICAL: The terminal will look broken without this!

export function TerminalWindow() {
    // A reference to the HTML div where the terminal will be injected
    const terminalRef = useRef<HTMLDivElement>(null);

    useEffect(() => {
        if (!terminalRef.current) return;

        const term = new Terminal({
            theme: {
                background: '#1e1e1e',
                foreground: '#f8fafc',
                cursor: '#3b82f6',
            },
            fontFamily: 'monospace',
            cursorBlink: true,
            scrollback: 10000, // Now it remembers 10,000 lines of history
        });

        const fitAddon = new FitAddon();
        term.loadAddon(fitAddon);
        term.open(terminalRef.current);
        fitAddon.fit();

        // 1. Establish the WebSocket connection back to the Vite Proxy / Axum server
        const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
        const wsUrl = `${protocol}//${window.location.host}/api/ws/terminal`;
        const socket = new WebSocket(wsUrl);

        // 2. When Rust sends us text or ANSI colors, instantly draw it to the terminal!
        socket.onmessage = (event) => {
            term.write(event.data);
        };

        // 3. When the user types on their keyboard, send the raw keystroke to Rust!
        term.onData((data) => {
            if (socket.readyState === WebSocket.OPEN) {
                socket.send(data);
            }
        });

        socket.onclose = () => {
            term.writeln('\r\n\x1b[1;31m[Disconnected]\x1b[0m Connection to backend lost.');
        };

        const resizeObserver = new ResizeObserver(() => fitAddon.fit());
        resizeObserver.observe(terminalRef.current);

        return () => {
            socket.close();
            resizeObserver.disconnect();
            term.dispose();
        };
    }, []);

    return (
        // Outer wrapper handles the padding safely
        <div style={{ width: '100%', height: '100%', background: '#1e1e1e', padding: '10px', boxSizing: 'border-box' }}>

            {/* The xterm canvas MUST have no padding so FitAddon calculates the math perfectly */}
            <div ref={terminalRef} style={{ width: '100%', height: '100%', overflow: 'hidden' }} />

        </div>
    );
}