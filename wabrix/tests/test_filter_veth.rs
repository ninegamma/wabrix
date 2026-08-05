use std::process::Command;
use std::thread;
use std::time::Duration;

#[test]
fn verify_filter_engine_forwarding() -> Result<(), Box<dyn std::error::Error>> {

    // Spawn UDP receiver thread inside the receiver namespace
    let receiver_handle = thread::spawn(|| {
        let script = r#"
import socket
s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
s.bind(('10.0.1.102', 6000))
s.settimeout(3.0)
try:
    data, addr = s.recvfrom(2048)
    print(f"RECEIVED:{data.decode('utf-8', errors='ignore')}")
except socket.timeout:
    print("TIMEOUT")
except Exception as e:
    print(f"ERROR:{e}")
"#;
        let output = Command::new("ip")
            .args(["netns", "exec", "ns_receiver", "python3", "-c", script])
            .output()
            .expect("Failed to run receiver in ns_receiver");

        String::from_utf8_lossy(&output.stdout).trim().to_string()
    });

    // Provide startup delay for the receiver socket binding
    thread::sleep(Duration::from_millis(400));

    // Execute standard packet transmission
    let send_clean_script = r#"
import socket
s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
s.sendto(b"CLEAN_PACKET_DATA", ('10.0.1.102', 6000))
"#;
    let _ = Command::new("ip")
        .args([
            "netns",
            "exec",
            "ns_sender",
            "python3",
            "-c",
            send_clean_script,
        ])
        .output()?;

    let result = receiver_handle.join().unwrap();
    assert_eq!(
        result, "RECEIVED:CLEAN_PACKET_DATA",
        "Clean packet should have been delivered"
    );

    // Spawn UDP receiver thread for blocked payload verification
    let receiver_handle_blocked = thread::spawn(|| {
        let script = r#"
import socket
s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
s.bind(('10.0.1.102', 6000))
s.settimeout(2.0)
try:
    data, addr = s.recvfrom(2048)
    print(f"RECEIVED:{data.decode('utf-8', errors='ignore')}")
except socket.timeout:
    print("TIMEOUT")
except Exception as e:
    print(f"ERROR:{e}")
"#;
        let output = Command::new("ip")
            .args(["netns", "exec", "ns_receiver", "python3", "-c", script])
            .output()
            .expect("Failed to run receiver in ns_receiver");

        String::from_utf8_lossy(&output.stdout).trim().to_string()
    });

    // Provide startup delay for the receiver socket binding
    thread::sleep(Duration::from_millis(400));

    // Execute malicious payload transmission
    let send_blocked_script = r#"
import socket
s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
s.sendto(b"PAYLOAD_WITH_MALICIOUS_PAYLOAD_V2_INSIDE", ('10.0.1.102', 6000))
"#;
    let _ = Command::new("ip")
        .args([
            "netns",
            "exec",
            "ns_sender",
            "python3",
            "-c",
            send_blocked_script,
        ])
        .output()?;

    let blocked_result = receiver_handle_blocked.join().unwrap();
    assert_eq!(
        blocked_result, "TIMEOUT",
        "Malicious packet should have been dropped by WasmEngine"
    );

    Ok(())
}