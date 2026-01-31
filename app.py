import streamlit as st
import paho.mqtt.client as mqtt
import time
import threading
from pathlib import Path
import ssl
import json
import base64
import binascii
from queue import Queue
from threading import Event


# MQTT Configuration - Users MUST enter their own details
MQTT_CONFIG = {
    "MQTT_BROKER": "",
    "MQTT_PORT": 8883,
    "MQTT_TOPIC": "",
    "MQTT_TX_COMMAND_TOPIC": "",
    "MQTT_USERNAME": "",
    "MQTT_PASSWORD": ""
}

CHUNK_SIZE = 250  # bytes
SEND_INTERVAL = 2  # seconds

def calculate_crc32(data):
    """Calculate CRC32 checksum of data"""
    return binascii.crc32(data) & 0xffffffff

def create_payload(chunk_data, chunk_num, total_chunks, filename, file_size):
    """
    Create JSON payload with headers and base64-encoded bin data
    Format:
    {
        "T": 40,
        "S": <sequence>,
        "D": {
            "s_q": 18,
            "nwt": <total_chunks>,
            "fn": <filename>,
            "fs": <file_size>,
            "cn": <chunk_num>,
            "cs": <chunk_size>,
            "crc": <crc32_checksum>,
            "data": <base64_encoded_binary>
        }
    }
    """
    crc_checksum = calculate_crc32(chunk_data)
    
    payload = {
        "T": 40,
        "S": chunk_num,
        "D": {
            "s_q": 18,
            "nwt": total_chunks,
            "fn": filename,
            "fs": file_size,
            "cn": chunk_num,
            "cs": len(chunk_data),
            "crc": f"{crc_checksum:08x}",
            "data": base64.b64encode(chunk_data).decode('utf-8')
        }
    }
    
    return json.dumps(payload)

class MQTTClient:
    def __init__(self, config=None):
        try:
            self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1)
        except AttributeError:
            self.client = mqtt.Client()
        
        # Use provided config or default
        self.config = config if config else MQTT_CONFIG
        
        self.client.username_pw_set(self.config["MQTT_USERNAME"], self.config["MQTT_PASSWORD"])
        self.client.tls_set(ca_certs=None, certfile=None, keyfile=None, cert_reqs=ssl.CERT_REQUIRED, tls_version=ssl.PROTOCOL_TLSv1_2, ciphers=None)
        self.client.tls_insecure_set(False)
        self.client.on_connect = self.on_connect
        self.client.on_disconnect = self.on_disconnect
        self.client.on_publish = self.on_publish
        self.client.on_message = self.on_message
        self.connected = False
        self.response_queue = Queue()
        self.file_bytes = None
        
    def on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            self.connected = True
            st.session_state.status_message = "✅ Connected to MQTT Broker"
        else:
            st.session_state.status_message = f"❌ Connection failed with code {rc}"
    
    def on_disconnect(self, client, userdata, rc):
        self.connected = False
        if rc != 0:
            st.session_state.status_message = f"⚠️ Disconnected with code {rc}"
    
    def on_publish(self, client, userdata, mid):
        pass
    
    def on_message(self, client, userdata, msg):
        """Handle incoming MQTT messages"""
        try:
            raw_payload = msg.payload.decode('utf-8')
            print(f"DEBUG: Received: {raw_payload}") # Check your terminal/console
            payload = json.loads(raw_payload)
            self.response_queue.put(payload) 
        except:
            pass
    
    def connect(self, config=None):
        try:
            cfg = config if config else self.config
            self.client.connect(cfg["MQTT_BROKER"], cfg["MQTT_PORT"], keepalive=60)
            # Only subscribe to the DATA topic where device sends responses
            # Do NOT subscribe to TX_COMMAND_TOPIC (that's where we publish)
            self.client.subscribe(cfg["MQTT_TOPIC"], qos=1)
            self.client.loop_start()
            return True
        except Exception as e:
            st.session_state.status_message = f"❌ Connection error: {str(e)}"
            return False
    
    def publish(self, topic, payload):
        try:
            result = self.client.publish(topic, payload, qos=1)
            return result.rc == mqtt.MQTT_ERR_SUCCESS
        except Exception as e:
            st.session_state.status_message = f"❌ Publish error: {str(e)}"
            return False
    
    def disconnect(self):
        self.client.loop_stop()
        self.client.disconnect()
    
    def wait_for_response(self, timeout=10):
        """Wait for MQTT response with timeout"""
        try:
            return self.response_queue.get(timeout=timeout)
        except:
            return None
    
    def clear_response_queue(self):
        """Clear any pending responses"""
        while not self.response_queue.empty():
            try:
                self.response_queue.get_nowait()
            except:
                break

def send_initial_payload(mqtt_client, status_placeholder):
    """Send initial handshake payload and wait for confirmation"""
    # Clear any stale messages before sending
    mqtt_client.clear_response_queue()
    time.sleep(0.5)  # Small delay to ensure queue is empty
    
    payload = json.dumps({"T": 14, "S": 86, "D": 1})
    cfg = mqtt_client.config
    mqtt_client.publish(cfg["MQTT_TX_COMMAND_TOPIC"], payload)
    status_placeholder.info("📤 Sent initial payload  to " + cfg["MQTT_TX_COMMAND_TOPIC"])
    status_placeholder.info("⏳ Waiting for response . (100 seconds)")
    
    # Wait for specific response: T=14, S=96, D=3
    for attempt in range(100):
        response = mqtt_client.wait_for_response(timeout=1)
        if response:
            if response.get('T') == 45 :
                status_placeholder.success(f"✅ Received expected response: T={response.get('T')}, S={response.get('S')}, D={response.get('D')}")
                mqtt_client.clear_response_queue()  # Clear any pending messages
                return response
            else:
                status_placeholder.warning(f"⚠️ Received response but not matching pattern (T={response.get('T')}, S={response.get('S')}). Waiting for T=14, S=96...")
        if attempt % 5 == 0 and attempt > 0:
            status_placeholder.info(f"⏳ Waiting... ({attempt}/110s)")
    
    status_placeholder.error("❌ No matching response received for initial payload (timeout)")
    return None

def send_second_payload(mqtt_client, status_placeholder):
    """Send second payload with URL and CRC info, and wait for exact confirmation"""
    # Clear any stale messages before sending
    mqtt_client.clear_response_queue()
    time.sleep(0.5)  # Small delay to ensure queue is empty
    
    payload = json.dumps({
        "T": 15,
        "S": 78,
        "D": {
            "url": "https://dev-api-apfc.peepul.farm/v1.0/devices/test-api-get/bytes-data?file_key=FOTA/MOTOR_STARTER_ADC_V1.0.bin",
            "crc": 811448179,
            "size": 162536
        }
    })
    cfg = mqtt_client.config
    mqtt_client.publish(cfg["MQTT_TX_COMMAND_TOPIC"], payload)
    status_placeholder.info("📤 Sent second payload (T=15, S=78) with URL and CRC info to " + cfg["MQTT_TX_COMMAND_TOPIC"])
    status_placeholder.info("⏳ Waiting for response with matching (110 seconds)")
    
    # Wait for specific response: T=15, S=78, D with url/crc/size
    for attempt in range(110):
        response = mqtt_client.wait_for_response(timeout=1)
        if response:
            if response.get('T') == 46:
                status_placeholder.success(f"✅ Received expected response: T={response.get('T')}, S={response.get('S')}")
                mqtt_client.clear_response_queue()  # Clear any pending messages
                return response
            else:
                status_placeholder.warning(f"⚠️ Received response but not matching pattern (T={response.get('T')}, S={response.get('S')}). Waiting for T=15, S=78...")
        if attempt % 5 == 0 and attempt > 0:
            status_placeholder.info(f"⏳ Waiting... ({attempt}/110s)")
    
    status_placeholder.error("❌ No matching response received for second payload (timeout)")
    return None

def send_download_command(mqtt_client, status_placeholder):
    """Send download start command to vehicle/tx_cmd topic to confirm start"""
    # Clear any stale messages before sending
    mqtt_client.clear_response_queue()
    time.sleep(0.5)  # Small delay to ensure queue is empty
    
    payload = json.dumps({"T": 16, "S": 86, "D": 1})
    cfg = mqtt_client.config
    mqtt_client.publish(cfg["MQTT_TX_COMMAND_TOPIC"], payload)
    status_placeholder.info("📤 Sent download command (T=16, S=86, D=1) to " + cfg["MQTT_TX_COMMAND_TOPIC"])
    status_placeholder.info("⏳ Waiting for first offset/size request... (100 seconds)")
    
    # Wait longer for first device request with offset and size
    for attempt in range(100):
        response = mqtt_client.wait_for_response(timeout=1)
        if response and response.get("T") == 51:
            req_data = response.get("D", {})
            # Check if this is an offset/size request (has offset and size fields)
            if "offset" in req_data and "size" in req_data:
                status_placeholder.success(f"✅ Received first request: offset={req_data.get('offset')}, size={req_data.get('size')}")
                mqtt_client.clear_response_queue()  # Clear any pending messages
                return response
        if attempt % 5 == 0 and attempt > 0:
            status_placeholder.info(f"⏳ Waiting for device request... ({attempt}/100s)")
    
    status_placeholder.error("❌ No device request received (timeout)")
    return None

def handle_offset_request(response, file_bytes, mqtt_client, status_placeholder, progress_placeholder):
    try:
        # 1️⃣ Validate message
        if not isinstance(response, dict):
            return False

        if response.get("T") != 51:
            return False

        req = response.get("D")
        if not isinstance(req, dict):
            status_placeholder.warning(f"⚠️ Invalid request D={req}")
            return False

        # 2️⃣ Extract offset & size dynamically
        offset = int(req.get("offset", -1))
        size = int(req.get("size", 0))

        file_size = len(file_bytes)

        if offset < 0 or size <= 0 or offset >= file_size:
            status_placeholder.error(
                f"❌ Invalid offset/size (offset={offset}, size={size}, file={file_size})"
            )
            return False

        # 3️⃣ Slice EXACTLY what device asked
        end = min(offset + size, file_size)
        chunk = file_bytes[offset:end]

        if not chunk:
            status_placeholder.warning("⚠️ No data left to send (EOF)")
            return False

        # 4️⃣ Build response payload
        payload = {
            "T": 23,
            "S": response.get("S", 86),
            "D": {
                "offset": offset,
                "size": len(chunk),     # 👈 actual size sent
                "data": base64.b64encode(chunk).decode("utf-8")
            }
        }

        cfg = mqtt_client.config
        mqtt_client.publish(cfg["MQTT_TX_COMMAND_TOPIC"], json.dumps(payload))

        # 5️⃣ UI + progress
        status_placeholder.success(
            f"✅ Sent bytes [{offset}:{end}] ({len(chunk)} bytes)"
        )

        progress = end / file_size
        progress_placeholder.progress(progress)

        return True

    except Exception as e:
        status_placeholder.error(f"❌ Offset handler error: {e}")
        return False


def send_bin_file_chunks(file_bytes, filename, mqtt_client, progress_placeholder, status_placeholder):
    """Handle the complete bin file transmission flow with robust type checking"""
    mqtt_client.file_bytes = file_bytes
    mqtt_client.clear_response_queue()
    
    try:
        # Step 1: Handshake (T=14)
        status_placeholder.info("🚀 Starting BIN file transmission...")
        response1 = send_initial_payload(mqtt_client, status_placeholder)
        if not response1 or not st.session_state.get("is_sending", False):
            st.session_state.is_sending = False
            return
        
        time.sleep(1)
        
        # Step 2: Metadata/URL (T=15)
        response2 = send_second_payload(mqtt_client, status_placeholder)
        if not response2 or not st.session_state.get("is_sending", False):
            st.session_state.is_sending = False
            return
        
        time.sleep(1)
        
        # Step 3: Start Command (T=16)
        response3 = send_download_command(mqtt_client, status_placeholder)
        if not response3 or not st.session_state.get("is_sending", False):
            st.session_state.is_sending = False
            return
        
        # Step 4: Continuous Data Requests
        status_placeholder.info("📦 Ready to send file chunks. Listening for device data requests...")
        request_count = 0
        last_offset = -1
        last_size = -1
        last_send_time = 0
        MIN_INTERVAL_BETWEEN_SENDS = 1.5  # Adjusted based on your debug log speed
        
        # Handle the first device request that came in response3
        if response3 and response3.get("T") == 51:
            req_data = response3.get("D", {})
            if isinstance(req_data, dict) and "offset" in req_data and "size" in req_data:
                request_count += 1
                last_offset = req_data.get("offset")
                last_size = req_data.get("size")
                last_send_time = time.time()
                status_placeholder.info(f"📋 Request #{request_count}: offset={last_offset}, size={last_size}")
                if not handle_offset_request(response3, file_bytes, mqtt_client, status_placeholder, progress_placeholder):
                    st.session_state.is_sending = False
                    return
                status_placeholder.info(f"✅ Chunk sent. Waiting for next request...")
        
        while st.session_state.get("is_sending", False):
            response = mqtt_client.wait_for_response(timeout=10)
            
            # --- FIX: TYPE CHECKING START ---
            if response is not None and isinstance(response, dict):
                msg_type = response.get("T")
                
                # Handle actual data requests
                if msg_type == 51:
                    req_data = response.get("D", {})
                    # Validate D is a dict and contains required fields
                    if not isinstance(req_data, dict) or "offset" not in req_data or "size" not in req_data:
                        status_placeholder.warning(f"⚠️ Invalid request format: D={req_data}")
                        continue
                    
                    try:
                        current_offset = int(req_data.get("offset", -1))
                        current_size = int(req_data.get("size", 0))
                    except (ValueError, TypeError) as e:
                        status_placeholder.warning(f"⚠️ Invalid offset/size values: {e}")
                        continue
                    
                    time_since_last_send = time.time() - last_send_time
                    
                    # Check if it's a new request or a valid retry
                    if current_offset != last_offset or time_since_last_send >= MIN_INTERVAL_BETWEEN_SENDS:
                        request_count += 1
                        last_offset = current_offset
                        last_size = current_size
                        last_send_time = time.time()
                        
                        status_placeholder.info(f"📋 Request #{request_count}: offset={current_offset}, size={current_size}")
                        
                        if not handle_offset_request(response, file_bytes, mqtt_client, status_placeholder, progress_placeholder):
                            break
                        
                        status_placeholder.info(f"✅ Chunk sent. Waiting for next request...")
                    else:
                        # Throttling duplicate requests sent too quickly
                        status_placeholder.warning(f"⚠️ Throttling: Request for offset {current_offset} ignored.")
                
                # Handle status updates seen in your debug logs
                elif msg_type == 47:
                    status_placeholder.info(f"ℹ️ Device status update received (T=47, D={response.get('D')})")
                
                else:
                    status_placeholder.warning(f"⚠️ Received message with T={msg_type}. Waiting for T=51...")

            elif response is not None:
                # This catches the 'int' or other non-dict types that caused your crash
                status_placeholder.error(f"❌ Received malformed data: {response} (Type: {type(response).__name__})")
                # We skip this specific item and continue the loop instead of crashing
                continue
            
            else:
                # Timeout occurred
                status_placeholder.info(f"⏳ Waiting for device request (#{request_count + 1})...")
            # --- FIX: TYPE CHECKING END ---
            
            time.sleep(0.1) # Small sleep to prevent CPU spiking
        
        if st.session_state.get("is_sending", False):
            status_placeholder.success(f"✅ Transmission complete! Handled {request_count} data requests")
        else:
            status_placeholder.warning("⏸️ Transmission stopped by user")
        
    except Exception as e:
        status_placeholder.error(f"❌ Error during transmission: {str(e)}")
    finally:
        st.session_state.is_sending = False

# Streamlit App
st.set_page_config(page_title="BIN File MQTT Uploader", layout="wide")

st.title("📦 BIN File MQTT Uploader")

# Initialize session state
if "mqtt_client" not in st.session_state:
    st.session_state.mqtt_client = None
if "is_sending" not in st.session_state:
    st.session_state.is_sending = False
if "status_message" not in st.session_state:
    st.session_state.status_message = "Ready"

# Initialize custom MQTT config
if "custom_mqtt_config" not in st.session_state:
    st.session_state.custom_mqtt_config = MQTT_CONFIG.copy()
if "use_custom_config" not in st.session_state:
    st.session_state.use_custom_config = False

# Sidebar Configuration
with st.sidebar:
    st.header("⚙️ MQTT Configuration")
    st.warning("⚠️ Configure MQTT settings to continue", icon="⚠️")
    
    with st.form("mqtt_config_form"):
        st.subheader("🖥️ Broker Settings")
        broker = st.text_input(
            "MQTT Broker Address",
            value=st.session_state.custom_mqtt_config.get("MQTT_BROKER", ""),
            placeholder="e.g., w8e06e1d.ala.asia-southeast1.emqxsl.com",
            help="Your MQTT broker hostname or IP address"
        )
        port = st.number_input(
            "MQTT Port",
            value=st.session_state.custom_mqtt_config.get("MQTT_PORT", 8883),
            min_value=1,
            max_value=65535,
            help="Usually 8883 for secure connection, 1883 for non-secure"
        )
        
        st.subheader("🔐 Authentication")
        username = st.text_input(
            "Username",
            value=st.session_state.custom_mqtt_config.get("MQTT_USERNAME", ""),
            placeholder="Enter your MQTT username"
        )
        password = st.text_input(
            "Password",
            value=st.session_state.custom_mqtt_config.get("MQTT_PASSWORD", ""),
            type="password",
            placeholder="Enter your MQTT password"
        )
        
        st.subheader("📝 Topics")
        data_topic = st.text_input(
            "Data Topic (receive)",
            value=st.session_state.custom_mqtt_config.get("MQTT_TOPIC", ""),
            placeholder="e.g., vehicle/bin_Data/data",
            help="Topic where device sends responses"
        )
        cmd_topic = st.text_input(
            "Command Topic (send)",
            value=st.session_state.custom_mqtt_config.get("MQTT_TX_COMMAND_TOPIC", ""),
            placeholder="e.g., vehicle/tx_cmd",
            help="Topic where you send commands and file data"
        )
        
        if st.form_submit_button("✅ Save & Connect", use_container_width=True):
            # Validate all fields are filled
            if not all([broker, username, password, data_topic, cmd_topic]):
                st.error("❌ All fields are required!")
            else:
                # Update custom config
                st.session_state.custom_mqtt_config = {
                    "MQTT_BROKER": broker,
                    "MQTT_PORT": int(port),
                    "MQTT_USERNAME": username,
                    "MQTT_PASSWORD": password,
                    "MQTT_TOPIC": data_topic,
                    "MQTT_TX_COMMAND_TOPIC": cmd_topic
                }
                st.session_state.use_custom_config = True
                
                # Auto-connect
                mqtt_client = MQTTClient(st.session_state.custom_mqtt_config)
                if mqtt_client.connect(st.session_state.custom_mqtt_config):
                    st.session_state.mqtt_client = mqtt_client
                    st.success("✅ Configuration saved and connected!")
                    time.sleep(1)
                    st.rerun()
                else:
                    st.error("❌ Failed to connect to MQTT broker. Check your settings.")
    
    st.divider()
    
    # Show connection status
    if st.session_state.mqtt_client and st.session_state.mqtt_client.connected:
        st.success(f"✅ Connected to {st.session_state.custom_mqtt_config['MQTT_BROKER']}")
        st.text(f"Status: {st.session_state.status_message}")
        
        if st.button("🔌 Disconnect", use_container_width=True):
            st.session_state.mqtt_client.disconnect()
            st.session_state.mqtt_client = None
            st.info("Disconnected from MQTT broker")
            st.rerun()
    elif st.session_state.use_custom_config:
        st.info("⏳ Connecting...")
    else:
        st.info("Configure MQTT to connect")

# Main Content
if not (st.session_state.mqtt_client and st.session_state.mqtt_client.connected):
    st.error("❌ Not Connected")
    st.warning("Please configure MQTT settings in the sidebar and connect first.")
    st.info("""
    ### Getting Started:
    1. Enter your MQTT broker details in the sidebar
    2. Click "✅ Save & Connect" button
    3. Once connected, you can upload and send BIN files
    """)
    st.stop()

# Only show file upload if connected
col1, col2 = st.columns([2, 1])

with col1:
    st.subheader("📤 Upload BIN File")
    uploaded_file = st.file_uploader("Select a .bin file", type="bin")
    
    if uploaded_file is not None:
        st.success(f"✅ File selected: {uploaded_file.name}")
        st.info(f"📊 File size: {uploaded_file.size:,} bytes")
        # st.info(f"📦 Will send in {(uploaded_file.size + CHUNK_SIZE - 1) // CHUNK_SIZE} chunks of {CHUNK_SIZE} bytes")
        # st.info(f"⏱️ Interval: {SEND_INTERVAL} seconds between chunks")

with col2:
    st.subheader("📊 Transmission Stats")
    if uploaded_file is not None:
        total_size = uploaded_file.size
        num_chunks = (total_size + CHUNK_SIZE - 1) // CHUNK_SIZE
        total_time = num_chunks * SEND_INTERVAL - SEND_INTERVAL  # Last chunk doesn't wait
        
        st.metric("Total Size", f"{total_size:,} bytes")
        # st.metric("Number of Chunks", num_chunks)
        # st.metric("Est. Time", f"{total_time}s")

# Send Button
st.divider()

if uploaded_file is not None:
    col1, col2, col3 = st.columns([1, 1, 2])
    
    with col1:
        if st.button("🚀 Send File", use_container_width=True, disabled=not (st.session_state.mqtt_client and st.session_state.mqtt_client.connected)):
            if st.session_state.mqtt_client and st.session_state.mqtt_client.connected:
                st.session_state.is_sending = True
                
                # Create placeholders for progress and status
                progress_placeholder = st.empty()
                status_placeholder = st.empty()
                
                # Read file bytes
                file_bytes = uploaded_file.getvalue()
                
                # Send chunks with JSON payload
                send_bin_file_chunks(file_bytes, uploaded_file.name, st.session_state.mqtt_client, progress_placeholder, status_placeholder)
            else:
                st.error("❌ Not connected to MQTT broker. Please connect first.")
    
    with col2:
        if st.button("⏹️ Stop", use_container_width=True, disabled=not st.session_state.is_sending):
            st.session_state.is_sending = False
            st.warning("⏸️ Transmission stopped")

# Instructions
with st.expander("ℹ️ How it works"):
    st.markdown("""
    ### Protocol Flow:
    1. **Connect to MQTT**: Enter broker details in sidebar and click "Save & Connect"
    2. **Upload BIN File**: Select a .bin file to upload
    3. **Click Start**: Initiates the device handshake protocol
    
    ### Message Sequence:
    - **Step 1**: Sends handshake (T=14) → waits for device response
    - **Step 2**: Sends download info (T=15) with URL/CRC → waits for response
    - **Step 3**: Sends download start (T=16) → waits for device request
    - **Step 4**: Device requests chunks via offset/size (T=14) → app responds
    - **Step 5**: Repeat until all file data sent
    
    ### Required Topics:
    - **Send To**: Your command topic (e.g., vehicle/tx_cmd)
    - **Listen On**: Your data topic (e.g., vehicle/bin_Data/data)
    
    ### Features:
    - Request-based protocol (device controls chunk requests)
    - Base64-encoded binary data
    - Real-time progress tracking
    - SSL/TLS encrypted MQTT connection
    - Stop button to halt transmission anytime
    """)

st.divider()
st.caption("🔒 Secure MQTT Connection | BIN File Uploader v2.0")
