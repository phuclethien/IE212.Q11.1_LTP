"""
Camera Server Module
Simulates a camera server that captures frames from webcam and sends them to processing server via TCP
"""

import cv2
import socket
import json
import base64
import time
import numpy as np
from config import Config

class CameraServer:
    def __init__(self):
        self.host = Config.STREAM_HOST
        self.port = Config.STREAM_PORT
        self.socket = None
        self.connection = None
        self.camera = None
        
    def initialize_camera(self):
        """Initialize webcam capture"""
        self.camera = cv2.VideoCapture(0)
        self.camera.set(cv2.CAP_PROP_FRAME_WIDTH, Config.CAMERA_WIDTH)
        self.camera.set(cv2.CAP_PROP_FRAME_HEIGHT, Config.CAMERA_HEIGHT)
        self.camera.set(cv2.CAP_PROP_FPS, Config.CAMERA_FPS)
        
        if not self.camera.isOpened():
            raise Exception("Cannot open camera")
        
        print(f"Camera initialized: {Config.CAMERA_WIDTH}x{Config.CAMERA_HEIGHT} @ {Config.CAMERA_FPS}fps")
    
    def connect_to_server(self):
        """Establish TCP connection as client to processing server"""
        print(f"Connecting to processing server at {self.host}:{self.port}...")
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        
        # Try to connect with retry
        max_retries = 5
        retry_count = 0
        while retry_count < max_retries:
            try:
                self.socket.connect((self.host, self.port))
                print(f"Connected to processing server at {self.host}:{self.port}")
                return True
            except ConnectionRefusedError:
                retry_count += 1
                print(f"Connection refused. Retry {retry_count}/{max_retries}...")
                time.sleep(2)
        
        print("Failed to connect to processing server")
        return False
    
    def encode_frame(self, frame):
        """Encode frame to base64 string for transmission"""
        # Encode frame as JPEG
        _, buffer = cv2.imencode('.jpg', frame, [cv2.IMWRITE_JPEG_QUALITY, 85])
        # Convert to base64
        frame_base64 = base64.b64encode(buffer).decode('utf-8')
        return frame_base64
    
    def send_frame(self, frame, frame_id):
        """Send frame as JSON payload via TCP"""
        try:
            # Encode frame
            frame_base64 = self.encode_frame(frame)
            
            # Prepare payload
            payload = {
                "frame_id": frame_id,
                "timestamp": time.time(),
                "width": frame.shape[1],
                "height": frame.shape[0],
                "data": frame_base64
            }
            
            # Serialize and send
            json_payload = json.dumps(payload) + "\n"
            self.socket.sendall(json_payload.encode('utf-8'))
            
            return True
        except BrokenPipeError:
            print("Connection broken. Server may have closed the connection.")
            return False
        except Exception as e:
            print(f"Error sending frame: {e}")
            return False
    
    def start_streaming(self, max_frames=None):
        """Start capturing and streaming frames"""
        print("Starting frame capture and streaming...")
        print("Press 'q' to quit")
        
        frame_id = 0
        frames_sent = 0
        start_time = time.time()
        
        try:
            while True:
                # Capture frame
                ret, frame = self.camera.read()
                if not ret:
                    print("Failed to capture frame")
                    break
                
                # Display frame locally
                cv2.imshow('Camera Server - Press Q to quit', frame)
                
                # Send frame to processing server
                if self.send_frame(frame, frame_id):
                    frames_sent += 1
                    frame_id += 1
                    
                    # Print stats every 30 frames
                    if frames_sent % 30 == 0:
                        elapsed = time.time() - start_time
                        fps = frames_sent / elapsed
                        print(f"Sent {frames_sent} frames | FPS: {fps:.2f}")
                else:
                    print("Failed to send frame. Stopping...")
                    break
                
                # Check max frames limit
                if max_frames and frames_sent >= max_frames:
                    print(f"Reached max frames limit: {max_frames}")
                    break
                
                # Check for quit key
                if cv2.waitKey(1) & 0xFF == ord('q'):
                    print("User requested quit")
                    break
                    
        except KeyboardInterrupt:
            print("\nInterrupted by user")
        finally:
            self.cleanup()
    
    def cleanup(self):
        """Release resources"""
        print("Cleaning up resources...")
        if self.camera:
            self.camera.release()
        if self.socket:
            self.socket.close()
        cv2.destroyAllWindows()
        print("Cleanup complete")

def main():
    """Main function to run camera server"""
    camera_server = CameraServer()
    
    # Initialize camera
    camera_server.initialize_camera()
    
    # Connect to processing server
    if not camera_server.connect_to_server():
        print("Cannot start streaming without connection to processing server")
        camera_server.cleanup()
        return
    
    # Start streaming (set max_frames=None for unlimited)
    camera_server.start_streaming(max_frames=None)

if __name__ == "__main__":
    main()
