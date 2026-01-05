"""
Processing Server Module
Receives frames via TCP, processes them using Spark to remove background, and saves results
"""

import socket
import json
import base64
import numpy as np
import cv2
import os
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from config import Config
import time

# Global function for Spark - must be outside class to be serializable
def process_single_frame(frame_data):
    """Process a single frame - called by Spark workers"""
    # Import inside function to avoid serialization issues
    from background_remover import remove_background
    
    try:
        frame_id = frame_data.get("frame_id")
        frame_base64 = frame_data.get("data")
        output_dir = frame_data.get("output_dir", "output_frames")
        
        # Decode base64
        frame_bytes = base64.b64decode(frame_base64)
        frame_array = np.frombuffer(frame_bytes, dtype=np.uint8)
        frame = cv2.imdecode(frame_array, cv2.IMREAD_COLOR)
        
        if frame is None:
            return {
                "frame_id": frame_id,
                "status": "error",
                "error": "Failed to decode frame"
            }
        
        # Remove background using MediaPipe
        processed_frame = remove_background(frame)
        
        # Save processed frame
        output_path = os.path.join(output_dir, f"frame_{frame_id:06d}.jpg")
        cv2.imwrite(output_path, processed_frame)
        
        return {
            "frame_id": frame_id,
            "status": "success",
            "output_path": output_path,
            "timestamp": time.time()
        }
    except Exception as e:
        return {
            "frame_id": frame_data.get("frame_id", -1),
            "status": "error",
            "error": str(e)
        }

class ProcessingServer:
    def __init__(self):
        self.host = Config.STREAM_HOST
        self.port = Config.STREAM_PORT
        self.server_socket = None
        self.output_dir = Config.OUTPUT_DIR
        
        # Create output directory
        os.makedirs(self.output_dir, exist_ok=True)
        print(f"Output directory: {self.output_dir}")
        
    def start_tcp_server(self):
        """Start TCP server to accept connections"""
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(1)
        print(f"Processing server listening on {self.host}:{self.port}")
        print("Waiting for camera server connection...")
    
    def receive_and_process_frames(self, connection):
        """Receive frames and process them with Spark"""
        print("\n=== Starting Spark-based Frame Processing ===\n")
        
        # Configure Spark
        conf = SparkConf().setAppName(Config.APP_NAME).setMaster(Config.SPARK_MASTER)
        sc = SparkContext(conf=conf)
        sc.setLogLevel("WARN")  # Reduce Spark logging
        
        print(f"Spark Context initialized: {Config.APP_NAME}")
        print(f"Spark Master: {Config.SPARK_MASTER}\n")
        
        frame_buffer = []
        batch_size = 10  # Process frames in batches
        frames_processed = 0
        start_time = time.time()
        
        try:
            buffer = ""
            while True:
                # Receive data
                data = connection.recv(Config.BUFFER_SIZE)
                if not data:
                    print("Connection closed by camera server")
                    break
                
                # Decode and accumulate buffer
                buffer += data.decode('utf-8')
                
                # Process complete messages (newline-delimited JSON)
                while '\n' in buffer:
                    line, buffer = buffer.split('\n', 1)
                    if line.strip():
                        try:
                            frame_data = json.loads(line)
                            # Add output_dir to frame data for Spark workers
                            frame_data["output_dir"] = self.output_dir
                            frame_buffer.append(frame_data)
                            
                            # Process batch when buffer is full
                            if len(frame_buffer) >= batch_size:
                                self._process_batch_with_spark(sc, frame_buffer)
                                frames_processed += len(frame_buffer)
                                frame_buffer = []
                                
                                # Print statistics
                                elapsed = time.time() - start_time
                                fps = frames_processed / elapsed if elapsed > 0 else 0
                                print(f"Processed {frames_processed} frames | FPS: {fps:.2f}")
                                
                        except json.JSONDecodeError as e:
                            print(f"Error decoding JSON: {e}")
            
            # Process remaining frames in buffer
            if frame_buffer:
                self._process_batch_with_spark(sc, frame_buffer)
                frames_processed += len(frame_buffer)
                print(f"Processed final batch: {len(frame_buffer)} frames")
            
            print(f"\nTotal frames processed: {frames_processed}")
            print(f"Total time: {time.time() - start_time:.2f} seconds")
            
        except KeyboardInterrupt:
            print("\nInterrupted by user")
        finally:
            # Stop Spark
            sc.stop()
            print("Spark Context stopped")
    
    def _process_batch_with_spark(self, sc, frame_batch):
        """Process a batch of frames using Spark RDD"""
        from background_remover import remove_background
        
        # Create RDD from frame batch (Spark data distribution)
        frames_rdd = sc.parallelize(frame_batch)
        
        # Collect frame data from RDD for processing
        frame_list = frames_rdd.collect()
        
        # Process each frame locally (MediaPipe doesn't serialize well)
        results = []
        for frame_data in frame_list:
            try:
                frame_id = frame_data.get("frame_id")
                frame_base64 = frame_data.get("data")
                
                # Decode frame
                frame_bytes = base64.b64decode(frame_base64)
                frame_array = np.frombuffer(frame_bytes, dtype=np.uint8)
                frame = cv2.imdecode(frame_array, cv2.IMREAD_COLOR)
                
                if frame is None:
                    results.append({
                        "frame_id": frame_id,
                        "status": "error",
                        "error": "Failed to decode frame"
                    })
                    continue
                
                # Remove background using MediaPipe
                processed_frame = remove_background(frame)
                
                # Save processed frame
                output_path = os.path.join(self.output_dir, f"frame_{frame_id:06d}.jpg")
                cv2.imwrite(output_path, processed_frame)
                
                results.append({
                    "frame_id": frame_id,
                    "status": "success",
                    "output_path": output_path,
                    "timestamp": time.time()
                })
            except Exception as e:
                results.append({
                    "frame_id": frame_data.get("frame_id", -1),
                    "status": "error",
                    "error": str(e)
                })
        
        # Log results
        success_count = sum(1 for r in results if r and r.get("status") == "success")
        print(f"Batch processed: {success_count}/{len(frame_batch)} frames successful")
        
        return results
    
    def run(self):
        """Main run method for processing server"""
        try:
            # Start TCP server
            self.start_tcp_server()
            
            # Accept connection
            connection, address = self.server_socket.accept()
            print(f"Connected to camera server: {address}\n")
            
            # Receive and process frames
            self.receive_and_process_frames(connection)
            
        except Exception as e:
            print(f"Error in processing server: {e}")
        finally:
            self.cleanup()
    
    def cleanup(self):
        """Clean up resources"""
        print("\nCleaning up resources...")
        if self.server_socket:
            self.server_socket.close()
        print("Processing server stopped")

def main():
    """Main function to run processing server"""
    print("="*60)
    print("Background Removal Processing Server with Spark")
    print("="*60)
    
    server = ProcessingServer()
    server.run()

if __name__ == "__main__":
    main()
