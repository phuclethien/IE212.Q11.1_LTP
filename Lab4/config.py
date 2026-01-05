"""
Configuration file for the Background Removal Streaming System
"""

class Config:
    # Network Configuration
    STREAM_HOST = "localhost"
    STREAM_PORT = 6100
    
    # Camera Configuration
    CAMERA_WIDTH = 320
    CAMERA_HEIGHT = 240
    CAMERA_FPS = 5
    
    # Processing Configuration
    OUTPUT_DIR = "output_frames"
    MODEL_PATH = "selfie_segmenter.tflite"
    
    # Spark Configuration
    APP_NAME = "BackgroundRemovalStreaming"
    SPARK_MASTER = "local[*]"  # Use all available cores
    
    # Background Colors
    BG_COLOR = (192, 192, 192)  # gray
    MASK_COLOR = (255, 255, 255)  # white
    
    # Buffer Configuration
    BUFFER_SIZE = 65536  # 64KB
    MAX_PACKET_SIZE = 1024 * 1024  # 1MB per packet
