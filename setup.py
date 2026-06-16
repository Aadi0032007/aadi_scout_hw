from setuptools import setup, find_packages

setup(
    name="aadi-scout-hw",
    version="0.1.0",
    description="Aditya — Jetson-based telepresence robot controller",
    python_requires=">=3.10",
    packages=find_packages(),
    install_requires=[
        "opencv-python>=4.8",
        "numpy>=1.24",
        "requests>=2.31",
        "pyserial>=3.5",
        "daily-python>=0.11",
        "onvif-zeep>=0.2.12",
        "hid>=1.0.4",
        "evdev>=1.6",
        "piper-tts>=1.2",
        "python-dotenv>=1.0",
    ],
)