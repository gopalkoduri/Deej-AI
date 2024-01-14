#!/bin/bash

while true; do
    python MP3ToVec.py Pickles mp3tovec --scan /datasets/universal-music-group-dataset/audio/
    sleep 10  
done

