# Streaming Mini Project
Use Databricks Free Edition for learning. Upload data/landing/*.json into a Unity Catalog Volume.

Run order:
1. Create catalog/schema/volume.
2. Upload both JSON files.
3. Run src/01_streaming_pipeline.py.
4. Observe bronze -> silver -> gold Delta tables.
5. Add another JSON file and rerun with the same checkpoints.
6. Experiment with duplicates and late events.
