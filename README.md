# ✅ Final Submission Steps for Hands-on L9: Streaming Analytics with Spark

## Step 1 – Update README.md

1. In GitHub Codespaces, open `README.md` from the Explorer (left sidebar).
2. Replace all its text with the following:

   ```markdown
   # Hands-on L9 – Streaming Analytics with Spark  

   ## Overview
   This project builds a real-time analytics pipeline for a ride-sharing platform using **Apache Spark Structured Streaming**.  
   It demonstrates live data ingestion, driver-level aggregations, and time-window analytics.

   ## How to Run (in GitHub Codespaces or locally)
   1. Install PySpark  
      ```bash
      pip install pyspark
      ```
   2. Open two terminals  
      - **Terminal A – Start Data Generator**  
        ```bash
        python generator/generator.py
        ```
      - **Terminal B – Run Spark Streaming Tasks**  
        - **Task 1:** Ingest and Parse  
          ```bash
          python streaming/streaming_app.py --task 1
          ```
        - **Task 2:** Driver-Level Aggregations  
          ```bash
          python streaming/streaming_app.py --task 2
          ```
        - **Task 3:** Windowed Time-Based Analytics  
          ```bash
          python streaming/streaming_app.py --task 3
          ```

   ## Tasks and Outputs
   | Task | Description | Output Folder |
   |------|--------------|---------------|
   | **Task 1** | Parse JSON stream and display structured records | `outputs/task1` |
   | **Task 2** | Compute `SUM(fare_amount)` and `AVG(distance_km)` by `driver_id` | `outputs/task2` |
   | **Task 3** | 5-minute window (slide 1 minute) sum of `fare_amount` | `outputs/task3` |

   Each task writes CSV files for further analysis.  

   ## Notes
   - Run the generator **before** starting any Spark task.  
   - Task 3 needs 5–6 minutes to produce output.  
   - Do **not** commit `checkpoints/` (it is git-ignored).  
   - Keep 1–3 CSV files per task for submission.  

   ## Author
   **Sai Harsha Vunnava**  
   ITCS 6190 / 8190 Cloud Computing for Data Analysis – Fall 2025
