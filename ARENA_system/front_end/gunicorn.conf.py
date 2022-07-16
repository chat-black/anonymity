import multiprocessing

bind = "0.0.0.0:5000"
workers = multiprocessing.cpu_count()
worker_connections = 1000
timeout = 300  # 超时时间300s
