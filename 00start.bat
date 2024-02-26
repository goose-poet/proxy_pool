@echo off
call E:\python\project\github\proxy_pool\proxy_pool\.venv\Scripts\activate 
cd /d E:\python\project\github\proxy_pool\proxy_pool
start cmd /k python proxyPool.py server
choice /t 2 /d y

python proxyPool.py schedule
pause