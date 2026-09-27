@echo off
rem feiq-glm Windows 双击启动脚本
chcp 65001 >nul
cd /d "%~dp0"
node server.js
pause
