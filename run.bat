@echo off
echo Starting TopStepX trading bot...
:loop
    python app.py
    set rc=%ERRORLEVEL%
    if %rc% EQU 0 (
        echo app.py exited normally with code 0. Exiting supervisor.
        goto :done
    )
    echo ❌ app.py crashed with exit code %rc%. Restarting in 5 seconds... >&2
    timeout /t 5 /nobreak >nul
goto :loop
:done
echo ✅ Script finished successfully.
pause
