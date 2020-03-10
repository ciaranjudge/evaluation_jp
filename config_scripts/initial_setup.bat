
@echo off
echo --------------------------------------------------------
echo                  *** DON'T PANIC! ***
echo --------------------------------------------------------

call %LOCALAPPDATA%\Continuum\miniconda3\Scripts\activate.bat

call conda install pyyaml
REM Clone or install brassicate!
if not exist %userprofile%\repos (
    call md %userprofile%\repos
)
call git clone https://github.com/ciaranjudge/brassicate
if %errorlevel% neq 0 (
    echo Arg! Looks like you're behind a corporate firewall.
    echo As a workaround, set git http.sslverify to false.
    call git config --global --replace-all http.sslverify false
    echo ...and try again
    call git clone https://github.com/ciaranjudge/brassicate
)
REM Do the initial setup
