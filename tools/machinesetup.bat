ECHO OFF


echo Installing Chocolatey

@powershell -NoProfile -ExecutionPolicy unrestricted -Command "iex ((new-object net.webclient).DownloadString('http://bit.ly/psChocInstall'))"
SET PATH=%PATH%;%systemdrive%\chocolatey\bin

echo Installing Software

call cinst git.install
SET PATH=%PATH%;C:\Program Files (x86)\Git\cmd

IF NOT EXIST "%PROGRAMFILES(X86)%\Microsoft Visual Studio 11.0\Common7\IDE\devenv.exe" (
   call cinst VisualStudioExpress2012Web
)

call cinst webpicommandline

call "%PROGRAMFILES(X86)%\Microsoft\Web Platform Installer\WebpiCmd.exe" /Install /Products:WindowsAzureToolsVS2012 /AcceptEula
