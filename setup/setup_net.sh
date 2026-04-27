#!/bin/bash

echo "--- 1. Installiere System-Abhängigkeiten ---"
sudo apt update
sudo apt install -y curl libgssapi-krb5-2 libicu-dev libssl-dev zlib1g

echo "--- 2. Lade Microsoft dotnet-install Skript ---"
curl -sSL https://dot.net -o dotnet-install.sh
chmod +x dotnet-install.sh

echo "--- 3. Installiere .NET 10 SDK ---"
./dotnet-install.sh --channel 10.0

echo "--- 4. Konfiguriere Umgebungsvariablen (~/.bashrc) ---"
# Prüfen, ob Pfade bereits existieren, um Duplikate zu vermeiden
if ! grep -q "DOTNET_ROOT" ~/.bashrc; then
  echo 'export DOTNET_ROOT=$HOME/.dotnet' >> ~/.bashrc
  echo 'export PATH=$PATH:$HOME/.dotnet' >> ~/.bashrc
  echo "Pfade wurden zur .bashrc hinzugefügt."
else
  echo "Pfade sind bereits in .bashrc vorhanden."
fi

# Aufräumen
rm dotnet-install.sh

echo "--- FERTIG! ---"
echo "BITTE FÜHRE DIESEN BEFEHL AUS, UM DIE ÄNDERUNGEN ZU ÜBERNEHMEN:"
echo "source ~/.bashrc"